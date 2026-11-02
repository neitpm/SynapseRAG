import sys
import os
import csv
import logging
import time
import json
import uuid
import threading
from datetime import datetime, timezone
from pathlib import Path
from neo4j import GraphDatabase
from transformers import AutoTokenizer
from sentence_transformers import SentenceTransformer

# Reconfigure stdout for UTF-8 on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("internal-chunk-ingest")

DATASETS_DIR = Path("datasets_sanitized")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "graphrag_password123")
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

MAX_TOKENS_PER_CHUNK = 220
TOKEN_OVERLAP = 40
LEASE_DURATION_SECONDS = 1800  # 30 phút
HEARTBEAT_INTERVAL_SECONDS = 30  # gửi heartbeat mỗi 30s

class IngestionLockManager:
    """
    Quản lý khóa phân tán Ingestion Lock với job_id, fencing_token (lock_generation),
    lease timeout recovery, atomic conditional update và heartbeat.
    """
    def __init__(self, driver, job_id: str, staging_version: str):
        self.driver = driver
        self.job_id = job_id
        self.staging_version = staging_version
        self.fencing_token = None
        self._stop_heartbeat = threading.Event()
        self._heartbeat_thread = None

    def acquire_lock(self) -> bool:
        """
        Thực hiện Cypher Atomic Conditional Update với Fencing Token và Crash Recovery:
        - Tăng s.lock_generation (fencing_token) thêm 1 mỗi lần acquire.
        - Chỉ acquire được khi:
          1. Chưa có lock: lock_job_id IS NULL (và status IDLE/FAILED)
          2. Re-entrant: lock_job_id = $job_id
          3. Crash Recovery (Lease Expired): lock_job_id IS NOT NULL nhưng last_heartbeat_at
             đã quá hạn > LEASE_DURATION_SECONDS (do job trước bị crash cứng / kill -9 / network partition).
        - Khi Crash Recovery: xóa sạch các chunk staging mồ côi theo cả old_staging_ver và old_job_id.
        """
        now_ts = int(time.time())
        lease_timeout_ts = now_ts - LEASE_DURATION_SECONDS

        acquire_cypher = """
        MERGE (s:SystemConfig {id: 'internal_rag_state'})
        WITH s
        WHERE (s.lock_job_id IS NULL AND coalesce(s.ingestion_status, 'IDLE') IN ['IDLE', 'FAILED'])
           OR (s.lock_job_id = $job_id)
           OR (s.lock_job_id IS NOT NULL AND coalesce(s.last_heartbeat_at, 0) < $lease_timeout_ts)
        
        WITH s, 
             s.lock_job_id AS old_job_id,
             s.staging_version AS old_staging_ver,
             CASE WHEN s.lock_job_id IS NOT NULL AND s.lock_job_id <> $job_id THEN true ELSE false END AS is_recovery

        SET s.lock_generation = coalesce(s.lock_generation, 0) + 1,
            s.lock_job_id = $job_id,
            s.staging_version = $staging_version,
            s.ingestion_status = 'INGESTING',
            s.lock_acquired_at = $now_ts,
            s.last_heartbeat_at = $now_ts
        RETURN s.lock_job_id AS lock_job_id, 
               s.staging_version AS staging_version,
               s.lock_generation AS fencing_token,
               is_recovery,
               old_job_id,
               old_staging_ver
        """
        with self.driver.session() as session:
            rec = session.run(
                acquire_cypher,
                job_id=self.job_id,
                staging_version=self.staging_version,
                now_ts=now_ts,
                lease_timeout_ts=lease_timeout_ts
            ).single()

            if rec and rec["lock_job_id"] == self.job_id:
                self.fencing_token = rec["fencing_token"]
                if rec["is_recovery"]:
                    old_ver = rec["old_staging_ver"]
                    old_jid = rec["old_job_id"]
                    logger.warning(
                        f"⚠️ CRASH RECOVERY: Phát hiện job trước bị treo/crash quá hạn lease "
                        f"(> {LEASE_DURATION_SECONDS}s). Đã tiếp quản lock an toàn [job_id: {self.job_id}, fencing_token: {self.fencing_token}]."
                    )
                    if old_ver and old_jid:
                        logger.info(f"🧹 Đang xóa các chunk staging mồ côi theo cả version ({old_ver}) VÀ job_id ({old_jid})...")
                        session.run("""
                            MATCH (c:InternalChunk)
                            WHERE c.version = $ver AND c.ingestion_job_id = $jid
                            DETACH DELETE c
                        """, ver=old_ver, jid=old_jid)
                        logger.info("✅ Đã dọn dẹp xong staging mồ côi.")
                    elif old_ver:
                        session.run("MATCH (c:InternalChunk {version: $ver}) DETACH DELETE c", ver=old_ver)

                logger.info(f"🔒 Đã chiếm Ingestion Lock thành công [job_id: {self.job_id}, fencing_token: {self.fencing_token}, staging_version: {self.staging_version}]")
                self._start_heartbeat()
                return True
            else:
                cur = session.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) RETURN s").single()
                cur_data = dict(cur["s"]) if cur else {}
                logger.error(
                    f"⛔ KHÔNG THỂ CHIẾM LOCK: Hệ thống đang bị chiếm bởi job khác còn hiệu lực lease "
                    f"[job_id: {cur_data.get('lock_job_id')}, status: {cur_data.get('ingestion_status')}, "
                    f"last_heartbeat: {cur_data.get('last_heartbeat_at')}]. Từ chối chạy để tránh xung đột."
                )
                return False

    def verify_ownership(self) -> bool:
        """
        Fencing check: Xác minh lock vẫn đang thuộc về job_id VÀ fencing_token hiện tại.
        Nếu job bị treo quá lâu và bị job mới takeover (lock_generation đã tăng), check này sẽ trả về False ngay.
        """
        check_cypher = """
        MATCH (s:SystemConfig {id: 'internal_rag_state'})
        RETURN s.lock_job_id AS lock_job_id, s.lock_generation AS lock_generation
        """
        with self.driver.session() as session:
            rec = session.run(check_cypher).single()
            if not rec or rec["lock_job_id"] != self.job_id or rec["lock_generation"] != self.fencing_token:
                logger.error(
                    f"🛑 FENCING TOKEN VIOLATION: Mất quyền sở hữu lock! "
                    f"Job hiện tại: [job_id: {self.job_id}, token: {self.fencing_token}], "
                    f"DB hiện tại: [job_id: {rec.get('lock_job_id') if rec else None}, token: {rec.get('lock_generation') if rec else None}]. "
                    f"DỪNG NGAY MỌI THAO TÁC GHI ĐỂ BẢO VỆ CSDL."
                )
                return False
            return True

    def _start_heartbeat(self):
        def _beat():
            while not self._stop_heartbeat.wait(HEARTBEAT_INTERVAL_SECONDS):
                now_ts = int(time.time())
                beat_cypher = """
                MATCH (s:SystemConfig {id: 'internal_rag_state'})
                WHERE s.lock_job_id = $job_id AND s.lock_generation = $token
                SET s.last_heartbeat_at = $now_ts
                """
                try:
                    with self.driver.session() as session:
                        session.run(beat_cypher, job_id=self.job_id, token=self.fencing_token, now_ts=now_ts)
                except Exception as e:
                    logger.warning(f"Lỗi khi gửi heartbeat ingestion lock: {e}")

        self._heartbeat_thread = threading.Thread(target=_beat, daemon=True)
        self._heartbeat_thread.start()

    def stop_heartbeat(self):
        self._stop_heartbeat.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2)

    def release_lock_on_failure(self):
        """
        Khi job thất bại, chỉ giải phóng nếu chính job_id và fencing_token này đang nắm lock.
        Đổi trạng thái sang FAILED và dọn dẹp các chunk staging gắn với job_id này.
        """
        self.stop_heartbeat()
        release_cypher = """
        MATCH (s:SystemConfig {id: 'internal_rag_state'})
        WHERE s.lock_job_id = $job_id AND s.lock_generation = $token
        SET s.ingestion_status = 'FAILED',
            s.failed_version = $staging_version,
            s.staging_version = null,
            s.lock_job_id = null,
            s.lock_acquired_at = null,
            s.last_heartbeat_at = null
        RETURN s.id
        """
        with self.driver.session() as session:
            res = session.run(
                release_cypher,
                job_id=self.job_id,
                token=self.fencing_token,
                staging_version=self.staging_version
            ).single()
            if res:
                logger.info(f"🔓 Đã chuyển trạng thái FAILED và giải phóng lock [job_id: {self.job_id}, token: {self.fencing_token}]")
                # Xóa sạch staging chunks thỏa mãn cả ingestion_job_id VÀ version của job này
                logger.info(f"🧹 Đang xóa các chunk staging của job lỗi [job_id: {self.job_id}, version: {self.staging_version}]...")
                session.run("""
                    MATCH (c:InternalChunk)
                    WHERE c.ingestion_job_id = $job_id AND c.version = $ver
                    DETACH DELETE c
                """, job_id=self.job_id, ver=self.staging_version)
                logger.info(f"✅ Đã dọn dẹp xong staging lỗi ({self.staging_version}).")

def read_sanitized_csv(file_name: str):
    path = DATASETS_DIR / file_name
    if not path.exists():
        logger.warning(f"File {file_name} không tồn tại.")
        return []
    with open(path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)

def clean_and_extract_threads(notes: str):
    if not notes:
        return ""
    idx = notes.find("[THREAD:")
    if idx == -1:
        return notes.strip()
    
    prefix = notes[:idx].strip()
    rest = notes[idx + len("[THREAD:"):].strip()
    
    if "[tech-status:" in rest:
        rest = rest[:rest.find("[tech-status:")].strip()
    if rest.endswith("]"):
        rest = rest[:-1].strip()
        
    meaningful = []
    try:
        messages = json.loads(rest)
        for m in messages:
            content = (m.get("content") or "").strip()
            if not content or content.lower() in ["đúng", "dạ", "dạ anh", "dạ vâng", "ok", "done"]:
                continue
            role = m.get("senderRole") or "Nhân sự"
            meaningful.append(f"{role}: {content}")
    except Exception:
        pass
        
    parts = []
    if prefix:
        parts.append(prefix)
    if meaningful:
        parts.append("Trao đổi công việc:\n" + "\n".join(meaningful))
    return "\n".join(parts).strip()

def split_text_by_tokenizer(text: str, tokenizer, max_tokens: int = MAX_TOKENS_PER_CHUNK, overlap: int = TOKEN_OVERLAP):
    token_ids = tokenizer.encode(text, add_special_tokens=False)
    if len(token_ids) <= max_tokens:
        return [text]
        
    chunks = []
    step = max_tokens - overlap
    for i in range(0, len(token_ids), step):
        batch_ids = token_ids[i:i + max_tokens]
        chunk_text = tokenizer.decode(batch_ids, skip_special_tokens=True).strip()
        if chunk_text:
            chunks.append(chunk_text)
        if i + max_tokens >= len(token_ids):
            break
    return chunks

def init_internal_indexes(session):
    logger.info("🔧 Đang kiểm tra Constraint & Index cho InternalChunk trên Neo4j...")
    
    # 1. Unique constraint trên chunk_id và version
    session.run("""
        CREATE CONSTRAINT internal_chunk_id_unique IF NOT EXISTS
        FOR (c:InternalChunk) REQUIRE c.chunk_id IS UNIQUE
    """)

    session.run("""
        CREATE CONSTRAINT system_config_id_unique IF NOT EXISTS
        FOR (s:SystemConfig) REQUIRE s.id IS UNIQUE
    """)

    session.run("""
        CREATE VECTOR INDEX internal_chunk_vector_index IF NOT EXISTS
        FOR (c:InternalChunk) ON (c.embedding)
        OPTIONS {indexConfig: {
          `vector.dimensions`: 384,
          `vector.similarity_function`: 'cosine'
        }}
    """)

    session.run("""
        CREATE FULLTEXT INDEX internal_chunk_fulltext_index IF NOT EXISTS
        FOR (c:InternalChunk) ON EACH [c.text]
    """)

    logger.info("⏳ Đang kiểm tra trạng thái ONLINE của vector index...")
    max_wait = 30
    start_t = time.time()
    while time.time() - start_t < max_wait:
        res = session.run("SHOW INDEXES YIELD name, state WHERE name = 'internal_chunk_vector_index' RETURN state").single()
        if res and res["state"] == "ONLINE":
            logger.info("✅ 'internal_chunk_vector_index' đã ONLINE và sẵn sàng.")
            break
        time.sleep(1)

def extract_chunks_from_field_logs(rows, tokenizer, version_tag: str, job_id: str):
    chunks = []
    for r in rows:
        fid = r.get("id") or ""
        p_code = r.get("project_code") or ""
        date = r.get("date") or ""
        created_by = r.get("created_by") or ""
        
        parts = []
        if r.get("work_completed"):
            parts.append(f"Công việc thực hiện: {r['work_completed'].strip()}")
        if r.get("issues"):
            parts.append(f"Vấn đề phát sinh / Sự cố: {r['issues'].strip()}")
        if r.get("notes"):
            parts.append(f"Ghi chú hiện trường: {r['notes'].strip()}")
            
        full_text = "\n".join(parts).strip()
        if not full_text:
            continue
            
        split_texts = split_text_by_tokenizer(full_text, tokenizer)
        for idx, t in enumerate(split_texts):
            chunks.append({
                "chunk_id": f"field_logs_{fid}_{idx}_{version_tag}",
                "source_table": "field_logs",
                "source_id": fid,
                "project_code": p_code,
                "date": date,
                "created_by": created_by,
                "text": t,
                "version": version_tag,
                "ingestion_job_id": job_id
            })
    return chunks

def extract_chunks_from_tasks(rows, tokenizer, version_tag: str, job_id: str):
    chunks = []
    for r in rows:
        tid = r.get("id") or ""
        p_code = r.get("project_code") or ""
        name = r.get("name") or ""
        notes = r.get("notes") or ""
        date = r.get("due_date") or r.get("created_at") or ""
        
        if not (notes and len(notes.strip()) > 10):
            continue
            
        cleaned_notes = clean_and_extract_threads(notes)
        full_text = f"Hạng mục: {name.strip()}\nGhi chú kỹ thuật: {cleaned_notes}".strip()
        
        split_texts = split_text_by_tokenizer(full_text, tokenizer)
        for idx, t in enumerate(split_texts):
            chunks.append({
                "chunk_id": f"tasks_{tid}_{idx}_{version_tag}",
                "source_table": "tasks",
                "source_id": tid,
                "project_code": p_code,
                "date": date[:10] if date else "",
                "created_by": "",
                "text": t,
                "version": version_tag,
                "ingestion_job_id": job_id
            })
    return chunks

def extract_chunks_from_material_plans(rows, tokenizer, version_tag: str, job_id: str):
    chunks = []
    for r in rows:
        mid = r.get("id") or ""
        p_code = r.get("project_code") or ""
        content = r.get("job_content") or ""
        notes = r.get("notes") or ""
        origin = r.get("tech_spec_origin") or ""
        date = r.get("created_at") or ""
        
        cleaned_notes = clean_and_extract_threads(notes) if notes else ""
        
        parts = []
        if content:
            parts.append(f"Vật tư / Thiết bị: {content.strip()}")
        if origin:
            parts.append(f"Yêu cầu xuất xứ / Tiêu chuẩn kỹ thuật: {origin.strip()}")
        if cleaned_notes and len(cleaned_notes.strip()) > 5:
            parts.append(f"Ghi chú: {cleaned_notes.strip()}")
            
        full_text = "\n".join(parts).strip()
        if not (full_text and len(full_text) > 15):
            continue
            
        split_texts = split_text_by_tokenizer(full_text, tokenizer)
        for idx, t in enumerate(split_texts):
            chunks.append({
                "chunk_id": f"material_plans_{mid}_{idx}_{version_tag}",
                "source_table": "material_plans",
                "source_id": mid,
                "project_code": p_code,
                "date": date[:10] if date else "",
                "created_by": "",
                "text": t,
                "version": version_tag,
                "ingestion_job_id": job_id
            })
    return chunks

def chunk_list(lst, size=200):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def perform_pre_swap_smoke_tests(session, staging_version: str, sample_chunk: dict) -> bool:
    """
    Smoke test retrieval thực tế trên Vector Index và Fulltext Index bằng dữ liệu đã biết của staging.
    Xác nhận chính xác sample_id của chunk mẫu xuất hiện trong kết quả top-k để khẳng định HNSW index
    đã index xong chunk vừa nạp.
    """
    logger.info("🧪 Đang thực hiện Pre-swap Smoke Test Retrieval trên chính phiên bản staging...")
    
    sample_text = sample_chunk["text"]
    sample_embedding = sample_chunk["embedding"]
    sample_id = sample_chunk["chunk_id"]

    # 1. Smoke test Vector Index - Phải tìm thấy chính xác sample_id trong top candidates
    vector_test_cypher = """
    CALL db.index.vector.queryNodes('internal_chunk_vector_index', 50, $sample_vec)
    YIELD node, score
    WHERE node.version = $version
    RETURN node.chunk_id AS chunk_id, score
    LIMIT 20
    """
    try:
        v_results = session.run(vector_test_cypher, sample_vec=sample_embedding, version=staging_version).data()
        retrieved_ids = [r["chunk_id"] for r in v_results]
        if sample_id not in retrieved_ids:
            logger.error(f"❌ SMOKE TEST VECTOR FAILED: sample_id '{sample_id}' không xuất hiện trong top kết quả vector ({retrieved_ids[:5]})!")
            return False
        
        sample_score = next(r["score"] for r in v_results if r["chunk_id"] == sample_id)
        logger.info(f"   ✅ Vector Index Test PASSED: Đã định vị chính xác sample_id '{sample_id}' (Score: {sample_score:.4f}).")
    except Exception as e:
        logger.error(f"❌ SMOKE TEST VECTOR ERROR: {e}")
        return False

    # 2. Smoke test Fulltext Index với từ khóa thực tế từ text của sample_chunk
    # Lấy các từ có độ dài >= 4 từ text thực tế
    words = [w for w in sample_text.split() if len(w) >= 4 and w.isalnum()]
    if not words:
        words = ["công", "việc", "thiết", "bị"]
    search_keyword = words[0]

    ft_test_cypher = """
    CALL db.index.fulltext.queryNodes('internal_chunk_fulltext_index', $kw)
    YIELD node, score
    WHERE node.version = $version
    RETURN node.chunk_id AS chunk_id, score
    LIMIT 20
    """
    try:
        ft_results = session.run(ft_test_cypher, kw=search_keyword, version=staging_version).data()
        ft_retrieved_ids = [r["chunk_id"] for r in ft_results]
        if not ft_results:
            logger.error(f"❌ SMOKE TEST FULLTEXT FAILED: Không tìm thấy kết quả cho từ khóa '{search_keyword}' trong staging version!")
            return False
        logger.info(f"   ✅ Fulltext Index Test PASSED: Tìm thấy {len(ft_results)} chunk(s) với từ khóa '{search_keyword}'.")
    except Exception as e:
        logger.error(f"❌ SMOKE TEST FULLTEXT ERROR: {e}")
        return False

    return True

def cleanup_old_version_async(neo4j_uri: str, neo4j_auth: tuple, old_version: str):
    """
    Tiến trình dọn dẹp phiên bản cũ chạy trên thread riêng VỚI DRIVER ĐỘC LẬP.
    Tránh hoàn toàn race condition khi main thread đóng driver chính trong finally block.
    """
    def _do_cleanup():
        logger.info(f"🧹 [Background Worker] Bắt đầu dọn dẹp các node thuộc phiên bản cũ ({old_version})...")
        bg_driver = None
        try:
            bg_driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
            with bg_driver.session() as session:
                session.run("MATCH (c:InternalChunk {version: $old_ver}) DETACH DELETE c", old_ver=old_version)
            logger.info(f"✅ [Background Worker] Đã hoàn thành dọn dẹp phiên bản cũ {old_version}.")
        except Exception as e:
            logger.error(f"⚠️ [Background Worker] Lỗi khi dọn dẹp phiên bản cũ {old_version}: {e}")
        finally:
            if bg_driver:
                bg_driver.close()

    t = threading.Thread(target=_do_cleanup, daemon=False)
    t.start()

def main():
    job_id = f"job_{uuid.uuid4().hex[:8]}"
    version_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("===============================================================")
    logger.info(f"🚀 BẮT ĐẦU SAFE INGESTION CHO DỮ LIỆU NỘI BỘ [Job ID: {job_id}] [Version: {version_tag}]")
    logger.info("===============================================================")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    lock_mgr = IngestionLockManager(driver, job_id, version_tag)

    # 1. Kiểm tra và chiếm Ingestion Lock (Atomic Conditional Update)
    if not lock_mgr.acquire_lock():
        driver.close()
        sys.exit(1)

    try:
        # 2. Khởi tạo tokenizer & embedder
        logger.info(f"🧠 Đang nạp Tokenizer & Mô hình '{EMBEDDING_MODEL_NAME}'...")
        tokenizer = AutoTokenizer.from_pretrained(f"sentence-transformers/{EMBEDDING_MODEL_NAME}")
        embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

        # 3. Đọc dữ liệu đã khử nhạy cảm
        field_logs_rows = read_sanitized_csv("field_logs.csv")
        tasks_rows = read_sanitized_csv("tasks.csv")
        mat_plans_rows = read_sanitized_csv("material_plans.csv")

        chunks = []
        chunks.extend(extract_chunks_from_field_logs(field_logs_rows, tokenizer, version_tag, job_id))
        chunks.extend(extract_chunks_from_tasks(tasks_rows, tokenizer, version_tag, job_id))
        chunks.extend(extract_chunks_from_material_plans(mat_plans_rows, tokenizer, version_tag, job_id))

        expected_count = len(chunks)
        logger.info(f"📄 Đã trích xuất tổng cộng {expected_count} Internal Chunks cho phiên bản {version_tag} [Job ID: {job_id}]:")
        logger.info(f"   - field_logs: {len([c for c in chunks if c['source_table'] == 'field_logs'])}")
        logger.info(f"   - tasks: {len([c for c in chunks if c['source_table'] == 'tasks'])}")
        logger.info(f"   - material_plans: {len([c for c in chunks if c['source_table'] == 'material_plans'])}")

        if expected_count == 0:
            logger.error("Không có chunk nào được trích xuất. Hủy bỏ nạp.")
            lock_mgr.release_lock_on_failure()
            driver.close()
            return

        # 4. Tính toán Vector Embedding cho phiên bản mới
        texts_to_embed = [c["text"] for c in chunks]
        logger.info(f"⚡ Đang tính toán Vector Embedding cho {len(texts_to_embed)} chunks...")
        embeddings = embedder.encode(texts_to_embed, batch_size=64, show_progress_bar=True).tolist()

        for i, emb in enumerate(embeddings):
            chunks[i]["embedding"] = emb

        # 5. Nạp Staging vào Neo4j (KHÔNG XÓA DỮ LIỆU CŨ KHI NẠP)
        with driver.session() as session:
            init_internal_indexes(session)

            logger.info(f"📥 Đang nạp {expected_count} chunks vào vùng Staging (version: {version_tag}, job_id: {job_id}, token: {lock_mgr.fencing_token})...")
            
            # Tách bạch rõ ràng:
            # 1. Ghi InternalChunk nguyên tử có gắn Fencing Token và đếm chính xác số chunk đã ghi
            # 2. Tạo quan hệ HAS_INTERNAL_CHUNK bằng OPTIONAL MATCH / WITH riêng biệt để không lọc mất chunk
            fenced_ingest_cypher = """
            MATCH (s:SystemConfig {id: 'internal_rag_state'})
            WHERE s.lock_job_id = $job_id AND s.lock_generation = $token
            WITH s
            UNWIND $batch AS row
            MERGE (c:InternalChunk {chunk_id: row.chunk_id})
            SET c.source_table = row.source_table,
                c.source_id = row.source_id,
                c.project_code = row.project_code,
                c.date = row.date,
                c.created_by = row.created_by,
                c.text = row.text,
                c.embedding = row.embedding,
                c.version = row.version,
                c.ingestion_job_id = row.ingestion_job_id
            WITH c, row
            OPTIONAL MATCH (p:Project {code: row.project_code})
            FOREACH (_ IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |
                MERGE (p)-[:HAS_INTERNAL_CHUNK]->(c)
            )
            RETURN count(c) AS written_chunks
            """

            for batch_idx, batch in enumerate(chunk_list(chunks, 200), 1):
                write_res = session.run(
                    fenced_ingest_cypher,
                    job_id=job_id,
                    token=lock_mgr.fencing_token,
                    batch=batch
                ).single()

                written_count = write_res["written_chunks"] if write_res else 0
                if written_count != len(batch):
                    # Chẩn đoán chính xác nguyên nhân: Kiểm tra xem lock có bị mất/takeover không
                    is_owner = lock_mgr.verify_ownership()
                    if not is_owner:
                        raise RuntimeError(
                            f"🛑 FENCING TOKEN VIOLATION TẠI BATCH {batch_idx}: "
                            f"Ghi được {written_count}/{len(batch)} chunks vì job đã mất quyền sở hữu lock! DỪNG TIẾN TRÌNH."
                        )
                    else:
                        raise RuntimeError(
                            f"⚠️ BATCH WRITE INTEGRITY FAILURE TẠI BATCH {batch_idx}: "
                            f"Số lượng chunk ghi nhận ({written_count}) không khớp batch ({len(batch)}) dù lock vẫn hợp lệ!"
                        )

            # 6. XÁC MINH 1: Tính toàn vẹn số lượng chunks (kiểm tra theo cả version và ingestion_job_id)
            logger.info("🔍 Đang xác minh tính toàn vẹn số lượng chunk của phiên bản mới...")
            verify_query = """
            MATCH (c:InternalChunk {version: $version, ingestion_job_id: $job_id})
            RETURN count(c) AS loaded_count
            """
            loaded_count = session.run(verify_query, version=version_tag, job_id=job_id).single()["loaded_count"]
            logger.info(f"   -> Kết quả: Đã nạp thành công {loaded_count}/{expected_count} chunks.")

            if loaded_count != expected_count:
                raise RuntimeError(f"Số lượng chunks nạp ({loaded_count}) không khớp dự kiến ({expected_count})")

            # 7. XÁC MINH 2: PRE-SWAP SMOKE TEST RETRIEVAL (Vector + Fulltext)
            if not lock_mgr.verify_ownership():
                raise RuntimeError("Mất quyền sở hữu Ingestion Lock trước Smoke Test!")

            sample_chunk = chunks[0]
            smoke_ok = perform_pre_swap_smoke_tests(session, version_tag, sample_chunk)
            if not smoke_ok:
                raise RuntimeError("Pre-swap Smoke Test Retrieval thất bại! Indexes chưa sẵn sàng truy xuất.")

            # 8. ATOMIC SWAP ACTIVE POINTER TRONG TRANSACTION (Có ràng buộc Fencing Token)
            logger.info("🔄 Đang thực hiện Atomic Pointer Swap chuyển active_version...")
            swap_query = """
            MATCH (s:SystemConfig {id: 'internal_rag_state'})
            WHERE s.lock_job_id = $job_id AND s.lock_generation = $token
            SET s.previous_version = coalesce(s.active_version, 'none'),
                s.active_version = $version,
                s.staging_version = null,
                s.ingestion_status = 'IDLE',
                s.lock_job_id = null,
                s.lock_acquired_at = null,
                s.last_heartbeat_at = null,
                s.updated_at = $updated_at,
                s.total_chunks = $total_chunks
            RETURN s.active_version AS active_version, s.previous_version AS previous_version
            """
            res = session.run(
                swap_query, 
                job_id=job_id,
                token=lock_mgr.fencing_token,
                version=version_tag, 
                updated_at=datetime.now().isoformat(),
                total_chunks=loaded_count
            ).single()

            if not res:
                raise RuntimeError(f"Không thể swap active pointer: job_id {job_id} hoặc fencing_token {lock_mgr.fencing_token} không còn hợp lệ!")

            active_ver = res["active_version"]
            prev_ver = res["previous_version"]
            logger.info(f"✅ ĐÃ CHUYỂN ACTIVE POINTER THÀNH CÔNG: active_version = '{active_ver}' (cũ: '{prev_ver}')")
            lock_mgr.stop_heartbeat()

            # 9. BÀN GIAO DỌN DẸP PHIÊN BẢN CŨ SANG TIẾN TRÌNH NỀN ASYNC (Driver riêng)
            if prev_ver and prev_ver != "none" and prev_ver != version_tag:
                cleanup_old_version_async(NEO4J_URI, (NEO4J_USER, NEO4J_PASSWORD), prev_ver)

        logger.info("===============================================================")
        logger.info(f"🎉 HOÀN THÀNH SAFE INGESTION THÀNH CÔNG (VERSION: {version_tag})!")
        logger.info("===============================================================")

    except Exception as e:
        logger.error(f"❌ LỖI TRONG QUÁ TRÌNH INGESTION: {e}")
        lock_mgr.release_lock_on_failure()
        sys.exit(1)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
