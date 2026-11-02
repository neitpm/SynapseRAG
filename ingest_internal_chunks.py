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
    Quản lý khóa phân tán Ingestion Lock với job_id, atomic conditional update và heartbeat.
    """
    def __init__(self, driver, job_id: str, staging_version: str):
        self.driver = driver
        self.job_id = job_id
        self.staging_version = staging_version
        self._stop_heartbeat = threading.Event()
        self._heartbeat_thread = None

    def acquire_lock(self) -> bool:
        """
        Thực hiện Cypher Atomic Conditional Update:
        Chỉ acquire được nếu:
        - Chưa có node SystemConfig, HOẶC
        - lock_job_id IS NULL, HOẶC
        - lock_job_id = $job_id (re-entrant), HOẶC
        - ingestion_status IN ['IDLE', 'FAILED']
        LƯU Ý QUAN TRỌNG: Nếu job khác đang giữ lock và chưa release, DÙ quá 30 phút cũng KHÔNG
        tự ý ghi đè lock hoặc xóa dữ liệu của nó (tuân thủ nguyên tắc an toàn).
        """
        now_ts = int(time.time())
        acquire_cypher = """
        MERGE (s:SystemConfig {id: 'internal_rag_state'})
        WITH s
        WHERE s.lock_job_id IS NULL 
           OR s.lock_job_id = $job_id
           OR coalesce(s.ingestion_status, 'IDLE') IN ['IDLE', 'FAILED']
        SET s.lock_job_id = $job_id,
            s.staging_version = $staging_version,
            s.ingestion_status = 'INGESTING',
            s.lock_acquired_at = $now_ts,
            s.last_heartbeat_at = $now_ts
        RETURN s.lock_job_id AS lock_job_id, s.staging_version AS staging_version
        """
        with self.driver.session() as session:
            rec = session.run(acquire_cypher, job_id=self.job_id, staging_version=self.staging_version, now_ts=now_ts).single()
            if rec and rec["lock_job_id"] == self.job_id:
                logger.info(f"🔒 Đã chiếm Ingestion Lock thành công [job_id: {self.job_id}, staging_version: {self.staging_version}]")
                self._start_heartbeat()
                return True
            else:
                # Kiểm tra job đang chiếm giữ lock để log chi tiết
                cur = session.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) RETURN s").single()
                cur_data = dict(cur["s"]) if cur else {}
                logger.error(
                    f"⛔ KHÔNG THỂ CHIẾM LOCK: Hệ thống đang bị chiếm bởi job khác "
                    f"[job_id: {cur_data.get('lock_job_id')}, status: {cur_data.get('ingestion_status')}, "
                    f"acquired_at: {cur_data.get('lock_acquired_at')}]. Từ chối chạy để tránh xung đột."
                )
                return False

    def _start_heartbeat(self):
        def _beat():
            while not self._stop_heartbeat.wait(HEARTBEAT_INTERVAL_SECONDS):
                now_ts = int(time.time())
                beat_cypher = """
                MATCH (s:SystemConfig {id: 'internal_rag_state'})
                WHERE s.lock_job_id = $job_id
                SET s.last_heartbeat_at = $now_ts
                """
                try:
                    with self.driver.session() as session:
                        session.run(beat_cypher, job_id=self.job_id, now_ts=now_ts)
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
        Khi job thất bại, chỉ giải phóng nếu chính job_id này đang nắm lock.
        Đổi trạng thái sang FAILED và dọn dẹp các chunk staging thuộc staging_version của job này.
        """
        self.stop_heartbeat()
        release_cypher = """
        MATCH (s:SystemConfig {id: 'internal_rag_state'})
        WHERE s.lock_job_id = $job_id
        SET s.ingestion_status = 'FAILED',
            s.failed_version = $staging_version,
            s.staging_version = null,
            s.lock_job_id = null,
            s.lock_acquired_at = null,
            s.last_heartbeat_at = null
        RETURN s.id
        """
        with self.driver.session() as session:
            res = session.run(release_cypher, job_id=self.job_id, staging_version=self.staging_version).single()
            if res:
                logger.info(f"🔓 Đã chuyển trạng thái FAILED và giải phóng lock [job_id: {self.job_id}]")
                # Xóa sạch toàn bộ staging chunks của job bị fail để tránh làm bẩn vector index
                logger.info(f"🧹 Đang xóa các chunk staging của phiên bản lỗi ({self.staging_version})...")
                session.run("MATCH (c:InternalChunk {version: $ver}) DETACH DELETE c", ver=self.staging_version)
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

def extract_chunks_from_field_logs(rows, tokenizer, version_tag: str):
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
                "version": version_tag
            })
    return chunks

def extract_chunks_from_tasks(rows, tokenizer, version_tag: str):
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
                "version": version_tag
            })
    return chunks

def extract_chunks_from_material_plans(rows, tokenizer, version_tag: str):
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
                "version": version_tag
            })
    return chunks

def chunk_list(lst, size=200):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def perform_pre_swap_smoke_tests(session, staging_version: str, sample_chunk: dict) -> bool:
    """
    Smoke test retrieval thực tế trên Vector Index và Fulltext Index bằng dữ liệu đã biết của staging.
    Overfetch 50 và kiểm tra xem chunk mẫu có nằm trong kết quả query không.
    """
    logger.info("🧪 Đang thực hiện Pre-swap Smoke Test Retrieval trên chính phiên bản staging...")
    
    sample_text = sample_chunk["text"]
    sample_embedding = sample_chunk["embedding"]
    sample_id = sample_chunk["chunk_id"]

    # 1. Smoke test Vector Index
    vector_test_cypher = """
    CALL db.index.vector.queryNodes('internal_chunk_vector_index', 50, $sample_vec)
    YIELD node, score
    WHERE node.version = $version
    RETURN node.chunk_id AS chunk_id, score
    LIMIT 10
    """
    try:
        v_results = session.run(vector_test_cypher, sample_vec=sample_embedding, version=staging_version).data()
        if not v_results:
            logger.error("❌ SMOKE TEST VECTOR FAILED: Vector index không trả về chunk nào thuộc staging version!")
            return False
        logger.info(f"   ✅ Vector Index Test PASSED: Truy xuất thành công {len(v_results)} chunk(s) (Top score: {v_results[0]['score']:.4f}).")
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
    LIMIT 10
    """
    try:
        ft_results = session.run(ft_test_cypher, kw=search_keyword, version=staging_version).data()
        if not ft_results:
            logger.error(f"❌ SMOKE TEST FULLTEXT FAILED: Không tìm thấy kết quả cho từ khóa '{search_keyword}' trong staging version!")
            return False
        logger.info(f"   ✅ Fulltext Index Test PASSED: Tìm thấy {len(ft_results)} chunk(s) với từ khóa '{search_keyword}'.")
    except Exception as e:
        logger.error(f"❌ SMOKE TEST FULLTEXT ERROR: {e}")
        return False

    return True

def cleanup_old_version_async(driver, old_version: str):
    """
    Tiến trình dọn dẹp phiên bản cũ chạy bất đồng bộ (background thread)
    tránh block job ingestion chính.
    """
    def _do_cleanup():
        logger.info(f"🧹 [Background Worker] Bắt đầu dọn dẹp các node thuộc phiên bản cũ ({old_version})...")
        try:
            with driver.session() as session:
                session.run("MATCH (c:InternalChunk {version: $old_ver}) DETACH DELETE c", old_ver=old_version)
            logger.info(f"✅ [Background Worker] Đã hoàn thành dọn dẹp phiên bản cũ {old_version}.")
        except Exception as e:
            logger.error(f"⚠️ [Background Worker] Lỗi khi dọn dẹp phiên bản cũ {old_version}: {e}")

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
        chunks.extend(extract_chunks_from_field_logs(field_logs_rows, tokenizer, version_tag))
        chunks.extend(extract_chunks_from_tasks(tasks_rows, tokenizer, version_tag))
        chunks.extend(extract_chunks_from_material_plans(mat_plans_rows, tokenizer, version_tag))

        expected_count = len(chunks)
        logger.info(f"📄 Đã trích xuất tổng cộng {expected_count} Internal Chunks cho phiên bản {version_tag}:")
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

            logger.info(f"📥 Đang nạp {expected_count} chunks vào vùng Staging (version: {version_tag})...")
            ingest_cypher = """
            UNWIND $batch AS row
            MERGE (c:InternalChunk {chunk_id: row.chunk_id})
            SET c.source_table = row.source_table,
                c.source_id = row.source_id,
                c.project_code = row.project_code,
                c.date = row.date,
                c.created_by = row.created_by,
                c.text = row.text,
                c.embedding = row.embedding,
                c.version = row.version
            WITH c, row
            WHERE row.project_code IS NOT NULL AND row.project_code <> ''
            MATCH (p:Project {code: row.project_code})
            MERGE (p)-[:HAS_INTERNAL_CHUNK]->(c)
            """

            for batch in chunk_list(chunks, 200):
                session.run(ingest_cypher, batch=batch)

            # 6. XÁC MINH 1: Tính toàn vẹn số lượng chunks
            logger.info("🔍 Đang xác minh tính toàn vẹn số lượng chunk của phiên bản mới...")
            verify_query = """
            MATCH (c:InternalChunk {version: $version})
            RETURN count(c) AS loaded_count
            """
            loaded_count = session.run(verify_query, version=version_tag).single()["loaded_count"]
            logger.info(f"   -> Kết quả: Đã nạp thành công {loaded_count}/{expected_count} chunks.")

            if loaded_count != expected_count:
                raise RuntimeError(f"Số lượng chunks nạp ({loaded_count}) không khớp dự kiến ({expected_count})")

            # 7. XÁC MINH 2: PRE-SWAP SMOKE TEST RETRIEVAL (Vector + Fulltext)
            sample_chunk = chunks[0]
            smoke_ok = perform_pre_swap_smoke_tests(session, version_tag, sample_chunk)
            if not smoke_ok:
                raise RuntimeError("Pre-swap Smoke Test Retrieval thất bại! Indexes chưa sẵn sàng truy xuất.")

            # 8. ATOMIC SWAP ACTIVE POINTER TRONG TRANSACTION
            logger.info("🔄 Đang thực hiện Atomic Pointer Swap chuyển active_version...")
            swap_query = """
            MATCH (s:SystemConfig {id: 'internal_rag_state'})
            WHERE s.lock_job_id = $job_id
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
                version=version_tag, 
                updated_at=datetime.now().isoformat(),
                total_chunks=loaded_count
            ).single()

            if not res:
                raise RuntimeError(f"Không thể swap active pointer: job_id {job_id} không còn sở hữu lock!")

            active_ver = res["active_version"]
            prev_ver = res["previous_version"]
            logger.info(f"✅ ĐÃ CHUYỂN ACTIVE POINTER THÀNH CÔNG: active_version = '{active_ver}' (cũ: '{prev_ver}')")
            lock_mgr.stop_heartbeat()

            # 9. BÀN GIAO DỌN DẸP PHIÊN BẢN CŨ SANG TIẾN TRÌNH NỀN ASYNC
            if prev_ver and prev_ver != "none" and prev_ver != version_tag:
                cleanup_old_version_async(driver, prev_ver)

        logger.info("===============================================================")
        logger.info(f"🎉 HOÀN THÀNH SAFE INGESTION THÀNH CÔNG (VERSION: {version_tag})!")
        logger.info("===============================================================")

    except Exception as e:
        logger.error(f"❌ LỖI TRONG QUÁ TRÌNH INGESTION: {e}")
        lock_mgr.release_lock_on_failure()
        driver.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
