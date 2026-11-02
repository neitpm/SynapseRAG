import sys
import os
import csv
import logging
import time
import json
from datetime import datetime
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

def main():
    version_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("===============================================================")
    logger.info(f"🚀 BẮT ĐẦU VERSIONED INGESTION CHO DỮ LIỆU NỘI BỘ (VERSION: {version_tag})")
    logger.info("===============================================================")

    # 1. Khởi tạo tokenizer & embedder
    logger.info(f"🧠 Đang nạp Tokenizer & Mô hình '{EMBEDDING_MODEL_NAME}'...")
    tokenizer = AutoTokenizer.from_pretrained(f"sentence-transformers/{EMBEDDING_MODEL_NAME}")
    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    # 2. Đọc dữ liệu đã khử nhạy cảm
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
        return

    # 3. Tính toán Vector Embedding cho phiên bản mới
    texts_to_embed = [c["text"] for c in chunks]
    logger.info(f"⚡ Đang tính toán Vector Embedding cho {len(texts_to_embed)} chunks...")
    embeddings = embedder.encode(texts_to_embed, batch_size=64, show_progress_bar=True).tolist()

    for i, emb in enumerate(embeddings):
        chunks[i]["embedding"] = emb

    # 4. Nạp Staging vào Neo4j (KHÔNG XÓA DỮ LIỆU CŨ KHI NẠP)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
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

        # 5. XÁC MINH TÍNH TOÀN VẸN TRƯỚC KHI CHUYỂN ACTIVE POINTER
        logger.info("🔍 Đang xác minh tính toàn vẹn số lượng chunk của phiên bản mới...")
        verify_query = """
        MATCH (c:InternalChunk {version: $version})
        RETURN count(c) AS loaded_count
        """
        loaded_count = session.run(verify_query, version=version_tag).single()["loaded_count"]
        logger.info(f"   -> Kết quả: Đã nạp thành công {loaded_count}/{expected_count} chunks.")

        if loaded_count != expected_count:
            logger.error(f"❌ THẤT BẠI: Số lượng chunks nạp ({loaded_count}) không khớp dự kiến ({expected_count}). "
                         f"HỦY BỎ CHUYỂN ACTIVE POINTER. Phiên bản cũ vẫn đang hoạt động an toàn.")
            driver.close()
            return

        # 6. ATOMIC SWAP ACTIVE POINTER TRONG MỘT TRANSACTION
        logger.info("🔄 Đang thực hiện Atomic Pointer Swap chuyển active_version...")
        swap_query = """
        MERGE (s:SystemConfig {id: 'internal_rag_state'})
        SET s.previous_version = coalesce(s.active_version, 'none'),
            s.active_version = $version,
            s.updated_at = $updated_at,
            s.total_chunks = $total_chunks
        RETURN s.active_version AS active_version, s.previous_version AS previous_version
        """
        res = session.run(
            swap_query, 
            version=version_tag, 
            updated_at=datetime.now().isoformat(),
            total_chunks=loaded_count
        ).single()
        logger.info(f"✅ ĐÃ CHUYỂN ACTIVE POINTER THÀNH CÔNG: active_version = '{res['active_version']}' (cũ: '{res['previous_version']}')")

        # 7. DỌN DẸP PHIÊN BẢN CŨ TIẾN TRÌNH NỀN
        prev_ver = res["previous_version"]
        if prev_ver and prev_ver != "none" and prev_ver != version_tag:
            logger.info(f"🧹 Đang dọn dẹp các node thuộc phiên bản cũ ({prev_ver})...")
            session.run("MATCH (c:InternalChunk {version: $old_ver}) DETACH DELETE c", old_ver=prev_ver)
            logger.info(f"✅ Đã dọn dẹp xong phiên bản cũ {prev_ver}.")
        else:
            # Dọn dẹp các chunk không có version (từ phiên bản nạp ban đầu)
            session.run("MATCH (c:InternalChunk) WHERE c.version IS NULL DETACH DELETE c")

    driver.close()
    logger.info("===============================================================")
    logger.info(f"🎉 HOÀN THÀNH ATOMIC ACTIVE POINTER INGESTION (VERSION: {version_tag})!")
    logger.info("===============================================================")

if __name__ == "__main__":
    main()
