import sys
import os
import csv
import logging
import time
import json
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

# Giới hạn an toàn token cho tokenizer của all-MiniLM-L6-v2 (model max: 256 / tokenizer max 512)
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
    """
    Tách bỏ chuỗi raw json [THREAD:[...]] và chỉ trích xuất các trao đổi công việc thực sự.
    Loại bỏ các tin nhắn ngắn rác/vô nghĩa (ví dụ: 'đúng', 'dạ anh', '').
    """
    if not notes:
        return ""
    idx = notes.find("[THREAD:")
    if idx == -1:
        return notes.strip()
    
    prefix = notes[:idx].strip()
    rest = notes[idx + len("[THREAD:"):].strip()
    
    # Bỏ tag [tech-status:...] nếu có
    if "[tech-status:" in rest:
        rest = rest[:rest.find("[tech-status:")].strip()
    if rest.endswith("]"):
        rest = rest[:-1].strip()
        
    meaningful = []
    try:
        messages = json.loads(rest)
        for m in messages:
            content = (m.get("content") or "").strip()
            # Bỏ qua tin rác không mang ngữ nghĩa công việc
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
    """
    Chia nhỏ text dựa trên tokenizer thực tế của mô hình embedding.
    Nếu vượt quá max_tokens, tạo sliding window với overlap để bảo toàn ngữ cảnh.
    """
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
    """
    Khai báo constraint và Vector/Fulltext Index chuyên biệt cho InternalChunk.
    Đợi index đạt trạng thái ONLINE theo đúng tài liệu Neo4j.
    """
    logger.info("🔧 Đang khởi tạo Constraint & Index cho InternalChunk trên Neo4j...")
    
    session.run("""
        CREATE CONSTRAINT internal_chunk_id_unique IF NOT EXISTS
        FOR (c:InternalChunk) REQUIRE c.chunk_id IS UNIQUE
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

def extract_chunks_from_field_logs(rows, tokenizer):
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
                "chunk_id": f"field_logs_{fid}_{idx}",
                "source_table": "field_logs",
                "source_id": fid,
                "project_code": p_code,
                "date": date,
                "created_by": created_by,
                "text": t
            })
    return chunks

def extract_chunks_from_tasks(rows, tokenizer):
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
                "chunk_id": f"tasks_{tid}_{idx}",
                "source_table": "tasks",
                "source_id": tid,
                "project_code": p_code,
                "date": date[:10] if date else "",
                "created_by": "",
                "text": t
            })
    return chunks

def extract_chunks_from_material_plans(rows, tokenizer):
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
                "chunk_id": f"material_plans_{mid}_{idx}",
                "source_table": "material_plans",
                "source_id": mid,
                "project_code": p_code,
                "date": date[:10] if date else "",
                "created_by": "",
                "text": t
            })
    return chunks

def chunk_list(lst, size=200):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def main():
    logger.info("===============================================================")
    logger.info("🚀 BẮT ĐẦU EXTRACTIVE RAG INGESTION DỮ LIỆU NỘI BỘ (TOKENIZER CHUNKING)")
    logger.info("===============================================================")

    # 1. Khởi tạo tokenizer của mô hình embedding
    logger.info(f"🧠 Đang nạp Tokenizer & Mô hình '{EMBEDDING_MODEL_NAME}'...")
    tokenizer = AutoTokenizer.from_pretrained(f"sentence-transformers/{EMBEDDING_MODEL_NAME}")
    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    # 2. Đọc dữ liệu đã khử nhạy cảm
    field_logs_rows = read_sanitized_csv("field_logs.csv")
    tasks_rows = read_sanitized_csv("tasks.csv")
    mat_plans_rows = read_sanitized_csv("material_plans.csv")

    chunks = []
    chunks.extend(extract_chunks_from_field_logs(field_logs_rows, tokenizer))
    chunks.extend(extract_chunks_from_tasks(tasks_rows, tokenizer))
    chunks.extend(extract_chunks_from_material_plans(mat_plans_rows, tokenizer))

    logger.info(f"📄 Đã trích xuất tổng cộng {len(chunks)} Internal Chunks (sau khi làm sạch THREAD & tokenize chunking):")
    logger.info(f"   - field_logs: {len([c for c in chunks if c['source_table'] == 'field_logs'])}")
    logger.info(f"   - tasks: {len([c for c in chunks if c['source_table'] == 'tasks'])}")
    logger.info(f"   - material_plans: {len([c for c in chunks if c['source_table'] == 'material_plans'])}")

    if not chunks:
        logger.warning("Không có chunk nào được trích xuất.")
        return

    # 3. Tính toán Vector Embedding cho các chunks
    texts_to_embed = [c["text"] for c in chunks]
    logger.info(f"⚡ Đang tính toán Vector Embedding cho {len(texts_to_embed)} chunks...")
    embeddings = embedder.encode(texts_to_embed, batch_size=64, show_progress_bar=True).tolist()

    for i, emb in enumerate(embeddings):
        chunks[i]["embedding"] = emb

    # 4. Dọn dẹp các node InternalChunk cũ và nạp lại bằng MERGE (Idempotent)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        init_internal_indexes(session)

        # Xóa các InternalChunk cũ để đồng bộ ID mới theo chunk_index chuẩn xác
        logger.info("🧹 Đang dọn dẹp các node InternalChunk cũ trên Neo4j...")
        session.run("MATCH (c:InternalChunk) DETACH DELETE c")

        logger.info("📥 Đang lưu các InternalChunk chuẩn hóa vào Neo4j...")
        ingest_cypher = """
        UNWIND $batch AS row
        MERGE (c:InternalChunk {chunk_id: row.chunk_id})
        SET c.source_table = row.source_table,
            c.source_id = row.source_id,
            c.project_code = row.project_code,
            c.date = row.date,
            c.created_by = row.created_by,
            c.text = row.text,
            c.embedding = row.embedding
        WITH c, row
        WHERE row.project_code IS NOT NULL AND row.project_code <> ''
        MATCH (p:Project {code: row.project_code})
        MERGE (p)-[:HAS_INTERNAL_CHUNK]->(c)
        """

        for batch in chunk_list(chunks, 200):
            session.run(ingest_cypher, batch=batch)

        # Thống kê sau nạp
        cnt = session.run("MATCH (c:InternalChunk) RETURN count(c) AS count").single()["count"]
        rel_cnt = session.run("MATCH ()-[r:HAS_INTERNAL_CHUNK]->() RETURN count(r) AS count").single()["count"]
        logger.info(f"✅ Tổng số Node InternalChunk trên Neo4j: {cnt}")
        logger.info(f"🔗 Tổng số quan hệ [:HAS_INTERNAL_CHUNK] nối với Project: {rel_cnt}")

    driver.close()
    logger.info("===============================================================")
    logger.info("🎉 HOÀN THÀNH INGESTION EXTRACTIVE RAG CHO DỮ LIỆU NỘI BỘ!")
    logger.info("===============================================================")

if __name__ == "__main__":
    main()
