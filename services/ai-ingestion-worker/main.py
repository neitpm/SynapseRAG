import sys
import json
import logging
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase

sys.stdout.reconfigure(encoding="utf-8")

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai-ingestion-worker")

class AIIngestionWorker:
    def __init__(self):
        # 1. Khởi tạo Kafka Consumer (Consumer Group riêng biệt cho Neo4j GraphRAG)
        self.consumer_conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "graphrag-ingestion-group-v2",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit chỉ khi lưu Neo4j thành công
        }
        self.consumer = Consumer(self.consumer_conf)
        self.topic = settings.KAFKA_TOPIC_WIKI_RAW
        self.consumer.subscribe([self.topic])

        # 2. Khởi tạo Neo4j Driver & Constraints & Vector Index
        logger.info(f"Đang kết nối Neo4j ({settings.NEO4J_URI})...")
        self.neo4j_driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
        )
        self._init_neo4j_schema()

        # 3. Khởi tạo Text Splitter (Chunking Engine)
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,
            chunk_overlap=50,
            separators=["\n\n", "\n", ". ", " ", ""]
        )

        # 4. Khởi tạo Mô hình Embedding
        logger.info(f"Đang tải mô hình Embedding '{settings.EMBEDDING_MODEL}' (384 dimensions)...")
        self.embedder = SentenceTransformer(settings.EMBEDDING_MODEL)
        logger.info("Mô hình Embedding và Neo4j đã sẵn sàng!")

    def _init_neo4j_schema(self):
        """Khởi tạo Constraint & Vector Index trên Neo4j 5.x"""
        with self.neo4j_driver.session() as session:
            try:
                session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Article) REQUIRE a.page_id IS UNIQUE")
                session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE")
                session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (cat:Category) REQUIRE cat.name IS UNIQUE")
                session.run("""
                    CALL db.index.vector.createNodeIndex(
                        'chunk_vector_index',
                        'Chunk',
                        'embedding',
                        384,
                        'cosine'
                    )
                """)
                logger.info("✅ Neo4j Vector Index & Constraints đã sẵn sàng.")
            except Exception as e:
                logger.info(f"ℹ️ Index schema status: {e}")

    def store_to_graph(self, article_data: dict, chunks: list[str], embeddings: list):
        """Lưu toàn diện Article, Chunk, Category và các mối quan hệ đồ thị"""
        page_id = str(article_data.get("page_id"))
        title = article_data.get("title", "")
        url = article_data.get("url", "")
        categories = article_data.get("categories", [])
        # Lọc bớt category rác của Wikipedia
        clean_categories = [
            c.replace("Thể loại:", "").strip() 
            for c in categories 
            if not any(k in c.lower() for k in ["bài viết", "bản mẫu", "cs1:", "liên kết wayback", "hatnote"])
        ]

        with self.neo4j_driver.session() as session:
            # 1. Tạo Node Article
            session.run("""
                MERGE (a:Article {page_id: $page_id})
                SET a.title = $title,
                    a.url = $url,
                    a.chunk_count = $chunk_count,
                    a.updated_at = datetime()
            """, {
                "page_id": page_id,
                "title": title,
                "url": url,
                "chunk_count": len(chunks)
            })

            # 2. Tạo Node Category và quan hệ (:Article)-[:IN_CATEGORY]->(:Category)
            if clean_categories:
                session.run("""
                    MATCH (a:Article {page_id: $page_id})
                    UNWIND $categories AS cat_name
                    MERGE (cat:Category {name: cat_name})
                    MERGE (a)-[:IN_CATEGORY]->(cat)
                """, {
                    "page_id": page_id,
                    "categories": clean_categories
                })

            # 3. Tạo Node Chunks, nhúng Vector và quan hệ (:Article)-[:HAS_CHUNK]->(:Chunk)
            chunk_records = []
            for i, (text, emb) in enumerate(zip(chunks, embeddings)):
                chunk_records.append({
                    "chunk_id": f"{page_id}_{i}",
                    "index": i,
                    "text": text,
                    "embedding": emb.tolist() if hasattr(emb, "tolist") else list(emb)
                })

            # Batch insert để tối ưu hiệu năng
            session.run("""
                UNWIND $chunks AS item
                MATCH (a:Article {page_id: $page_id})
                MERGE (c:Chunk {chunk_id: item.chunk_id})
                SET c.text = item.text,
                    c.index = item.index,
                    c.embedding = item.embedding
                MERGE (a)-[:HAS_CHUNK]->(c)
            """, {
                "page_id": page_id,
                "chunks": chunk_records
            })

            # 4. Tạo quan hệ đồ thị giữa các Article có cùng Category
            session.run("""
                MATCH (a1:Article {page_id: $page_id})-[:IN_CATEGORY]->(cat:Category)<-[:IN_CATEGORY]-(a2:Article)
                WHERE a1 <> a2
                MERGE (a1)-[r:SHARED_CATEGORY {category: cat.name}]-(a2)
            """, {"page_id": page_id})

            # 5. Tạo quan hệ đồ thị nếu một Chunk của bài viết đề cập đến tiêu đề bài viết khác
            session.run("""
                MATCH (other:Article)
                WHERE other.page_id <> $page_id AND size(other.title) > 3
                MATCH (c:Chunk)
                WHERE c.chunk_id STARTS WITH $prefix AND c.text CONTAINS other.title
                MERGE (c)-[:MENTIONS]->(other)
            """, {
                "page_id": page_id,
                "prefix": f"{page_id}_"
            })

        logger.info(f"🕸️ [GRAPH & VECTOR STORED] Bài: '{title}' | {len(chunks)} Chunks | {len(clean_categories)} Categories | Graph Relations Updated!")

    def process_article(self, article_data: dict):
        page_id = article_data.get("page_id")
        title = article_data.get("title")
        content = article_data.get("content", "")

        logger.info("=" * 70)
        logger.info(f"📥 [KAFKA CONSUME] Xử lý: '{title}' (ID: {page_id})")
        logger.info(f"   • Kích thước văn bản gốc: {len(content):,} ký tự")

        # Chia đoạn toàn bộ nội dung (không giới hạn 20 chunks)
        chunks = self.text_splitter.split_text(content)
        logger.info(f"✂️  [CHUNKING] Chia thành {len(chunks)} chunks độc lập (toàn bộ nội dung)")

        if not chunks:
            return

        # Tính toán embeddings theo batch 32 để nhanh và ổn định RAM
        logger.info(f"🧠 [EMBEDDING] Đang tạo Vector Embeddings cho {len(chunks)} chunks...")
        embeddings = self.embedder.encode(chunks, batch_size=32, show_progress_bar=False)

        # Lưu trữ mô hình GraphRAG hoàn chỉnh vào Neo4j
        self.store_to_graph(article_data, chunks, embeddings)
        logger.info(f"✅ Hoàn tất bài: '{title}'!")
        logger.info("=" * 70 + "\n")

    def run(self, max_messages: int = None):
        """Lắng nghe Kafka liên tục và nạp vào Neo4j GraphRAG"""
        logger.info(f"🚀 AI Ingestion Worker đang lắng nghe Kafka topic: '{self.topic}'...")
        processed = 0

        try:
            while True:
                if max_messages and processed >= max_messages:
                    logger.info(f"Đã xử lý đạt hạn mức test ({max_messages} messages). Dừng worker.")
                    break

                msg = self.consumer.poll(3.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    self.process_article(payload)
                    self.consumer.commit(msg)  # Chỉ commit offset khi đã lưu thành công vào Neo4j
                    processed += 1
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Dừng consumer theo yêu cầu.")
        finally:
            self.consumer.close()
            self.neo4j_driver.close()
            logger.info("Đã đóng kết nối Consumer và Neo4j.")

if __name__ == "__main__":
    worker = AIIngestionWorker()
    worker.run()  # Mặc định max_messages=None -> Chạy liên tục

