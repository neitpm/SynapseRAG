import sys
import json
import logging
from pathlib import Path
import asyncore  # Tương thích Python 3.12 với cassandra-driver
from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster

# Đảm bảo UTF-8 cho terminal Windows
sys.stdout.reconfigure(encoding="utf-8")

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cassandra-sink-worker")

class CassandraSinkWorker:
    def __init__(self):
        # 1. Khởi tạo Cassandra Cluster Connection & Schema
        logger.info(f"Đang kết nối Cassandra cluster ({settings.CASSANDRA_HOSTS}:{settings.CASSANDRA_PORT})...")
        self.cluster = Cluster(settings.CASSANDRA_HOSTS, port=settings.CASSANDRA_PORT)
        self.session = self.cluster.connect()
        self._init_cassandra_schema()

        # 2. Khởi tạo Kafka Consumer với Consumer Group riêng biệt
        self.consumer_conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "cassandra-sink-group-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit chỉ khi ghi Cassandra thành công
        }
        self.consumer = Consumer(self.consumer_conf)
        self.topic = settings.KAFKA_TOPIC_WIKI_RAW
        self.consumer.subscribe([self.topic])

    def _init_cassandra_schema(self):
        """Khởi tạo Keyspace và Bảng lưu bài viết thô (Raw Article Lake)"""
        # Tạo Keyspace nếu chưa tồn tại
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {settings.CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """)
        self.session.set_keyspace(settings.CASSANDRA_KEYSPACE)

        # Tạo bảng raw_articles
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS raw_articles (
                page_id text PRIMARY KEY,
                title text,
                url text,
                summary text,
                content text,
                categories list<text>,
                raw_payload text,
                crawled_at timestamp
            );
        """)

        # Chuẩn bị prepared statement để insert tốc độ cao
        self.insert_stmt = self.session.prepare("""
            INSERT INTO raw_articles (page_id, title, url, summary, content, categories, raw_payload, crawled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, toTimestamp(now()));
        """)
        logger.info(f"✅ Schema Cassandra Keyspace '{settings.CASSANDRA_KEYSPACE}' & table 'raw_articles' đã sẵn sàng.")

    def store_raw_article(self, payload: dict, raw_json_str: str):
        page_id = str(payload.get("page_id"))
        title = payload.get("title", "")
        url = payload.get("url", "")
        summary = payload.get("summary", "")
        content = payload.get("content", "")
        categories = payload.get("categories", [])

        self.session.execute(self.insert_stmt, (
            page_id,
            title,
            url,
            summary,
            content,
            categories,
            raw_json_str
        ))
        logger.info(f"💾 [CASSANDRA SINK] Đã lưu bài viết thô: '{title}' (ID: {page_id}) | Dung lượng: {len(content):,} chars")

    def run(self, max_messages: int = None):
        """Lắng nghe Kafka và đổ vào Cassandra Data Lake liên tục"""
        logger.info(f"🚀 Cassandra Sink Worker đang lắng nghe Kafka topic: '{self.topic}'...")
        processed = 0

        try:
            while True:
                if max_messages and processed >= max_messages:
                    logger.info(f"Đã xử lý đạt hạn mức test ({max_messages} messages). Dừng worker.")
                    break

                msg = self.consumer.poll(3.0)
                if msg is None:
                    # Timeout poll
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break

                raw_bytes = msg.value()
                raw_json_str = raw_bytes.decode("utf-8")
                payload = json.loads(raw_json_str)

                self.store_raw_article(payload, raw_json_str)
                self.consumer.commit(msg)  # Chỉ commit offset khi đã insert Cassandra thành công
                processed += 1

        except KeyboardInterrupt:
            logger.info("Nhận tín hiệu dừng từ người dùng.")
        finally:
            self.consumer.close()
            self.cluster.shutdown()
            logger.info("Đã đóng kết nối Consumer và Cassandra.")

if __name__ == "__main__":
    worker = CassandraSinkWorker()
    worker.run()
