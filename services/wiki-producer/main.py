import sys
import time
import logging
from typing import List
from pathlib import Path
import wikipediaapi
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Đảm bảo in tiếng Việt trên console Windows chuẩn xác
sys.stdout.reconfigure(encoding="utf-8")

# Thêm root vào sys.path để import shared
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings
from shared.schemas.wiki_message import WikiArticleMessage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("wiki-producer")

class WikiKafkaProducer:
    def __init__(self, language: str = "vi"):
        self.language = language
        self.producer_config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "wiki-producer-service",
            "acks": "all",
        }
        self.producer = Producer(self.producer_config)
        self.topic = settings.KAFKA_TOPIC_WIKI_RAW

        self.wiki = wikipediaapi.Wikipedia(
            user_agent="SynapseRAG-Bot/1.0 (contact@synapserag.local)",
            language=self.language,
            extract_format=wikipediaapi.ExtractFormat.WIKI
        )
        self._ensure_topic_exists()

    def _ensure_topic_exists(self):
        admin_client = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=10)
        if self.topic not in metadata.topics:
            logger.info(f"Topic '{self.topic}' chưa tồn tại. Đang tạo...")
            new_topic = NewTopic(self.topic, num_partitions=3, replication_factor=1)
            futures = admin_client.create_topics([new_topic])
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Đã tạo thành công topic '{topic}'!")
                except Exception as e:
                    logger.warning(f"Không thể tạo topic {topic}: {e}")

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"❌ Gửi vào Kafka thất bại: {err}")
        else:
            logger.info(
                f"✅ ĐÃ ĐẨY VÀO KAFKA THÀNH CÔNG! Topic: '{msg.topic()}' | Partition: {msg.partition()} | Offset: {msg.offset()}"
            )

    def fetch_and_publish(self, article_title: str) -> bool:
        logger.info(f"🌐 Đang cào bài viết từ Wikipedia: '{article_title}' ({self.language.upper()})...")
        page = self.wiki.page(article_title)

        if not page.exists():
            logger.warning(f"❌ Bài viết '{article_title}' không tồn tại trên Wikipedia!")
            return False

        categories = [cat.replace("Category:", "").replace("Thể loại:", "") for cat in list(page.categories.keys())[:10]]

        message = WikiArticleMessage(
            schema_version="1.0",
            page_id=page.pageid,
            title=page.title,
            url=page.fullurl,
            content=page.text,
            summary=page.summary[:500] if page.summary else None,
            categories=categories,
            metadata={
                "language": self.language,
                "char_length": len(page.text)
            }
        )

        payload = message.model_dump_json()
        self.producer.produce(
            topic=self.topic,
            key=str(page.pageid),
            value=payload.encode("utf-8"),
            callback=self._delivery_report
        )
        self.producer.flush()
        logger.info(f"🎉 Hoàn tất cào và bắn vào Kafka: '{page.title}' ({len(page.text):,} ký tự)")
        return True

if __name__ == "__main__":
    # Nhận từ khóa từ tham số terminal nếu có
    if len(sys.argv) > 1:
        target_title = sys.argv[1]
        target_lang = sys.argv[2] if len(sys.argv) > 2 else "vi"
    else:
        target_title = "Điện toán đám mây"
        target_lang = "vi"

    producer = WikiKafkaProducer(language=target_lang)
    producer.fetch_and_publish(target_title)
