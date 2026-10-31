import time
import json
import logging
from typing import List
import wikipediaapi
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

import sys
from pathlib import Path

# Thêm root vào sys.path để import shared
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings
from shared.schemas.wiki_message import WikiArticleMessage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("wiki-producer")

class WikiKafkaProducer:
    def __init__(self):
        # 1. Khởi tạo Kafka Producer
        self.producer_config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "wiki-producer-service",
            "acks": "all",  # Đảm bảo độ tin cậy ghi dữ liệu
        }
        self.producer = Producer(self.producer_config)
        self.topic = settings.KAFKA_TOPIC_WIKI_RAW

        # 2. Khởi tạo Wikipedia API Client (User-Agent chuẩn theo quy định của Wikimedia)
        self.wiki = wikipediaapi.Wikipedia(
            user_agent="GraphRAGBot/1.0 (contact@microservices.local)",
            language="vi",  # Mặc định lấy bài viết tiếng Việt (có thể đổi sang 'en')
            extract_format=wikipediaapi.ExtractFormat.WIKI
        )

        self._ensure_topic_exists()

    def _ensure_topic_exists(self):
        """Tự động kiểm tra và tạo Kafka Topic nếu chưa tồn tại"""
        admin_client = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=10)
        
        if self.topic not in metadata.topics:
            logger.info(f"Topic '{self.topic}' chưa tồn tại. Đang tiến hành tạo mới...")
            new_topic = NewTopic(self.topic, num_partitions=3, replication_factor=1)
            futures = admin_client.create_topics([new_topic])
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Đã tạo thành công topic '{topic}'!")
                except Exception as e:
                    logger.warning(f"Không thể tạo topic {topic}: {e}")
        else:
            logger.info(f"Topic '{self.topic}' đã sẵn sàng.")

    def _delivery_report(self, err, msg):
        """Callback thông báo kết quả gửi message vào Kafka"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(
                f"Đã gửi thành công vào topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}"
            )

    def fetch_and_publish(self, article_title: str) -> bool:
        """Lấy một bài viết từ Wikipedia và đẩy vào Kafka"""
        logger.info(f"Đang tìm nạp bài viết Wikipedia: '{article_title}'...")
        page = self.wiki.page(article_title)

        if not page.exists():
            logger.warning(f"Bài viết '{article_title}' không tồn tại trên Wikipedia!")
            return False

        # Trích xuất categories
        categories = [cat.replace("Category:", "") for cat in list(page.categories.keys())[:10]]

        # Đóng gói dữ liệu theo Schema Contract
        message = WikiArticleMessage(
            schema_version="1.0",
            page_id=page.pageid,
            title=page.title,
            url=page.fullurl,
            content=page.text,
            summary=page.summary[:500] if page.summary else None,
            categories=categories,
            metadata={
                "language": "vi",
                "char_length": len(page.text)
            }
        )

        # Gửi vào Kafka với Key là page_id
        payload = message.model_dump_json()
        self.producer.produce(
            topic=self.topic,
            key=str(page.pageid),
            value=payload.encode("utf-8"),
            callback=self._delivery_report
        )
        self.producer.flush()
        logger.info(f"Hoàn tất push bài viết: '{page.title}' ({len(page.text)} ký tự)")
        return True

    def publish_topics(self, titles: List[str]):
        """Publish một danh sách bài viết"""
        for title in titles:
            self.fetch_and_publish(title)
            time.sleep(1)  # Giãn cách để tránh rate limit của Wikipedia

if __name__ == "__main__":
    producer = WikiKafkaProducer()
    # Danh sách các bài viết mẫu thuộc chủ đề Công nghệ & AI
    sample_articles = [
        "Trí tuệ nhân tạo",
        "Học máy",
        "Kiến trúc hướng dịch vụ",
    ]
    logger.info("Bắt đầu tiến trình nạp bài viết Wikipedia vào Kafka...")
    producer.publish_topics(sample_articles)
    logger.info("Hoàn tất tiến trình nạp bài viết!")
