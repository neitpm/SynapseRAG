import sys
import json
from pathlib import Path

# Thêm root vào sys.path để import shared
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

# Đảm bảo in tiếng Việt trên console Windows không bị lỗi font
sys.stdout.reconfigure(encoding="utf-8")

from confluent_kafka import Consumer
from shared.config import settings

def inspect_kafka_data():
    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "cli-data-inspector-group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    topic = settings.KAFKA_TOPIC_WIKI_RAW
    consumer.subscribe([topic])

    print("=" * 70)
    print(f"🔍 ĐANG ĐỌC DỮ LIỆU TỪ KAFKA TOPIC: [{topic}]")
    print("=" * 70)

    count = 0
    try:
        while True:
            msg = consumer.poll(3.0)
            if msg is None:
                # Hết message trong topic
                break
            if msg.error():
                continue

            count += 1
            key = msg.key().decode("utf-8") if msg.key() else "No-Key"
            data = json.loads(msg.value().decode("utf-8"))

            print(f"\n📚 Bài viết #{count} (Partition: {msg.partition()} | Offset: {msg.offset()}):")
            print(f"   • Page ID       : {key}")
            print(f"   • Tiêu đề       : {data.get('title')}")
            print(f"   • Đường dẫn URL : {data.get('url')}")
            print(f"   • Schema Version: {data.get('schema_version')}")
            print(f"   • Độ dài văn bản: {len(data.get('content', ''))} ký tự")
            print(f"   • Danh mục      : {', '.join(data.get('categories', [])[:4])}...")
            print(f"   • Trích đoạn đầu: {data.get('content', '')[:120].strip()}...")

    finally:
        consumer.close()

    print("\n" + "=" * 70)
    print(f"✅ Hoàn tất kiểm tra! Đã đọc thành công {count} bài viết từ Kafka.")
    print("=" * 70)

if __name__ == "__main__":
    inspect_kafka_data()
