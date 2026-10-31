import os
from pathlib import Path
from dotenv import load_dotenv

# Tự động nạp .env từ thư mục gốc
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

class BaseSettings:
    # Kafka / Redpanda
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_WIKI_RAW: str = os.getenv("KAFKA_TOPIC_WIKI_RAW", "wiki-raw-articles")
    KAFKA_TOPIC_WIKI_DLQ: str = os.getenv("KAFKA_TOPIC_WIKI_DLQ", "wiki-raw-articles-dlq")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "wiki-ingestion-group")

    # Neo4j
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "graphrag_password123")

    # Cassandra
    CASSANDRA_HOSTS: list[str] = [h.strip() for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")]
    CASSANDRA_PORT: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE: str = os.getenv("CASSANDRA_KEYSPACE", "synapse_lake")

    # Embeddings & LLM
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    EMBEDDING_DIMENSION: int = int(os.getenv("EMBEDDING_DIMENSION", "384"))

settings = BaseSettings()
