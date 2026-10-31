import os
from pydantic import BaseModel

class Config(BaseModel):
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_WIKI_RAW: str = os.getenv("KAFKA_TOPIC_WIKI_RAW", "wiki-raw-articles")
    KAFKA_TOPIC_WIKI_DLQ: str = os.getenv("KAFKA_TOPIC_WIKI_DLQ", "wiki-raw-articles-dlq")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "wiki-ingestion-group")

    # Neo4j
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "graphrag_password123")

    # Chunking & Embeddings
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "500"))
    CHUNK_OVERLAP: int = int(os.getenv("CHUNK_OVERLAP", "50"))
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

config = Config()
