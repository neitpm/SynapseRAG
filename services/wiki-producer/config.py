import os
from pydantic import BaseModel

class Config(BaseModel):
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_WIKI_RAW: str = os.getenv("KAFKA_TOPIC_WIKI_RAW", "wiki-raw-articles")
    WIKI_USER_AGENT: str = os.getenv("WIKI_USER_AGENT", "GraphRAGBot/1.0 (contact@example.com)")

config = Config()
