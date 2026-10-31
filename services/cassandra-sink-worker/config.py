import os
from pydantic import BaseModel

class Config(BaseModel):
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_WIKI_RAW: str = os.getenv("KAFKA_TOPIC_WIKI_RAW", "wiki-raw-articles")
    KAFKA_CONSUMER_GROUP: str = os.getenv("CASSANDRA_CONSUMER_GROUP", "cassandra-sink-group")

    # Cassandra
    CASSANDRA_CONTACT_POINTS: str = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost")
    CASSANDRA_PORT: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE: str = os.getenv("CASSANDRA_KEYSPACE", "wiki_knowledge")

config = Config()
