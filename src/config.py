import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "ecommerce.events")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "ecommerce-dw-ingestion-group")
    
    # Producer Throughput Tuning
    PRODUCER_LINGER_MS: int = int(os.getenv("PRODUCER_LINGER_MS", "15"))  # wait up to 15ms to batch
    PRODUCER_BATCH_SIZE: int = int(os.getenv("PRODUCER_BATCH_SIZE", "65536"))  # 64KB batch
    PRODUCER_COMPRESSION: str = os.getenv("PRODUCER_COMPRESSION", "snappy")
    PRODUCER_ACKS: str = os.getenv("PRODUCER_ACKS", "1")  # Leader ack for high throughput
    
    # Consumer Tuning
    CONSUMER_BATCH_SIZE: int = int(os.getenv("CONSUMER_BATCH_SIZE", "1000"))
    CONSUMER_POLL_TIMEOUT_SEC: float = float(os.getenv("CONSUMER_POLL_TIMEOUT_SEC", "1.0"))
    CONSUMER_AUTO_OFFSET_RESET: str = os.getenv("CONSUMER_AUTO_OFFSET_RESET", "earliest")
    
    # PostgreSQL Settings
    PG_HOST: str = os.getenv("PG_HOST", "127.0.0.1")
    PG_PORT: int = int(os.getenv("PG_PORT", "5433"))
    PG_DB: str = os.getenv("PG_DB", "ecommerce_dw")
    PG_USER: str = os.getenv("PG_USER", "postgres")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "postgrespassword")
    
    @property
    def pg_connection_string(self) -> str:
        return f"postgresql://{self.PG_USER}:{self.PG_PASSWORD}@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DB}"

settings = Settings()

