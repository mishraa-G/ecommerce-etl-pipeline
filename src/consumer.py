import json
import logging
import time
from typing import List, Optional
from src.config import settings
from src.models import EcommerceEvent
from src.db import DatabaseManager

logger = logging.getLogger(__name__)

class EventConsumer:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        db_manager: Optional[DatabaseManager] = None
    ):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.group_id = group_id or settings.KAFKA_CONSUMER_GROUP
        self.topic = settings.KAFKA_TOPIC
        self.db = db_manager or DatabaseManager()
        self.db.initialize_schema()
        self._consumer = None
        self._is_mock = False
        self.total_ingested = 0
        self.total_batches = 0
        self._init_consumer()

    def _init_consumer(self):
        try:
            from confluent_kafka import Consumer
            from confluent_kafka.admin import AdminClient, NewTopic

            # Ensure topic exists before subscribing to prevent UNKNOWN_TOPIC_OR_PART error
            admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            cluster_metadata = admin.list_topics(timeout=5.0)
            if self.topic not in cluster_metadata.topics:
                logger.info(f"Topic '{self.topic}' does not exist yet. Creating with 3 partitions...")
                fs = admin.create_topics([NewTopic(self.topic, num_partitions=3, replication_factor=1)])
                for t, f in fs.items():
                    try:
                        f.result()
                    except Exception as e:
                        logger.warning(f"Topic creation notice: {e}")
                time.sleep(1)  # Allow metadata propagation

            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': settings.CONSUMER_AUTO_OFFSET_RESET,
                'enable.auto.commit': False,  # CRITICAL: Manual commits strictly after DB flush for At-Least-Once
                'max.poll.interval.ms': 300000,
                'fetch.min.bytes': 1024,
                'fetch.wait.max.ms': 100,
            }
            self._consumer = Consumer(conf)
            self._consumer.subscribe([self.topic])
            logger.info(f"Consumer group '{self.group_id}' subscribed to topic '{self.topic}' (Manual Commit Mode)")
        except Exception as e:
            logger.warning(f"Could not initialize Kafka Consumer ({e}). Running in offline simulation mode.")
            self._is_mock = True

    def process_batch(self, events: List[EcommerceEvent]) -> int:
        """
        Executes transactional bulk upsert into PostgreSQL.
        Guarantees idempotency via ON CONFLICT (event_id) DO NOTHING.
        """
        if not events:
            return 0
        
        count = self.db.bulk_upsert_events(events)
        self.total_ingested += count
        self.total_batches += 1
        return count

    def commit_offset(self, asynchronous: bool = False):
        """
        Manually commits Kafka offset ONLY after DB write succeeds.
        Guarantees At-Least-Once processing semantics.
        """
        if self._consumer and not self._is_mock:
            self._consumer.commit(asynchronous=asynchronous)

    def consume_loop(
        self,
        batch_size: int = 1000,
        poll_timeout: float = 1.0,
        max_messages: Optional[int] = None
    ):
        """
        High-throughput micro-batch consumption loop.
        Polls up to batch_size messages, executes bulk upsert, and commits offsets.
        """
        processed_count = 0
        target_db = "PostgreSQL" if not self.db._is_mock else "In-Memory Mock"
        print(f"\n🎧 [Kafka Consumer] Worker active for group '{self.group_id}'")
        print(f"   Target Database: {target_db}")
        print(f"   Listening on topic: '{self.topic}' (batch size: {batch_size:,})")
        print("   ⏳ Waiting for incoming messages from Kafka... (Press Ctrl+C to stop)\n")
        
        while True:
            if max_messages and processed_count >= max_messages:
                print(f"🏁 Reached target limit ({max_messages:,} events). Stopping consumer.")
                break

            if self._is_mock or self._consumer is None:
                # Mock consumer loop does not block indefinitely
                break

            # Poll batch of messages from Kafka broker
            messages = self._consumer.consume(num_messages=batch_size, timeout=poll_timeout)
            if not messages:
                continue

            events: List[EcommerceEvent] = []
            for msg in messages:
                if msg.error():
                    print(f"⚠️ Kafka consumer notice: {msg.error()}")
                    continue
                try:
                    raw_data = json.loads(msg.value().decode('utf-8'))
                    events.append(EcommerceEvent.from_dict(raw_data))
                except Exception as e:
                    print(f"⚠️ Malformed event skipped: {e}")

            if events:
                # 1. Transactional bulk upsert in Postgres
                t0 = time.perf_counter()
                self.process_batch(events)
                db_duration_ms = (time.perf_counter() - t0) * 1000
                
                # 2. Manual offset commit AFTER successful DB upsert
                self.commit_offset(asynchronous=False)
                
                processed_count += len(events)
                rate = len(events) / (db_duration_ms / 1000) if db_duration_ms > 0 else 0
                print(f"⚡ [Consumer] Ingested & committed {len(events):,} events to PostgreSQL in {db_duration_ms:.1f}ms "
                      f"({rate:,.0f} msg/sec) | Total in DB: {processed_count:,}")

    def close(self):
        if self._consumer:
            self._consumer.close()
        self.db.close()

