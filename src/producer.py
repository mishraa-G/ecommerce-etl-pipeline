import json
import logging
import time
from typing import Optional, Callable
from src.config import settings
from src.models import EcommerceEvent

logger = logging.getLogger(__name__)

class EventProducer:
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = settings.KAFKA_TOPIC
        self._producer = None
        self._is_mock = False
        self.delivered_count = 0
        self.failed_count = 0
        self._init_producer()

    def _init_producer(self):
        try:
            from confluent_kafka import Producer
            from confluent_kafka.admin import AdminClient, NewTopic

            # Ensure topic exists
            admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            cluster_metadata = admin.list_topics(timeout=5.0)
            if self.topic not in cluster_metadata.topics:
                logger.info(f"Topic '{self.topic}' does not exist yet. Creating with 3 partitions...")
                fs = admin.create_topics([NewTopic(self.topic, num_partitions=3, replication_factor=1)])
                for t, f in fs.items():
                    try:
                        f.result()
                    except Exception:
                        pass
                time.sleep(1)

            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'linger.ms': settings.PRODUCER_LINGER_MS,
                'batch.size': settings.PRODUCER_BATCH_SIZE,
                'compression.type': settings.PRODUCER_COMPRESSION,
                'acks': settings.PRODUCER_ACKS,
                'queue.buffering.max.messages': 100000,
                'queue.buffering.max.kbytes': 1048576,
            }
            self._producer = Producer(conf)
            logger.info(f"Confluent Kafka Producer initialized for topic {self.topic}")
        except Exception as e:
            logger.warning(f"Could not initialize Kafka Producer ({e}). Running in offline simulation mode.")
            self._is_mock = True

    def _delivery_callback(self, err, msg):
        if err is not None:
            self.failed_count += 1
            logger.error(f"Message delivery failed: {err}")
        else:
            self.delivered_count += 1

    def send_event(self, event: EcommerceEvent, callback: Optional[Callable] = None):
        """Asynchronously send single event to Kafka topic."""
        cb = callback or self._delivery_callback
        payload = event.to_json().encode('utf-8')
        key = event.user_id.encode('utf-8')  # Partition by user_id to preserve per-user event order!

        if self._is_mock or self._producer is None:
            self.delivered_count += 1
            return

        try:
            self._producer.produce(
                topic=self.topic,
                key=key,
                value=payload,
                on_delivery=cb
            )
            # Poll(0) serves delivery callbacks without blocking
            self._producer.poll(0)
        except BufferError:
            # Buffer full - wait for queue to drain
            self._producer.poll(0.1)
            self._producer.produce(topic=self.topic, key=key, value=payload, on_delivery=cb)

    def produce_batch(self, count: int, target_rate: Optional[int] = None) -> int:
        """
        Produces `count` synthetic events. If `target_rate` (msg/sec) is provided,
        throttles output to benchmark specific throughput thresholds.
        """
        print(f"\n🚀 [Kafka Producer] Starting stream of {count:,} events to topic '{self.topic}'...")
        if target_rate:
            print(f"   Target emission rate: {target_rate:,} msg/sec")

        start_time = time.time()
        last_log = start_time

        for i in range(count):
            event = EcommerceEvent.generate_random()
            self.send_event(event)

            # Periodic progress output every 1,000 messages or every 1 second
            if (i + 1) % 1000 == 0 or (time.time() - last_log) > 1.0:
                elapsed = time.time() - start_time
                current_rate = (i + 1) / elapsed if elapsed > 0 else 0
                print(f"   📤 Streamed {i + 1:,}/{count:,} events ({current_rate:,.0f} msg/sec)...")
                last_log = time.time()

            # Optional rate limiter
            if target_rate:
                expected_elapsed = (i + 1) / target_rate
                actual_elapsed = time.time() - start_time
                if actual_elapsed < expected_elapsed:
                    time.sleep(expected_elapsed - actual_elapsed)

        self.flush()
        total_time = time.time() - start_time
        rate = count / total_time if total_time > 0 else 0
        print(f"✅ [Kafka Producer] Done! Emitted {count:,} events in {total_time:.2f}s ({rate:,.0f} msg/sec)\n")
        return count

    def flush(self, timeout: float = 10.0):
        if self._producer:
            self._producer.flush(timeout=timeout)

