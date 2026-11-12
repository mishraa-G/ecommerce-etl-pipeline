import json
import logging
from typing import List, Optional, Tuple, Dict, Any
from src.config import settings
from src.models import EcommerceEvent

logger = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO ecommerce_events (
    event_id,
    event_type,
    user_id,
    product_id,
    order_id,
    quantity,
    amount,
    currency,
    event_timestamp,
    metadata
) VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""

class DatabaseManager:
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or settings.pg_connection_string
        self._conn = None
        self._is_mock = False
        self._mock_storage: Dict[str, Dict[str, Any]] = {}

    def get_connection(self):
        if self._is_mock:
            return None

        if self._conn is not None and not self._conn.closed:
            return self._conn
        
        try:
            import psycopg2
            self._conn = psycopg2.connect(
                host=settings.PG_HOST,
                port=settings.PG_PORT,
                dbname=settings.PG_DB,
                user=settings.PG_USER,
                password=settings.PG_PASSWORD,
                connect_timeout=3
            )
            self._conn.autocommit = False  # Explicit transaction control for at-least-once commits
            print(f"🐘 [PostgreSQL] Connected successfully to {settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB}")
            return self._conn
        except Exception as e:
            print(f"⚠️ [PostgreSQL Connection Error]: {e}")
            print("   -> Falling back to in-memory mock engine.")
            self._is_mock = True
            return None

    def initialize_schema(self, schema_file: str = "sql/init.sql"):
        """Creates the database schema if running against PostgreSQL."""
        conn = self.get_connection()
        if self._is_mock or conn is None:
            logger.info("Using mock DB storage; skipping SQL DDL execution.")
            return

        with open(schema_file, "r") as f:
            ddl = f.read()

        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        logger.info("Database schema initialized successfully.")

    def bulk_upsert_events(self, events: List[EcommerceEvent]) -> int:
        """
        Executes a high-throughput bulk SQL upsert with ON CONFLICT (event_id) DO NOTHING.
        Returns the number of rows processed.
        
        Uses psycopg2.extras.execute_values for batched execution in a single round-trip.
        """
        if not events:
            return 0

        conn = self.get_connection()

        # In-memory mock simulation for unit tests & offline stress benchmarks
        if self._is_mock or conn is None:
            inserted = 0
            for ev in events:
                if ev.event_id not in self._mock_storage:
                    self._mock_storage[ev.event_id] = ev.to_dict()
                    inserted += 1
            return inserted

        import psycopg2.extras

        # Prepare records tuple
        records: List[Tuple] = [
            (
                ev.event_id,
                ev.event_type,
                ev.user_id,
                ev.product_id,
                ev.order_id,
                ev.quantity,
                ev.amount,
                ev.currency,
                ev.event_timestamp,
                json.dumps(ev.metadata)
            )
            for ev in events
        ]

        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    UPSERT_SQL,
                    records,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
                    page_size=len(records)
                )
            conn.commit()
            return len(records)
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to bulk upsert batch of {len(records)} events: {e}")
            raise e

    def get_total_count(self) -> int:
        """Returns total distinct events in database."""
        conn = self.get_connection()
        if self._is_mock or conn is None:
            return len(self._mock_storage)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ecommerce_events;")
            count = cur.fetchone()[0]
        return count

    def get_duplicate_count(self) -> int:
        """Verifies 0 duplicates exist for event_id primary key."""
        conn = self.get_connection()
        if self._is_mock or conn is None:
            return 0  # Dict key guarantees 0 duplicates

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT event_id, COUNT(*)
                    FROM ecommerce_events
                    GROUP BY event_id
                    HAVING COUNT(*) > 1
                ) duplicates;
            """)
            dup_count = cur.fetchone()[0]
        return dup_count

    def close(self):
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
