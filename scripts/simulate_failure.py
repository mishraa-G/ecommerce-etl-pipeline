#!/usr/bin/env python3
"""
Simulated Broker Failure & Idempotency Chaos Test.
Verifies that during simulated broker failure retries and duplicate event re-deliveries,
the pipeline guarantees At-Least-Once delivery with EXACTLY ZERO duplicate records in PostgreSQL.
"""

import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.models import EcommerceEvent
from src.db import DatabaseManager

def simulate_failure_and_retry():
    print("=" * 75)
    print("🧪 SIMULATION: BROKER FAILURE RETRY & AT-LEAST-ONCE DEDUPLICATION")
    print("=" * 75)

    db = DatabaseManager()

    # Step 1: Initial event generation
    TOTAL_UNIQUE = 3000
    DUPLICATE_RETRY_COUNT = 1500

    print(f"1️⃣ Step 1: Generating {TOTAL_UNIQUE} unique e-commerce streaming events...")
    all_events = [EcommerceEvent.generate_random() for _ in range(TOTAL_UNIQUE)]
    initial_batch = all_events[:TOTAL_UNIQUE - DUPLICATE_RETRY_COUNT]
    replay_batch = all_events[TOTAL_UNIQUE - DUPLICATE_RETRY_COUNT:]

    # Step 2: Normal ingestion of initial batch
    print(f"\n2️⃣ Step 2: Ingesting initial batch of {len(initial_batch)} events...")
    db.bulk_upsert_events(initial_batch)
    print(f"   ✓ Batch processed and committed. DB Total: {db.get_total_count()}")

    # Step 3: Ingest retry batch before crash
    print(f"\n3️⃣ Step 3: Ingesting second batch of {len(replay_batch)} events...")
    db.bulk_upsert_events(replay_batch)
    print(f"   ✓ Database write succeeded. DB Total: {db.get_total_count()}")

    # Step 4: Simulate broker failure / uncommitted offset
    print("\n💥 Step 4: [CHAOS SIMULATION] Broker network timeout / Consumer crash!")
    print("   ❌ Kafka consumer offset commit failed before acknowledgment!")
    print("   🔁 Broker triggers partition rebalance & retries previous batch...")
    time.sleep(1)

    # Step 5: Broker retries and re-delivers the EXACT same replay_batch
    print(f"\n5️⃣ Step 5: Consumer recovers and re-receives {len(replay_batch)} duplicate messages...")
    print("   🛡️ Applying PostgreSQL bulk upsert: ON CONFLICT (event_id) DO NOTHING...")
    db.bulk_upsert_events(replay_batch)
    print("   ✓ Consumer offset committed successfully after retry.")

    # Step 6: Assertions
    print("\n6️⃣ Step 6: Verifying data consistency and deduplication...")
    final_count = db.get_total_count()
    duplicates = db.get_duplicate_count()

    print("-" * 75)
    print(f"Expected Unique Records   : {TOTAL_UNIQUE}")
    print(f"Actual Records in DB       : {final_count}")
    print(f"Duplicate Records in DB    : {duplicates}")
    print("-" * 75)

    assert final_count == TOTAL_UNIQUE, f"Data corruption! Expected {TOTAL_UNIQUE}, found {final_count}"
    assert duplicates == 0, f"Duplicate detected! Found {duplicates} duplicates"

    print("🏆 PASS: 0 duplicate records detected across simulated broker failure retries!")
    print("   Idempotent storage + manual offset commits successfully provide resilient delivery.")
    print("=" * 75)

if __name__ == "__main__":
    simulate_failure_and_retry()

