#!/usr/bin/env python3
"""Run Kafka Streaming Consumer Worker"""
import sys
import os
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.consumer import EventConsumer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume e-commerce events and upsert to PostgreSQL")
    parser.add_argument("--batch-size", type=int, default=1000, help="Micro-batch size for DB upsert")
    parser.add_argument("--limit", type=int, default=None, help="Stop after consuming N events (optional)")
    args = parser.parse_args()

    try:
        consumer = EventConsumer()
        consumer.consume_loop(batch_size=args.batch_size, max_messages=args.limit)
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user.")

