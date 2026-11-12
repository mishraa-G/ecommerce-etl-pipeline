#!/usr/bin/env python3
"""Run Kafka Streaming Producer"""
import sys
import os
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.producer import EventProducer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream synthetic e-commerce events to Kafka")
    parser.add_argument("--count", type=int, default=10000, help="Total number of events to stream")
    parser.add_argument("--rate", type=int, default=2000, help="Target rate in msg/sec")
    args = parser.parse_args()

    producer = EventProducer()
    producer.produce_batch(count=args.count, target_rate=args.rate)

