.PHONY: help setup up down test benchmark simulate-failure producer consumer

help:
	@echo "Available commands:"
	@echo "  make setup            Install Python dependencies"
	@echo "  make up               Start Kafka, Kafka-UI, and PostgreSQL via Docker"
	@echo "  make down             Stop all Docker containers"
	@echo "  make test             Run test suite"
	@echo "  make benchmark        Run local 1,500+ msg/sec stress benchmark"
	@echo "  make simulate-failure Run broker failure retry chaos test (0 duplicates)"
	@echo "  make producer         Start high-throughput synthetic event producer"
	@echo "  make consumer         Start streaming consumer worker"

setup:
	pip install -r requirements.txt

up:
	docker compose up -d
	@echo "Services started:"
	@echo "  Kafka:      localhost:9092"
	@echo "  Kafka-UI:   http://localhost:8080"
	@echo "  Postgres:   localhost:5433"

down:
	docker compose down -v

test:
	python3 -m unittest discover tests

benchmark:
	python3 scripts/benchmark.py --events 15000 --batch-size 1000

simulate-failure:
	python3 scripts/simulate_failure.py

producer:
	python3 scripts/run_producer.py --count 10000 --rate 2000

consumer:
	python3 scripts/run_consumer.py --batch-size 1000

