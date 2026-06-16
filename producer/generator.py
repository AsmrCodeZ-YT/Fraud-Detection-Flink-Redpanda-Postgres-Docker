#!/usr/bin/env python3
"""
High-Performance Financial Transaction Generator
Produces fake transactions with configurable fraud injection to RedPanda.
Supports very high TPS for Flink throughput benchmarking.
"""

import os
import sys
import json
import time
import uuid
import random
import threading
import logging
import signal
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from typing import Optional

import requests
from confluent_kafka import Producer, KafkaException
from faker import Faker
from prometheus_client import start_http_server, Counter, Histogram, Gauge, Summary

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
BROKERS           = os.getenv("REDPANDA_BROKERS", "localhost:9092")
SCHEMA_REGISTRY   = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC             = "transactions"
TPS               = int(os.getenv("TRANSACTIONS_PER_SECOND", "100"))
FRAUD_RATE        = float(os.getenv("FRAUD_RATE", "0.30"))
BURST_MODE        = os.getenv("BURST_MODE", "false").lower() == "true"
METRICS_PORT      = int(os.getenv("METRICS_PORT", "8000"))
LOG_EVERY_N       = int(os.getenv("LOG_EVERY_N", "500"))
WORKER_THREADS    = int(os.getenv("WORKER_THREADS", "4"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("generator")

# ─────────────────────────────────────────────────────────────
# Prometheus Metrics
# ─────────────────────────────────────────────────────────────
TX_PRODUCED      = Counter("generator_transactions_total",  "Total transactions produced", ["tx_type"])
TX_ERRORS        = Counter("generator_produce_errors_total", "Kafka produce errors")
TX_LATENCY       = Histogram("generator_produce_latency_seconds", "Produce call latency",
                              buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1])
CURRENT_TPS      = Gauge("generator_current_tps", "Real-time transactions per second")
SCHEMA_ERRORS    = Counter("generator_schema_errors_total", "Schema registry errors")
QUEUE_SIZE       = Gauge("generator_kafka_queue_size", "Kafka producer queue depth")

# ─────────────────────────────────────────────────────────────
# Data Constants
# ─────────────────────────────────────────────────────────────
VALID_CURRENCIES   = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "AED", "IRR"]
INVALID_CURRENCIES = ["XYZ", "ZZZ", "AAA", "QQQ", "FOO", "BAR", "BTC_FAKE", "DOGE_FAKE"]

MERCHANT_CATEGORIES = [
    "grocery", "restaurant", "gas_station", "pharmacy", "clothing",
    "electronics", "travel", "hotel", "entertainment", "healthcare",
]
SUSPICIOUS_CATEGORIES = ["gambling", "crypto_exchange", "adult", "pawn_shop"]

COUNTRIES = ["US", "GB", "DE", "FR", "JP", "CA", "AU", "AE", "IR", "NL", "SG"]
CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]

# Pre-generated blacklisted accounts
BLACKLISTED_ACCOUNTS = [f"ACC_BLACKLIST_{i:03d}" for i in range(1, 21)]

# Pool of normal accounts – shared across threads for velocity simulation
ACCOUNT_POOL = [f"ACC_{i:06d}" for i in range(1, 5001)]

# Recent transaction IDs for duplicate injection
_recent_ids: deque = deque(maxlen=200)
_recent_ids_lock = threading.Lock()

fake = Faker()

# ─────────────────────────────────────────────────────────────
# Avro Schema Registration
# ─────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = {
    "type": "record",
    "name": "Transaction",
    "namespace": "com.frauddetection",
    "fields": [
        {"name": "transaction_id",    "type": "string"},
        {"name": "account_id",        "type": "string"},
        {"name": "amount",            "type": "double"},
        {"name": "currency",          "type": "string"},
        {"name": "merchant",          "type": "string"},
        {"name": "merchant_category", "type": "string"},
        {"name": "ts",                "type": "long"},
        {"name": "location_country",  "type": "string"},
        {"name": "location_city",     "type": "string"},
        {"name": "ip_address",        "type": "string"},
        {"name": "device_id",         "type": "string"},
        {"name": "card_type",         "type": "string"},
        {"name": "is_online",         "type": "boolean"},
    ],
}


def register_schema() -> Optional[int]:
    """Register Avro schema with Schema Registry (RedPanda built-in)."""
    url = f"{SCHEMA_REGISTRY}/subjects/{TOPIC}-value/versions"
    payload = {"schema": json.dumps(TRANSACTION_SCHEMA)}
    for attempt in range(10):
        try:
            r = requests.post(url, json=payload, timeout=5)
            if r.status_code in (200, 201):
                schema_id = r.json().get("id")
                log.info(f"Schema registered/confirmed – id={schema_id}")
                return schema_id
            log.warning(f"Schema registry returned {r.status_code}: {r.text}")
        except Exception as e:
            log.warning(f"Schema registry not ready (attempt {attempt+1}/10): {e}")
        time.sleep(3)
    log.error("Could not register schema after 10 attempts, continuing without schema validation")
    return None


# ─────────────────────────────────────────────────────────────
# Transaction Builders
# ─────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def build_normal_transaction() -> dict:
    account = random.choice(ACCOUNT_POOL)
    return {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        account,
        "amount":            round(random.uniform(1.0, 4999.0), 2),
        "currency":          random.choice(VALID_CURRENCIES),
        "merchant":          fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "ts":                _now_ms(),
        "location_country":  random.choice(COUNTRIES),
        "location_city":     fake.city(),
        "ip_address":        fake.ipv4(),
        "device_id":         f"DEV_{random.randint(100000, 999999)}",
        "card_type":         random.choice(CARD_TYPES),
        "is_online":         random.choice([True, False]),
    }


def build_high_amount_fraud() -> dict:
    tx = build_normal_transaction()
    tx["amount"] = round(random.uniform(10001.0, 500000.0), 2)
    return tx


def build_negative_amount_fraud() -> dict:
    tx = build_normal_transaction()
    tx["amount"] = round(random.uniform(-10000.0, -0.01), 2)
    return tx


def build_zero_amount_fraud() -> dict:
    tx = build_normal_transaction()
    tx["amount"] = 0.0
    return tx


def build_late_transaction_fraud() -> dict:
    """Transaction with timestamp hours/days in the past."""
    tx = build_normal_transaction()
    hours_ago = random.randint(2, 72)
    tx["ts"] = int((datetime.now(timezone.utc) - timedelta(hours=hours_ago)).timestamp() * 1000)
    return tx


def build_future_timestamp_fraud() -> dict:
    """Transaction with timestamp in the future."""
    tx = build_normal_transaction()
    minutes_ahead = random.randint(5, 1440)
    tx["ts"] = int((datetime.now(timezone.utc) + timedelta(minutes=minutes_ahead)).timestamp() * 1000)
    return tx


def build_invalid_currency_fraud() -> dict:
    tx = build_normal_transaction()
    tx["currency"] = random.choice(INVALID_CURRENCIES)
    return tx


def build_missing_fields_fraud() -> dict:
    tx = build_normal_transaction()
    field = random.choice(["transaction_id", "account_id", "merchant"])
    tx[field] = ""
    return tx


def build_blacklisted_account_fraud() -> dict:
    tx = build_normal_transaction()
    tx["account_id"] = random.choice(BLACKLISTED_ACCOUNTS)
    return tx


def build_duplicate_transaction() -> dict:
    """Reuse a recent transaction_id."""
    with _recent_ids_lock:
        if _recent_ids:
            old_id = random.choice(list(_recent_ids))
            tx = build_normal_transaction()
            tx["transaction_id"] = old_id
            return tx
    return build_normal_transaction()   # fallback


def build_structuring_fraud() -> dict:
    """Amounts just below $10 000 reporting threshold."""
    tx = build_normal_transaction()
    tx["amount"] = round(random.uniform(9000.0, 9999.99), 2)
    return tx


def build_suspicious_merchant_fraud() -> dict:
    tx = build_normal_transaction()
    kw = random.choice(["Casino Royal", "BitcoinATM", "Online Gambling Hub", "CryptoFast Exchange"])
    tx["merchant"] = kw
    tx["merchant_category"] = random.choice(SUSPICIOUS_CATEGORIES)
    return tx


def build_round_number_fraud() -> dict:
    """Exact round amounts – common in money mule activity."""
    tx = build_normal_transaction()
    tx["amount"] = float(random.choice([500, 1000, 2000, 3000, 5000, 10000]))
    tx["is_online"] = True
    return tx


def build_velocity_burst(account_id: Optional[str] = None) -> list[dict]:
    """Return a burst of 8-15 rapid transactions from the same account."""
    acc = account_id or random.choice(ACCOUNT_POOL)
    n = random.randint(8, 15)
    txs = []
    for _ in range(n):
        tx = build_normal_transaction()
        tx["account_id"] = acc
        tx["amount"] = round(random.uniform(10.0, 200.0), 2)
        txs.append(tx)
    return txs


def build_invalid_account_format_fraud() -> dict:
    tx = build_normal_transaction()
    tx["account_id"] = f"INVALID_ACCT_{''.join(random.choices('!@#$%^&*()', k=8))}"
    return tx


def build_schema_data_mismatch_fraud() -> dict:
    """Valid schema but nonsensical data combinations."""
    tx = build_normal_transaction()
    # E.g., online transaction but amount == 0, or Iranian Rial with US location
    tx["amount"] = 0.01
    tx["currency"] = "IRR"
    tx["location_country"] = "US"
    tx["merchant_category"] = random.choice(SUSPICIOUS_CATEGORIES)
    return tx


# ─────────────────────────────────────────────────────────────
# Fraud Type Registry
# ─────────────────────────────────────────────────────────────
FRAUD_BUILDERS = {
    "HIGH_AMOUNT":           (build_high_amount_fraud,         0.08),
    "NEGATIVE_AMOUNT":       (build_negative_amount_fraud,     0.04),
    "ZERO_AMOUNT":           (build_zero_amount_fraud,         0.02),
    "LATE_TRANSACTION":      (build_late_transaction_fraud,    0.06),
    "FUTURE_TIMESTAMP":      (build_future_timestamp_fraud,    0.02),
    "INVALID_CURRENCY":      (build_invalid_currency_fraud,    0.04),
    "MISSING_FIELDS":        (build_missing_fields_fraud,      0.03),
    "BLACKLISTED_ACCOUNT":   (build_blacklisted_account_fraud, 0.03),
    "DUPLICATE":             (build_duplicate_transaction,     0.02),
    "STRUCTURING":           (build_structuring_fraud,         0.04),
    "SUSPICIOUS_MERCHANT":   (build_suspicious_merchant_fraud, 0.03),
    "ROUND_NUMBER":          (build_round_number_fraud,        0.02),
    "INVALID_ACCOUNT_FMT":   (build_invalid_account_format_fraud, 0.02),
    "SCHEMA_DATA_MISMATCH":  (build_schema_data_mismatch_fraud, 0.03),
}

# Weighted list for random selection
_fraud_types  = list(FRAUD_BUILDERS.keys())
_fraud_weights = [FRAUD_BUILDERS[k][1] for k in _fraud_types]


# ─────────────────────────────────────────────────────────────
# Kafka Producer
# ─────────────────────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BROKERS,
        "linger.ms":         5,
        "batch.size":        65536,
        "compression.type":  "lz4",
        "acks":              "1",
        "retries":           3,
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes":   1_048_576,
    })


_delivery_errors = 0

def _on_delivery(err, msg):
    global _delivery_errors
    if err:
        _delivery_errors += 1
        TX_ERRORS.inc()


# ─────────────────────────────────────────────────────────────
# Worker Thread
# ─────────────────────────────────────────────────────────────
_total_sent    = 0
_lock          = threading.Lock()
_running       = True


def worker_loop(producer: Producer, target_tps_per_thread: float):
    """Each worker thread runs its own produce loop."""
    global _total_sent
    interval = 1.0 / target_tps_per_thread if target_tps_per_thread > 0 else 0.001
    local_count = 0

    while _running:
        t0 = time.monotonic()

        # Decide: normal or fraud?
        if random.random() < FRAUD_RATE:
            fraud_type = random.choices(_fraud_types, weights=_fraud_weights, k=1)[0]
            builder = FRAUD_BUILDERS[fraud_type][0]
            if fraud_type == "VELOCITY_BURST":
                txs = build_velocity_burst()
            else:
                result = builder()
                txs = [result] if isinstance(result, dict) else result
            label = fraud_type.lower()
        else:
            # Occasionally inject velocity burst even in normal mode
            if random.random() < 0.01:
                txs = build_velocity_burst()
                label = "velocity_burst"
            else:
                txs = [build_normal_transaction()]
                label = "normal"

        for tx in txs:
            payload = json.dumps(tx).encode()
            # Track ID for duplicate injection
            with _recent_ids_lock:
                _recent_ids.append(tx["transaction_id"])

            t1 = time.monotonic()
            try:
                producer.produce(
                    topic=TOPIC,
                    key=tx["account_id"].encode(),
                    value=payload,
                    callback=_on_delivery,
                )
                TX_LATENCY.observe(time.monotonic() - t1)
                TX_PRODUCED.labels(tx_type=label).inc()
                local_count += 1
                QUEUE_SIZE.set(len(producer))
            except BufferError:
                producer.poll(0.1)
            except KafkaException as e:
                TX_ERRORS.inc()
                log.warning(f"Kafka error: {e}")

        producer.poll(0)

        # Rate limiting
        elapsed = time.monotonic() - t0
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    with _lock:
        _total_sent += local_count


# ─────────────────────────────────────────────────────────────
# TPS Reporter Thread
# ─────────────────────────────────────────────────────────────
def tps_reporter():
    global _total_sent
    prev = 0
    while _running:
        time.sleep(5)
        with _lock:
            current = _total_sent
        diff = current - prev
        tps = diff / 5.0
        CURRENT_TPS.set(tps)
        if current % LOG_EVERY_N < diff:
            log.info(
                f"[STATS] Sent={current:,}  TPS={tps:.0f}  "
                f"Fraud rate={FRAUD_RATE*100:.0f}%  DeliveryErrors={_delivery_errors}"
            )
        prev = current


# ─────────────────────────────────────────────────────────────
# Burst Mode  (max-speed stress test)
# ─────────────────────────────────────────────────────────────
def burst_mode(producer: Producer):
    """Push as fast as possible – ignore rate limiting."""
    log.info("BURST MODE ENABLED – pushing max TPS")
    while _running:
        for _ in range(1000):
            if random.random() < FRAUD_RATE:
                fraud_type = random.choices(_fraud_types, weights=_fraud_weights, k=1)[0]
                tx = FRAUD_BUILDERS[fraud_type][0]()
                if not isinstance(tx, dict):
                    tx = tx[0]
                label = fraud_type.lower()
            else:
                tx = build_normal_transaction()
                label = "normal"

            with _recent_ids_lock:
                _recent_ids.append(tx["transaction_id"])

            try:
                producer.produce(
                    topic=TOPIC,
                    key=tx["account_id"].encode(),
                    value=json.dumps(tx).encode(),
                    callback=_on_delivery,
                )
                TX_PRODUCED.labels(tx_type=label).inc()
            except BufferError:
                producer.poll(0.01)

        producer.poll(0)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def shutdown(sig, frame):
    global _running
    log.info("Shutting down gracefully...")
    _running = False


def main():
    global _running

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info(f"Starting transaction generator")
    log.info(f"  Brokers:          {BROKERS}")
    log.info(f"  Schema Registry:  {SCHEMA_REGISTRY}")
    log.info(f"  Target TPS:       {TPS}")
    log.info(f"  Fraud Rate:       {FRAUD_RATE*100:.0f}%")
    log.info(f"  Burst Mode:       {BURST_MODE}")
    log.info(f"  Worker Threads:   {WORKER_THREADS}")

    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    log.info(f"Prometheus metrics on :{METRICS_PORT}/metrics")

    # Wait for broker to be ready
    for attempt in range(20):
        try:
            p = make_producer()
            # Quick connection test
            meta = p.list_topics(timeout=5)
            log.info(f"Connected to RedPanda. Topics: {list(meta.topics.keys())}")
            p.close()
            break
        except Exception as e:
            log.warning(f"Waiting for RedPanda (attempt {attempt+1}/20): {e}")
            time.sleep(3)

    # Register Avro schema
    register_schema()

    # Start TPS reporter
    reporter = threading.Thread(target=tps_reporter, daemon=True)
    reporter.start()

    if BURST_MODE:
        prod = make_producer()
        burst_mode(prod)
        prod.flush()
    else:
        tps_per_thread = TPS / WORKER_THREADS
        producers = [make_producer() for _ in range(WORKER_THREADS)]
        threads = [
            threading.Thread(
                target=worker_loop,
                args=(producers[i], tps_per_thread),
                daemon=True,
                name=f"worker-{i}",
            )
            for i in range(WORKER_THREADS)
        ]
        for t in threads:
            t.start()

        # Wait until stopped
        while _running:
            time.sleep(1)

        log.info("Flushing producers...")
        for p in producers:
            p.flush(timeout=10)

    log.info(f"Generator stopped. Total sent: {_total_sent:,}")


if __name__ == "__main__":
    main()
