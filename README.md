# 🛡️ Real-Time Fraud Detection System

**Stack:** RedPanda · PyFlink · PostgreSQL · Grafana · Prometheus · Docker Compose

---

## 🏗️ Architecture

```
┌─────────────────┐     Avro/JSON      ┌──────────────────────┐
│  Python Producer│──────────────────►│  RedPanda (Kafka)    │
│  (Fake Txns)    │                    │  + Schema Registry   │
│  ~200 TPS       │                    │  Topic: transactions │
└─────────────────┘                    └──────────┬───────────┘
                                                  │
                                        ┌─────────▼───────────┐
                                        │   Apache Flink       │
                                        │   (PyFlink Job)      │
                                        │                      │
                                        │  11 Fraud Rules:     │
                                        │  ✗ High Amount       │
                                        │  ✗ Negative Amount   │
                                        │  ✗ Structuring       │
                                        │  ✗ Invalid Currency  │
                                        │  ✗ Blacklisted Acct  │
                                        │  ✗ Suspicious Merch  │
                                        │  ✗ Missing Fields    │
                                        │  ✗ Late Transaction  │
                                        │  ✗ Future Timestamp  │
                                        │  ✗ Round Num Online  │
                                        │  ✗ Velocity Fraud    │
                                        └──────┬──────┬────────┘
                                               │      │
                              ┌────────────────▼─┐  ┌─▼──────────────────┐
                              │  fraud-alerts     │  │  clean-transactions│
                              │  (Kafka topic)    │  │  (Kafka topic)     │
                              └────────┬──────────┘  └──────┬─────────────┘
                                       │                     │
                              ┌────────▼─────────────────────▼──────┐
                              │          PostgreSQL                   │
                              │  fraud_records | valid_transactions   │
                              └────────────────┬─────────────────────┘
                                               │
                              ┌────────────────▼─────────────────────┐
                              │   Grafana Dashboards + Prometheus     │
                              └───────────────────────────────────────┘
```

---

## 🚀 Quick Start

```bash
# 1. Start everything
make up

# 2. Wait ~2 minutes for all services to initialize

# 3. Open dashboards
open http://localhost:3000   # Grafana (admin/admin)
open http://localhost:8088   # Flink Web UI
open http://localhost:8080   # RedPanda Console
open http://localhost:9090   # Prometheus
```

---

## 🎛️ Load Testing

```bash
# Default: 200 TPS
make up

# Medium load: 500 TPS
make load-test-medium

# Heavy load: 2000 TPS
make load-test-high

# Max speed (burst mode – unlimited TPS)
make load-test-burst

# Stop producer
make load-test-stop
```

You can also override at startup:
```bash
TRANSACTIONS_PER_SECOND=5000 FRAUD_RATE=0.5 docker compose up -d producer
```

---

## 🔍 Fraud Types Injected by Producer

| Type                   | Description                                              | Rate |
|------------------------|----------------------------------------------------------|------|
| `HIGH_AMOUNT`          | Amount > $10,000                                         | 8%   |
| `NEGATIVE_AMOUNT`      | Negative or zero amount                                  | 4%   |
| `ZERO_AMOUNT`          | Exactly $0.00                                            | 2%   |
| `LATE_TRANSACTION`     | Timestamp 2–72 hours in the past                         | 6%   |
| `FUTURE_TIMESTAMP`     | Timestamp in the future                                  | 2%   |
| `INVALID_CURRENCY`     | Unknown currency code (XYZ, FOO, etc.)                   | 4%   |
| `MISSING_FIELDS`       | Empty transaction_id or account_id                       | 3%   |
| `BLACKLISTED_ACCOUNT`  | Known bad account (ACC_BLACKLIST_XXX)                    | 3%   |
| `DUPLICATE`            | Reused transaction_id                                    | 2%   |
| `STRUCTURING`          | Amount $9,000–$9,999 (just below reporting limit)        | 4%   |
| `SUSPICIOUS_MERCHANT`  | Casino / crypto / gambling keywords                      | 3%   |
| `ROUND_NUMBER`         | Exact round amounts online ($500, $1000...)              | 2%   |
| `INVALID_ACCOUNT_FMT`  | Special chars in account ID                              | 2%   |
| `SCHEMA_DATA_MISMATCH` | Valid schema but inconsistent data (IRR + US location)   | 3%   |
| `VELOCITY_BURST`       | 8–15 rapid transactions from same account                | ~1%  |
| **NORMAL**             | Legitimate transactions                                  | 65%  |

---

## 🔎 Flink Fraud Detection Rules

Flink applies 11 rule categories in the PyFlink SQL/UDF pipeline:

| Rule                    | Logic                                                          | Risk Score |
|-------------------------|----------------------------------------------------------------|------------|
| BLACKLISTED_ACCOUNT     | account_id in blacklist                                        | 100        |
| MISSING_REQUIRED_FIELDS | Empty transaction_id or account_id                             | 95         |
| INVALID_AMOUNT          | amount ≤ 0                                                     | 90         |
| INVALID_ACCOUNT_FORMAT  | Special chars in account_id                                    | 85         |
| VELOCITY_FRAUD          | > 5 txns from same account in 60s (OVER window)               | 85         |
| STRUCTURING             | 9,000 ≤ amount ≤ 9,999.99                                      | 80         |
| HIGH_AMOUNT             | amount > 10,000                                                | 75         |
| INVALID_CURRENCY        | Not in whitelist of 19 currencies                              | 70         |
| SUSPICIOUS_MERCHANT     | Casino/crypto/gambling keywords                                | 65         |
| FUTURE_TIMESTAMP        | Timestamp > 5 min in future                                    | 60         |
| ROUND_NUMBER_ONLINE     | Exact round amount in online transaction                       | 55         |
| LATE_TRANSACTION        | Timestamp > 1 hour old                                         | 40         |

---

## 🛠️ Debugging Commands

```bash
# See live transaction stream
make topic-msgs

# See live fraud alerts
make fraud-msgs

# Database stats
make fraud-stats
make valid-stats
make recent-frauds

# Flink job status
open http://localhost:8088

# Shell access
make shell-postgres
make shell-flink
make shell-redpanda

# All logs
make logs
make logs-producer
make logs-flink
```

---

## 📊 Grafana Panels

The main dashboard (`Fraud Detection – Real-Time`) includes:

- **Top row stats**: TPS · Fraud Rate % · Total Frauds · Valid Txns · Avg Risk Score · Errors
- **Time series**: Throughput breakdown (normal vs fraud) · Fraud by type per minute
- **Pie chart**: Fraud type distribution
- **Risk score timeline**: avg / min / max
- **Latest fraud alerts table**: last 50, color-coded by risk
- **Produce latency**: p50 / p99
- **Top fraudulent accounts**
- **Fraud by currency**

---

## ⚙️ Environment Variables

### Producer
| Variable                  | Default | Description                        |
|---------------------------|---------|------------------------------------|
| `TRANSACTIONS_PER_SECOND` | `200`   | Target TPS (across all threads)    |
| `FRAUD_RATE`              | `0.30`  | Fraction of fraudulent transactions|
| `BURST_MODE`              | `false` | Unlimited speed stress test        |
| `WORKER_THREADS`          | `4`     | Producer thread count              |

### Flink Job
| Variable                    | Default | Description                      |
|-----------------------------|---------|----------------------------------|
| `HIGH_AMOUNT_THRESHOLD`     | `10000` | Amount fraud trigger             |
| `VELOCITY_MAX_TRANSACTIONS` | `5`     | Max txns per window              |
| `VELOCITY_WINDOW_SECONDS`   | `60`    | Velocity window size             |
| `LATE_TRANSACTION_HOURS`    | `1`     | Age threshold for late txns      |

---

## 🧹 Cleanup

```bash
make down     # stop containers
make clean    # stop + remove volumes + images
make reset-db # clear DB tables only
```
