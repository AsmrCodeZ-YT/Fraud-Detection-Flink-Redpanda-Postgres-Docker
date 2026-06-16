#!/usr/bin/env python3
"""
Fraud Detection – Apache Flink (PyFlink) Job
==============================================
Reads transactions from RedPanda → detects fraud with 10+ rule categories
→ writes fraud alerts to Kafka + PostgreSQL
→ writes clean transactions to PostgreSQL

Fraud types detected:
  1. NEGATIVE_AMOUNT        – amount <= 0
  2. HIGH_AMOUNT            – amount > 10 000
  3. STRUCTURING            – amount 9 000–9 999 (just below reporting threshold)
  4. INVALID_CURRENCY       – unknown currency code
  5. BLACKLISTED_ACCOUNT    – known bad account IDs
  6. SUSPICIOUS_MERCHANT    – gambling / crypto / adult keywords
  7. MISSING_REQUIRED_FIELDS – empty transaction_id or account_id
  8. LATE_TRANSACTION       – timestamp more than 1 hour old
  9. FUTURE_TIMESTAMP       – timestamp more than 5 minutes in future
  10. ROUND_NUMBER_ONLINE   – exact round amounts in online transactions
  11. VELOCITY_FRAUD        – > N transactions from same account in 60 s (OVER window)
"""

import os
import uuid
import logging
import time
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fraud_detector")

# ─────────────────────────────────────────────────────────────
# Configuration from environment
# ─────────────────────────────────────────────────────────────
KAFKA_BROKERS       = os.getenv("KAFKA_BROKERS", "redpanda:29092")
POSTGRES_URL        = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/fraud_detection")
POSTGRES_USER       = os.getenv("POSTGRES_USER", "fraud_user")
POSTGRES_PASSWORD   = os.getenv("POSTGRES_PASSWORD", "fraud_pass")
HIGH_AMOUNT         = float(os.getenv("HIGH_AMOUNT_THRESHOLD", "10000"))
VELOCITY_MAX_TX     = int(os.getenv("VELOCITY_MAX_TRANSACTIONS", "5"))
VELOCITY_WINDOW_S   = int(os.getenv("VELOCITY_WINDOW_SECONDS", "60"))
LATE_TX_HOURS       = int(os.getenv("LATE_TRANSACTION_HOURS", "1"))

# ─────────────────────────────────────────────────────────────
# Blacklist & valid currencies
# ─────────────────────────────────────────────────────────────
BLACKLISTED_ACCOUNTS = frozenset(
    [f"ACC_BLACKLIST_{i:03d}" for i in range(1, 21)]
)

VALID_CURRENCIES = frozenset([
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD",
    "CHF", "CNY", "AED", "IRR", "TRY", "SAR",
    "SGD", "HKD", "NOK", "SEK", "DKK", "NZD",
])

SUSPICIOUS_KEYWORDS = frozenset([
    "casino", "gambling", "lottery", "bitcoin", "crypto",
    "adult", "pawn", "payday", "loan shark",
])

# ─────────────────────────────────────────────────────────────
# UDFs
# ─────────────────────────────────────────────────────────────

@udf(result_type=DataTypes.STRING())
def gen_uuid(_dummy: str) -> str:
    return str(uuid.uuid4())


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_blacklisted(account_id: str) -> bool:
    if not account_id:
        return False
    return account_id in BLACKLISTED_ACCOUNTS or account_id.startswith("ACC_BLACKLIST_")


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_valid_currency(currency: str) -> bool:
    if not currency:
        return False
    return currency.upper() in VALID_CURRENCIES


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_suspicious_merchant(merchant: str) -> bool:
    if not merchant or not merchant.strip():
        return True
    ml = merchant.lower()
    return any(kw in ml for kw in SUSPICIOUS_KEYWORDS)


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_late_transaction(ts: int) -> bool:
    if ts is None:
        return True
    current_ms = int(time.time() * 1000)
    age_hours = (current_ms - ts) / 3_600_000
    return age_hours > LATE_TX_HOURS


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_future_timestamp(ts: int) -> bool:
    if ts is None:
        return False
    current_ms = int(time.time() * 1000)
    return ts > current_ms + 300_000   # more than 5 min future


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_round_amount(amount: float) -> bool:
    if amount is None:
        return False
    for threshold in [10000, 5000, 3000, 2000, 1000, 500]:
        if float(amount) == float(threshold):
            return True
    return False


@udf(result_type=DataTypes.BOOLEAN())
def udf_is_invalid_account_format(account_id: str) -> bool:
    if not account_id or len(account_id) < 3:
        return True
    # Valid format: ACC_XXXXXX or BLACKLIST – anything else is suspicious
    invalid_chars = set("!@#$%^&*()+= []{}|\\;':\",/<>?`~")
    return bool(invalid_chars.intersection(set(account_id)))


@udf(result_type=DataTypes.INT())
def udf_risk_score(
    flag_blacklisted: bool,
    flag_missing_fields: bool,
    flag_negative_amount: bool,
    flag_high_amount: bool,
    flag_structuring: bool,
    flag_invalid_currency: bool,
    flag_suspicious_merchant: bool,
    flag_late_transaction: bool,
    flag_future_timestamp: bool,
    flag_round_online: bool,
    flag_invalid_account_fmt: bool,
) -> int:
    scores = {
        flag_blacklisted:          100,
        flag_missing_fields:        95,
        flag_negative_amount:       90,
        flag_invalid_account_fmt:   85,
        flag_structuring:           80,
        flag_high_amount:           75,
        flag_invalid_currency:      70,
        flag_suspicious_merchant:   65,
        flag_future_timestamp:      60,
        flag_round_online:          55,
        flag_late_transaction:      40,
    }
    flags = [k for k, v in scores.items() if k]
    if not flags:
        return 0
    return max(scores[f] for f in flags)


# ─────────────────────────────────────────────────────────────
# Environment Setup
# ─────────────────────────────────────────────────────────────

def create_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30_000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register all UDFs
    udfs = [
        ("gen_uuid",                       gen_uuid),
        ("udf_is_blacklisted",            udf_is_blacklisted),
        ("udf_is_valid_currency",         udf_is_valid_currency),
        ("udf_is_suspicious_merchant",    udf_is_suspicious_merchant),
        ("udf_is_late_transaction",       udf_is_late_transaction),
        ("udf_is_future_timestamp",       udf_is_future_timestamp),
        ("udf_is_round_amount",           udf_is_round_amount),
        ("udf_is_invalid_account_format", udf_is_invalid_account_format),
        ("udf_risk_score",                udf_risk_score),
    ]

    for name, fn in udfs:
        t_env.create_temporary_function(name, fn)
        
    return t_env


# ─────────────────────────────────────────────────────────────
# Table Definitions
# ─────────────────────────────────────────────────────────────

def create_tables(t_env):

    # ── Source: Kafka transactions ──────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE transactions (
            transaction_id      STRING,
            account_id          STRING,
            amount              DOUBLE,
            currency            STRING,
            merchant            STRING,
            merchant_category   STRING,
            ts                  BIGINT,
            location_country    STRING,
            location_city       STRING,
            ip_address          STRING,
            device_id           STRING,
            card_type           STRING,
            is_online           BOOLEAN,
            proc_time           AS PROCTIME()
        ) WITH (
            'connector'                         = 'kafka',
            'topic'                             = 'transactions',
            'properties.bootstrap.servers'      = '{KAFKA_BROKERS}',
            'properties.group.id'               = 'flink-fraud-detector',
            'properties.auto.offset.reset'      = 'latest',
            'format'                            = 'json',
            'json.fail-on-missing-field'        = 'false',
            'json.ignore-parse-errors'          = 'true',
            'scan.startup.mode'                 = 'latest-offset'
        )
    """)

    # ── Sink: Fraud Alerts Kafka topic ──────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE fraud_alerts_kafka (
            fraud_id        STRING,
            transaction_id  STRING,
            account_id      STRING,
            amount          DOUBLE,
            currency        STRING,
            merchant        STRING,
            fraud_type      STRING,
            fraud_reason    STRING,
            detected_at     STRING,
            risk_score      INT,
            original_ts     BIGINT
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'fraud-alerts',
            'properties.bootstrap.servers'  = '{KAFKA_BROKERS}',
            'format'                        = 'json'
        )
    """)

    # ── Sink: Fraud Records PostgreSQL ──────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE fraud_records_pg (
            fraud_id        STRING,
            transaction_id  STRING,
            account_id      STRING,
            amount          DOUBLE,
            currency        STRING,
            merchant        STRING,
            fraud_type      STRING,
            fraud_reason    STRING,
            detected_at     STRING,
            risk_score      INT,
            original_ts     BIGINT,
            PRIMARY KEY (fraud_id) NOT ENFORCED
        ) WITH (
            'connector'                     = 'jdbc',
            'url'                           = '{POSTGRES_URL}',
            'table-name'                    = 'fraud_records',
            'username'                      = '{POSTGRES_USER}',
            'password'                      = '{POSTGRES_PASSWORD}',
            'driver'                        = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows'    = '100',
            'sink.buffer-flush.interval'    = '2s',
            'sink.max-retries'              = '3'
        )
    """)

    # ── Sink: Valid Transactions PostgreSQL ─────────────────
    t_env.execute_sql(f"""
        CREATE TABLE valid_transactions_pg (
            transaction_id      STRING,
            account_id          STRING,
            amount              DOUBLE,
            currency            STRING,
            merchant            STRING,
            merchant_category   STRING,
            location_country    STRING,
            card_type           STRING,
            is_online           BOOLEAN,
            processed_at        STRING,
            ts                  BIGINT,
            PRIMARY KEY (transaction_id) NOT ENFORCED
        ) WITH (
            'connector'                     = 'jdbc',
            'url'                           = '{POSTGRES_URL}',
            'table-name'                    = 'valid_transactions',
            'username'                      = '{POSTGRES_USER}',
            'password'                      = '{POSTGRES_PASSWORD}',
            'driver'                        = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows'    = '500',
            'sink.buffer-flush.interval'    = '3s'
        )
    """)


# ─────────────────────────────────────────────────────────────
# Fraud Detection Pipeline
# ─────────────────────────────────────────────────────────────

def build_pipeline(t_env):

    # ── 1. Enrich every transaction with fraud flags ────────
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW enriched AS
        SELECT
            COALESCE(transaction_id, '')        AS transaction_id,
            COALESCE(account_id, '')            AS account_id,
            COALESCE(amount, 0.0)               AS amount,
            COALESCE(currency, '')              AS currency,
            COALESCE(merchant, '')              AS merchant,
            COALESCE(merchant_category, '')     AS merchant_category,
            COALESCE(ts, 0)                     AS ts,
            COALESCE(location_country, '')      AS location_country,
            COALESCE(location_city, '')         AS location_city,
            COALESCE(ip_address, '')            AS ip_address,
            COALESCE(device_id, '')             AS device_id,
            COALESCE(card_type, '')             AS card_type,
            COALESCE(is_online, FALSE)          AS is_online,
            proc_time,

            -- Fraud flags
            (transaction_id IS NULL OR transaction_id = ''
             OR account_id IS NULL OR account_id = '')     AS flag_missing_fields,

            (COALESCE(amount, 0.0) <= 0.0)                AS flag_negative_amount,

            (COALESCE(amount, 0.0) > 10000.0)             AS flag_high_amount,

            (COALESCE(amount, 0.0) BETWEEN 9000.0
                                       AND 9999.99)        AS flag_structuring,

            (NOT udf_is_valid_currency(currency))          AS flag_invalid_currency,

            udf_is_blacklisted(account_id)                 AS flag_blacklisted,

            udf_is_suspicious_merchant(merchant)           AS flag_suspicious_merchant,

            udf_is_late_transaction(ts)                    AS flag_late_transaction,

            udf_is_future_timestamp(ts)                    AS flag_future_timestamp,

            (udf_is_round_amount(COALESCE(amount, 0.0))
             AND COALESCE(is_online, FALSE) = TRUE)        AS flag_round_online,

            udf_is_invalid_account_format(account_id)      AS flag_invalid_account_fmt

        FROM transactions
    """)

    # ── 2. Rule-based fraud detection view ─────────────────
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW rule_frauds AS
        SELECT
            gen_uuid(transaction_id)    AS fraud_id,
            transaction_id,
            account_id,
            amount,
            currency,
            merchant,
            CASE
                WHEN flag_missing_fields      THEN 'MISSING_REQUIRED_FIELDS'
                WHEN flag_blacklisted         THEN 'BLACKLISTED_ACCOUNT'
                WHEN flag_negative_amount     THEN 'INVALID_AMOUNT'
                WHEN flag_invalid_account_fmt THEN 'INVALID_ACCOUNT_FORMAT'
                WHEN flag_structuring         THEN 'STRUCTURING'
                WHEN flag_high_amount         THEN 'HIGH_AMOUNT'
                WHEN flag_invalid_currency    THEN 'INVALID_CURRENCY'
                WHEN flag_suspicious_merchant THEN 'SUSPICIOUS_MERCHANT'
                WHEN flag_future_timestamp    THEN 'FUTURE_TIMESTAMP'
                WHEN flag_round_online        THEN 'ROUND_NUMBER_ONLINE'
                WHEN flag_late_transaction    THEN 'LATE_TRANSACTION'
                ELSE 'MULTI_RULE_VIOLATION'
            END AS fraud_type,
            CASE
                WHEN flag_missing_fields
                    THEN 'Missing transaction_id or account_id'
                WHEN flag_blacklisted
                    THEN CONCAT('Blacklisted account: ', account_id)
                WHEN flag_negative_amount
                    THEN CONCAT('Invalid amount: ', CAST(amount AS STRING))
                WHEN flag_invalid_account_fmt
                    THEN CONCAT('Malformed account ID: ', account_id)
                WHEN flag_structuring
                    THEN CONCAT('Structuring: amount ', CAST(amount AS STRING),
                                ' just below 10000 threshold')
                WHEN flag_high_amount
                    THEN CONCAT('Amount ', CAST(amount AS STRING),
                                ' exceeds high-value threshold of 10000')
                WHEN flag_invalid_currency
                    THEN CONCAT('Unknown currency code: ', currency)
                WHEN flag_suspicious_merchant
                    THEN CONCAT('Suspicious merchant: ', merchant)
                WHEN flag_future_timestamp
                    THEN CONCAT('Future timestamp detected: ts=', CAST(ts AS STRING))
                WHEN flag_round_online
                    THEN CONCAT('Suspicious round amount ', CAST(amount AS STRING),
                                ' in online transaction')
                WHEN flag_late_transaction
                    THEN CONCAT('Late transaction: older than 1 hour, ts=',
                                CAST(ts AS STRING))
                ELSE 'Multiple fraud indicators triggered simultaneously'
            END AS fraud_reason,
            CAST(CURRENT_TIMESTAMP AS STRING) AS detected_at,
            udf_risk_score(
                flag_blacklisted,
                flag_missing_fields,
                flag_negative_amount,
                flag_high_amount,
                flag_structuring,
                flag_invalid_currency,
                flag_suspicious_merchant,
                flag_late_transaction,
                flag_future_timestamp,
                flag_round_online,
                flag_invalid_account_fmt
            ) AS risk_score,
            ts AS original_ts
        FROM enriched
        WHERE
            flag_missing_fields      OR
            flag_negative_amount     OR
            flag_blacklisted         OR
            flag_high_amount         OR
            flag_structuring         OR
            flag_invalid_currency    OR
            flag_suspicious_merchant OR
            flag_late_transaction    OR
            flag_future_timestamp    OR
            flag_round_online        OR
            flag_invalid_account_fmt
    """)

    # ── 3. Velocity fraud (OVER window – processing time) ───
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW velocity_candidates AS
        SELECT
            transaction_id,
            account_id,
            amount,
            currency,
            merchant,
            ts,
            COUNT(*) OVER (
                PARTITION BY account_id
                ORDER BY proc_time
                RANGE INTERVAL '{VELOCITY_WINDOW_S}' SECOND PRECEDING
            ) AS tx_count_window
        FROM enriched
        WHERE NOT flag_missing_fields
          AND NOT flag_negative_amount
    """)

    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW velocity_frauds AS
        SELECT
            gen_uuid(transaction_id)    AS fraud_id,
            transaction_id,
            account_id,
            amount,
            currency,
            merchant,
            'VELOCITY_FRAUD'            AS fraud_type,
            CONCAT('Account ', account_id, ' made ',
                   CAST(tx_count_window AS STRING),
                   ' transactions within {VELOCITY_WINDOW_S}s (limit={VELOCITY_MAX_TX})')
                                        AS fraud_reason,
            CAST(CURRENT_TIMESTAMP AS STRING) AS detected_at,
            85                          AS risk_score,
            ts                          AS original_ts
        FROM velocity_candidates
        WHERE tx_count_window > {VELOCITY_MAX_TX}
    """)

    # ── 4. Clean transactions ───────────────────────────────
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW clean_transactions AS
        SELECT
            transaction_id,
            account_id,
            amount,
            currency,
            merchant,
            merchant_category,
            location_country,
            card_type,
            is_online,
            CAST(CURRENT_TIMESTAMP AS STRING) AS processed_at,
            ts
        FROM enriched
        WHERE
            NOT flag_missing_fields      AND
            NOT flag_negative_amount     AND
            NOT flag_blacklisted         AND
            NOT flag_high_amount         AND
            NOT flag_structuring         AND
            NOT flag_invalid_currency    AND
            NOT flag_suspicious_merchant AND
            NOT flag_late_transaction    AND
            NOT flag_future_timestamp    AND
            NOT flag_round_online        AND
            NOT flag_invalid_account_fmt
    """)

    # ── 5. Execute all inserts as a statement set ──────────
    stmt_set = t_env.create_statement_set()

    # Rule-based frauds → Kafka + PG
    stmt_set.add_insert("fraud_alerts_kafka", t_env.from_path("rule_frauds"))
    stmt_set.add_insert("fraud_records_pg",   t_env.from_path("rule_frauds"))

    # Velocity frauds → Kafka + PG
    stmt_set.add_insert("fraud_alerts_kafka", t_env.from_path("velocity_frauds"))
    stmt_set.add_insert("fraud_records_pg",   t_env.from_path("velocity_frauds"))

    # Clean transactions → PG
    stmt_set.add_insert("valid_transactions_pg", t_env.from_path("clean_transactions"))

    log.info("Submitting Flink statement set (6 inserts)...")
    return stmt_set.execute()


# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  Fraud Detection Flink Job Starting")
    log.info(f"  Kafka:     {KAFKA_BROKERS}")
    log.info(f"  Postgres:  {POSTGRES_URL}")
    log.info(f"  HighAmt:   {HIGH_AMOUNT}")
    log.info(f"  Velocity:  max {VELOCITY_MAX_TX} tx / {VELOCITY_WINDOW_S}s")
    log.info(f"  LateTx:    >{LATE_TX_HOURS}h")
    log.info("=" * 60)

    t_env = create_env()
    create_tables(t_env)
    result = build_pipeline(t_env)
    log.info("Pipeline running. Waiting for job completion...")
    result.wait()


if __name__ == "__main__":
    main()
