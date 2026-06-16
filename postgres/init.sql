-- ─────────────────────────────────────────────────────────────
-- Fraud Detection Database Schema
-- ─────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─────────────────────────────────────────────────────────────
-- Fraud Records (detected by Flink)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_records (
    fraud_id            VARCHAR(36) PRIMARY KEY,
    transaction_id      VARCHAR(36),
    account_id          VARCHAR(100),
    amount              DOUBLE PRECISION,
    currency            VARCHAR(10),
    merchant            VARCHAR(255),
    fraud_type          VARCHAR(50)   NOT NULL,
    fraud_reason        TEXT,
    detected_at         VARCHAR(50),
    risk_score          INT           DEFAULT 0,
    original_ts         BIGINT,
    created_at          TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX idx_fraud_type       ON fraud_records (fraud_type);
CREATE INDEX idx_fraud_account    ON fraud_records (account_id);
CREATE INDEX idx_fraud_risk_score ON fraud_records (risk_score DESC);
CREATE INDEX idx_fraud_created    ON fraud_records (created_at DESC);
CREATE INDEX idx_fraud_detected   ON fraud_records (detected_at);

-- ─────────────────────────────────────────────────────────────
-- Valid (clean) Transactions
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS valid_transactions (
    transaction_id      VARCHAR(36) PRIMARY KEY,
    account_id          VARCHAR(100),
    amount              DOUBLE PRECISION,
    currency            VARCHAR(10),
    merchant            VARCHAR(255),
    merchant_category   VARCHAR(100),
    location_country    VARCHAR(10),
    card_type           VARCHAR(20),
    is_online           BOOLEAN,
    processed_at        VARCHAR(50),
    ts                  BIGINT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_valid_account  ON valid_transactions (account_id);
CREATE INDEX idx_valid_created  ON valid_transactions (created_at DESC);
CREATE INDEX idx_valid_currency ON valid_transactions (currency);
CREATE INDEX idx_valid_category ON valid_transactions (merchant_category);

-- ─────────────────────────────────────────────────────────────
-- Fraud Summary (materialised view refreshed by Grafana queries)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW fraud_summary_by_type AS
SELECT
    fraud_type,
    COUNT(*)                        AS total_frauds,
    AVG(risk_score)                 AS avg_risk_score,
    MAX(risk_score)                 AS max_risk_score,
    SUM(ABS(amount))                AS total_amount_blocked,
    MIN(created_at)                 AS first_seen,
    MAX(created_at)                 AS last_seen
FROM fraud_records
GROUP BY fraud_type
ORDER BY total_frauds DESC;

CREATE OR REPLACE VIEW fraud_by_account AS
SELECT
    account_id,
    COUNT(*)                        AS fraud_count,
    MAX(risk_score)                 AS max_risk,
    SUM(ABS(amount))                AS total_flagged_amount,
    ARRAY_AGG(DISTINCT fraud_type)  AS fraud_types,
    MAX(created_at)                 AS last_fraud_at
FROM fraud_records
GROUP BY account_id
ORDER BY fraud_count DESC;

CREATE OR REPLACE VIEW hourly_fraud_stats AS
SELECT
    date_trunc('minute', created_at) AS minute_bucket,
    COUNT(*)                          AS fraud_count,
    COUNT(DISTINCT account_id)        AS unique_accounts,
    AVG(risk_score)                   AS avg_risk,
    SUM(ABS(amount))                  AS amount_blocked
FROM fraud_records
WHERE created_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW transaction_health AS
SELECT
    (SELECT COUNT(*) FROM fraud_records
     WHERE created_at >= NOW() - INTERVAL '1 hour') AS frauds_last_hour,
    (SELECT COUNT(*) FROM valid_transactions
     WHERE created_at >= NOW() - INTERVAL '1 hour') AS valid_last_hour,
    (SELECT AVG(risk_score) FROM fraud_records
     WHERE created_at >= NOW() - INTERVAL '1 hour') AS avg_risk_last_hour,
    (SELECT MAX(created_at) FROM fraud_records)      AS last_fraud_detected;

-- ─────────────────────────────────────────────────────────────
-- Seed: Confirm tables exist
-- ─────────────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE 'Database schema initialised successfully';
    RAISE NOTICE 'Tables: fraud_records, valid_transactions';
    RAISE NOTICE 'Views: fraud_summary_by_type, fraud_by_account, hourly_fraud_stats, transaction_health';
END $$;
