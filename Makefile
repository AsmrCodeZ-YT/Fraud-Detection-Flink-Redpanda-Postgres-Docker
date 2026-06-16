.PHONY: up down build logs ps clean shell-flink shell-postgres shell-redpanda \
        load-test-low load-test-medium load-test-high load-test-burst \
        fraud-stats topics topic-msgs reset

# ─────────────────────────────────────────────────────────────
# Core Commands
# ─────────────────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo ""
	@echo "═══════════════════════════════════════════════════════"
	@echo "  Services starting up..."
	@echo "  RedPanda Console : http://localhost:8080"
	@echo "  Flink Web UI     : http://localhost:8088"
	@echo "  Grafana          : http://localhost:3000  (admin/admin)"
	@echo "  Prometheus       : http://localhost:9090"
	@echo "  Producer Metrics : http://localhost:8000/metrics"
	@echo "═══════════════════════════════════════════════════════"

down:
	docker compose down

build:
	docker compose build --no-cache

logs:
	docker compose logs -f --tail=50

ps:
	docker compose ps

clean:
	docker compose down -v --remove-orphans
	docker image rm fraud-flink:1.18 2>/dev/null || true

# ─────────────────────────────────────────────────────────────
# Load Testing
# ─────────────────────────────────────────────────────────────
load-test-low:
	docker compose exec producer sh -c "echo Already running at configured TPS"
	@echo "Current TPS is set by TRANSACTIONS_PER_SECOND env var"

load-test-medium:
	docker compose stop producer
	TRANSACTIONS_PER_SECOND=500 docker compose up -d producer
	@echo "Producer restarted at 500 TPS"

load-test-high:
	docker compose stop producer
	TRANSACTIONS_PER_SECOND=2000 docker compose up -d producer
	@echo "Producer restarted at 2000 TPS"

load-test-burst:
	docker compose stop producer
	BURST_MODE=true docker compose up -d producer
	@echo "Producer restarted in BURST mode (max speed)"

load-test-stop:
	docker compose stop producer
	@echo "Producer stopped"

# ─────────────────────────────────────────────────────────────
# Debugging
# ─────────────────────────────────────────────────────────────
logs-producer:
	docker compose logs -f producer

logs-flink:
	docker compose logs -f jobmanager taskmanager flink-job-submitter

logs-postgres:
	docker compose logs -f postgres

shell-flink:
	docker compose exec jobmanager bash

shell-postgres:
	docker compose exec postgres psql -U fraud_user -d fraud_detection

shell-redpanda:
	docker compose exec redpanda bash

# ─────────────────────────────────────────────────────────────
# Kafka / RedPanda helpers
# ─────────────────────────────────────────────────────────────
topics:
	docker compose exec redpanda rpk topic list

topic-msgs:
	@echo "=== Last 10 transactions ==="
	docker compose exec redpanda rpk topic consume transactions --num 10 --offset end

fraud-msgs:
	@echo "=== Last 10 fraud alerts ==="
	docker compose exec redpanda rpk topic consume fraud-alerts --num 10 --offset end

# ─────────────────────────────────────────────────────────────
# PostgreSQL Queries
# ─────────────────────────────────────────────────────────────
fraud-stats:
	docker compose exec postgres psql -U fraud_user -d fraud_detection -c \
	"SELECT fraud_type, COUNT(*) as count, ROUND(AVG(risk_score)::numeric,1) as avg_risk FROM fraud_records GROUP BY fraud_type ORDER BY count DESC;"

valid-stats:
	docker compose exec postgres psql -U fraud_user -d fraud_detection -c \
	"SELECT COUNT(*) as total_valid, ROUND(AVG(amount)::numeric,2) as avg_amount, MAX(created_at) as last_processed FROM valid_transactions;"

recent-frauds:
	docker compose exec postgres psql -U fraud_user -d fraud_detection -c \
	"SELECT account_id, amount, fraud_type, risk_score, detected_at FROM fraud_records ORDER BY created_at DESC LIMIT 20;"

# ─────────────────────────────────────────────────────────────
# Reset
# ─────────────────────────────────────────────────────────────
reset-db:
	docker compose exec postgres psql -U fraud_user -d fraud_detection -c \
	"TRUNCATE fraud_records, valid_transactions;"
	@echo "Database tables cleared"

reset: down clean
	@echo "Full reset done"
