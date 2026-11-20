# Airport payments: Spark + Kafka lab. Run `make` or `make help` to list the commands.
SHELL := /bin/bash
.DEFAULT_GOAL := help
export PYTHONPATH := src
PY := .venv/bin/python
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0
SPARK_CONF := --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.driver.host=spark-master --conf spark.executor.memory=768m --conf spark.executor.cores=1 --conf spark.cores.max=1
SPARK_SUBMIT := /opt/spark/bin/spark-submit --driver-memory 768m \
                --master spark://spark-master:7077 $(SPARK_CONF) --packages $(SPARK_PACKAGES)
SPARK := docker compose exec -T spark-master $(SPARK_SUBMIT)
MC := docker compose exec -T minio-client mc
DATE ?=
PASSENGER ?=

help: ## Show the commands
	awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ───────── Setup and infrastructure ─────────
setup: ## Create the Python venv and install lab dependencies (host)
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt
	echo "venv ready: .venv"

up: ## Start the whole stack and wait for health checks
	docker compose up -d --build --wait
	$(MAKE) --no-print-directory urls

down: ## Stop the stack (keeps volumes)
	docker compose down

clean: ## Stop the stack and delete volumes and generated data
	docker compose down -v
	rm -rf data/raw/*.csv data/processed reports control/scenario

status: ## Show container status
	docker compose ps

urls: ## Print the lab URLs
	echo "Kafka UI         http://localhost:8080"
	echo "Spark master UI  http://localhost:8082   (application UI http://localhost:4040 during a job)"
	echo "MinIO console    http://localhost:9001   (minioadmin / minioadmin)"
	echo "Schema Registry  http://localhost:8081"
	echo "Kafka Connect    http://localhost:8083"
	echo "Prefect UI       http://localhost:4200"
	echo "Prometheus       http://localhost:9090"
	echo "Pushgateway      http://localhost:9091"
	echo "Grafana          http://localhost:3000"

# ───────── Step 1: data ─────────
test: ## Run the unit tests (rules, simulator)
	$(PY) -m pytest tests -q

generate: ## Simulate 60 days of airport payments into data/raw (labels stay separate)
	$(PY) -m lab.generate

bronze: ## Upload the raw files to MinIO (bronze bucket)
	$(MC) cp /data/raw/transactions.csv local/bronze/transactions/transactions.csv
	$(MC) cp /data/raw/passengers.csv local/bronze/passengers/passengers.csv
	$(MC) ls --recursive local/bronze/

silver: ## Spark: validate bronze and write silver Delta (optional DATE=YYYY-MM-DD)
	$(SPARK) /opt/spark-apps/silver_transactions.py $(if $(DATE),--date $(DATE),)

features: ## Spark: passenger features as of each day (no leakage)
	$(SPARK) /opt/spark-apps/features_30d.py

score: ## Spark: score one day with the rules (DATE=YYYY-MM-DD, default: latest)
	$(SPARK) /opt/spark-apps/score_batch.py $(if $(DATE),--date $(DATE),)

evaluate: ## Spark: precision and recall by DATE (default: latest scored day)
	$(SPARK) /opt/spark-apps/evaluate.py $(if $(DATE),--date $(DATE),)
	mkdir -p data/reports
	docker compose cp spark-master:/tmp/airport-evaluation.json data/reports/evaluation.json

batch: generate bronze silver features score evaluate ## Run the whole batch pipeline end to end

smoke-delta: ## Verify Spark + MinIO + Delta runtime using isolated temporary tables
	$(SPARK) /opt/spark-apps/smoke_delta.py

spark-job: ## Run an extra job from spark/jobs (JOB=explore_silver.py)
	@test -n "$(JOB)" && test "$(notdir $(JOB))" = "$(JOB)" && test -f "spark/jobs/$(JOB)" || { echo "Set JOB to a file in spark/jobs/"; exit 1; }
	$(SPARK) /opt/spark-apps/$(JOB)

history: ## Delta history of the gold features table (commits, operations)
	$(SPARK) /opt/spark-apps/history.py

blocked-passengers: ## Scored payments of one passenger (PASSENGER=7, default 7)
	$(SPARK) /opt/spark-apps/blocked_payments.py $(or $(PASSENGER),7) $(if $(DATE),--date $(DATE),)

blocklist-version: ## Blocklist as of a Delta version (V=2)
	$(SPARK) /opt/spark-apps/blocklist_version.py $(V)

# ───────── Step 2: CDC blocklist ─────────
cdc-setup: ## Create the blocklist table and topic, then register Debezium
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
	  --topic fraud.public.passenger_blocklist --partitions 1 --replication-factor 1 --config retention.ms=-1
	docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d airport -q < cdc/blocklist.sql
	@for i in $$(seq 1 60); do curl -fsS http://localhost:8083/connectors >/dev/null 2>&1 && break; sleep 2; done
	python3 -c 'import json; print(json.dumps(json.load(open("cdc/blocklist-connector.json"))["config"]))' | \
	curl -fsS -X PUT -H "Content-Type: application/json" --data-binary @- http://localhost:8083/connectors/blocklist-connector/config

cdc-status: ## Show the connector state
	curl -fsS http://localhost:8083/connectors/blocklist-connector/status && echo

block: ## Block a passenger (PASSENGER=42 REASON="stolen card")
	@test -n "$(PASSENGER)" || { echo "Set PASSENGER (e.g. PASSENGER=42)"; exit 1; }
	printf '%s\n' "INSERT INTO passenger_blocklist VALUES (:'passenger'::bigint, :'reason', (extract(epoch from clock_timestamp()) * 1000)::bigint, NULL) ON CONFLICT (passenger_id) DO UPDATE SET unblocked_at = NULL, reason = EXCLUDED.reason, blocked_at = EXCLUDED.blocked_at;" | \
	docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d airport -v "passenger=$(PASSENGER)" -v "reason=$(or $(REASON),manual_review)"

unblock: ## Unblock a passenger (PASSENGER=42)
	@test -n "$(PASSENGER)" || { echo "Set PASSENGER (e.g. PASSENGER=42)"; exit 1; }
	printf '%s\n' "UPDATE passenger_blocklist SET unblocked_at = (extract(epoch from clock_timestamp()) * 1000)::bigint WHERE passenger_id = :'passenger'::bigint;" | \
	docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d airport -v "passenger=$(PASSENGER)"

blocklist: ## Spark: MERGE CDC changes into the active blocklist (Delta, change feed on)
	$(SPARK) /opt/spark-apps/blocklist_from_cdc.py

blocklist-changes: ## Delta Change Data Feed: every block (insert) and unblock (delete)
	$(SPARK) /opt/spark-apps/blocklist_changes.py

# ───────── Step 3: streaming ─────────
scenario-normal: ## Live traffic: normal behaviour
	mkdir -p control && echo normal > control/scenario && echo "scenario: normal"

scenario-drift: ## Live traffic: holiday season (data drift)
	mkdir -p control && echo drift > control/scenario && echo "scenario: drift"

scenario-attack: ## Live traffic: card-testing attack (fraud spike)
	mkdir -p control && echo attack > control/scenario && echo "scenario: attack"

stream-start: ## Spark Structured Streaming: score live payments (detached)
	docker compose exec -T spark-master bash -c '! pgrep -f "[o]pt/spark-apps/stream_fraud.py" >/dev/null' \
	  || { echo "The streaming job is already running or Spark is not ready"; exit 1; }
	docker compose exec -d spark-master bash -c 'exec "$$@" >/tmp/stream-fraud.log 2>&1' _ $(SPARK_SUBMIT) /opt/spark-apps/stream_fraud.py
	@echo "Streaming job submitted. Check make stream-logs, then visit http://localhost:4040."

stream-logs: ## Show recent logs from the streaming job
	docker compose exec -T spark-master tail -n 60 /tmp/stream-fraud.log

stream-stop: ## Stop the streaming job
	docker compose exec -T spark-master pkill -f '[o]pt/spark-apps/stream_fraud.py' || true

stream-produce: ## Payment terminals: send live payments (DURATION=20, or Ctrl+C to stop)
	$(PY) -m lab.producer $(if $(DURATION),--seconds $(DURATION),)

alerts: ## Read the fraud alerts topic from Kafka (Ctrl+C to stop)
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
	--topic airport.fraud-alerts --from-beginning

lag-consume: ## Read payments in a demo consumer group that commits Kafka offsets (Ctrl+C to stop)
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
	  --topic airport.transactions --group lab-lag-demo --from-beginning \
	  --consumer-property enable.auto.commit=true --consumer-property auto.commit.interval.ms=1000

lag: ## Consumer lag for every consumer group
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
	--all-groups --describe

# ───────── Step 4: orchestration ─────────
pipeline: ## Run the daily pipeline once as a Prefect flow (DATE=YYYY-MM-DD)
	PREFECT_API_URL=http://localhost:4200/api $(PY) flows/pipeline.py $(DATE)

automate: ## Serve the daily flow as a Prefect deployment (cron 06:00)
	PREFECT_API_URL=http://localhost:4200/api $(PY) flows/pipeline.py serve

mask: ## Spark: analyst-safe dataset without direct identifiers
	$(SPARK) /opt/spark-apps/mask_payments.py
