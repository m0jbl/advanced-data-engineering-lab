# Airport Payments: Spark + Kafka Lab

Hands-on lab for the **DataOps using Spark and Kafka** course. Every lab uses one dataset: card payments at
an airport (tickets, duty free, currency exchange, food, car rental). Some are fraudulent. The lab builds a
pipeline that lands, validates, enriches, scores, and monitors them, all on your laptop with Docker Compose.

Adapted from the local MLOps lab *Airport Real-Time Fraud Detection*. The simulator is reused as-is. The ML
stack (Feast, MLflow, DVC, FastAPI, Evidently) is replaced by Spark jobs and transparent rules.

## Quick start

```bash
make setup      # Python venv on the host (for the generator and the producer)
make up         # start the stack and wait for health checks (first run pulls images: allow 10 minutes)
make urls       # print the UIs
make test       # unit tests (no Docker needed)
make smoke-delta # writes and removes isolated test tables on MinIO
```

Allocate at least 12 GB to Docker for the full stack (8 GB is not enough for a reliable end-to-end run). Python 3.11 is recommended; 3.9+ works.

## What runs where

| Component | Where | Port (host) | Role |
|---|---|---|---|
| Kafka 3.9 (KRaft) | container | 9094 | Payment events, fraud alerts |
| Kafka UI | container | 8080 | Browse topics and messages |
| Schema Registry | container | 8081 | Contracts for topics (Lab 03) |
| Postgres + Debezium (Kafka Connect) | containers | 5432 / 8083 | Blocklist changes as CDC events (Lab 08) |
| MinIO (S3 API) | container | 9000 / 9001 | Bronze, silver, gold Delta tables |
| Spark master + worker | containers | 7077 / 8082 | Batch and streaming jobs (`spark/jobs/`) |
| Prefect | container | 4200 | Pipeline runs, schedule, retries (Lab 11) |
| Prometheus, Pushgateway, kafka-exporter, Grafana | containers | 9090 / 9091 / 9308 / 3000 | Metrics and alerts (Lab 10) |
| Payment terminals (simulator) | host (`make stream-produce`) | - | Sends payments to Kafka |

## Data flow

```
simulator ──► data/raw/*.csv ──► bronze (MinIO) ──► silver (Delta, by day) ──► features (as of each day)
                                                                                     │
Postgres blocklist ──► Debezium ──► Kafka ──► blocklist (silver) ──────────────────► score ──► gold scores
                                                                                     │         evaluate (ground truth)
producer ──► airport.transactions ──► Spark Streaming ──► airport.fraud-alerts (Kafka)
                                                       └──► gold/stream_scores (Delta)
```

## Commands

Run `make help` for the full list. The main ones:

| Command | What it does |
|---|---|
| `make generate` | Simulate 60 days of payments (labels written separately) |
| `make bronze` | Upload the raw files to MinIO |
| `make silver DATE=...` | Validate and write one UTC day (omit DATE to process all bronze rows) |
| `make features` | Passenger features as of each day (no leakage) |
| `make score DATE=2026-11-18` | Score one day with the rules |
| `make evaluate` | Precision and recall against the ground truth |
| `make batch` | Everything above, in order |
| `make cdc-setup` | Create the blocklist table and register Debezium |
| `make block PASSENGER=42 REASON="stolen card"` | Block a passenger (captured as CDC) |
| `make blocklist` | MERGE CDC changes into the active blocklist (Delta) |
| `make blocklist-changes` | Read the Delta change feed of the blocklist |
| `make history` | Delta commit history of the features table |
| `make blocklist-version V=2` | The blocklist as of an earlier Delta version |
| `make scenario-normal / -drift / -attack` | Choose the live traffic pattern |
| `make stream-start` / `make stream-logs` | Start the detached scorer and check when its Kafka queries are up |
| `make stream-produce DURATION=20` | Send live payments for 20 seconds (omit DURATION to run until Ctrl+C) |
| `make alerts` | Read `airport.fraud-alerts` from Kafka |
| `make lag` | Consumer lag per group |
| `make pipeline DATE=...` | Run the daily flow once against the local Prefect server (omit DATE to use the latest silver day) |
| `make automate` | Serve the daily flow on a schedule |
| `make down` / `make clean` | Stop (keep data) / stop and delete volumes |

## Fraud rules

Rules are in `spark/jobs/fraud_rules.py` and documented in the course. Each rule adds points; a payment
at score 50 or more is an alert.

| Rule | Points | Meaning |
|---|---|---|
| `AMOUNT_SPIKE` | 40 | Amount at least 4× the passenger's 30-day average |
| `NIGHT_PAYMENT` | 15 | Between 00:00 and 05:59 |
| `LAST_MINUTE` | 15 | Less than 30 minutes before departure |
| `CARD_NOT_PRESENT_HIGH_RISK` | 20 | No card present at exchange or electronics |
| `NEW_FOREIGN_PATTERN` | 10 | International payment with fewer than 2 airports in 30 days |
| `NEW_ACCOUNT` | 10 | Account younger than 30 days |
| `BLOCKED_PASSENGER` | 100 | Passenger on the blocklist |

The labels never reach the pipeline. Only `make evaluate` reads them.

## Scenarios

`make scenario-normal` (baseline), `make scenario-drift` (holiday: bigger baskets, night flights), and
`make scenario-attack` (card-testing ring: fraud rate rises several-fold). Switch them while the producer
runs; the change takes effect on the next payment.

## Layout

```
spark/jobs/   Spark jobs and the rule module (mounted into the Spark containers)
src/lab/      simulator, batch generator, Kafka producer (host)
flows/        Prefect flow for the daily pipeline
cdc/          blocklist table and Debezium connector definition
monitoring/   Prometheus, alert rules, Grafana datasource
tests/        unit tests for the rules and the simulator
params.yaml   simulation size and seed
```

## Versions to check before a class

- Spark uses `apache/spark:3.5.3-python3`, built locally with pandas and PyArrow (`spark/Dockerfile`).
  MinIO and its client use archived `bitnamilegacy` images: mirror these before a course; upstream images
  can disappear. Before class, run `docker compose pull --ignore-buildable` and `docker compose build spark-master`.
  Run `make up`, `make smoke-delta`, and `make batch` on the same machine/network learners will use.
- The first Spark run downloads Kafka, Hadoop S3 and Delta jars from Maven. Offline classrooms need a
  pre-warmed image/JAR cache. `make up` needs internet for images; check it *before* learners arrive.
- Use the recommended 12 GB Docker allocation for the full stack. See [VALIDATION.md](VALIDATION.md) for runtime checks that passed and what still needs testing before class.
