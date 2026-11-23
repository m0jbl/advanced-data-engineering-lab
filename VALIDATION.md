# Lab review and validation

> **Note (2026-11-21):** the Makefile was removed. The results below were recorded with the equivalent `make` targets; the commands now follow [README.md](README.md) (explicit `docker compose`, `spark-submit`, and `uv run` commands).

Last run: 2026-11-20, Docker 29.9.3 on an arm64 laptop with about 8 GB allocated to Docker. Fresh volumes (`docker compose down -v` first).

Versions under test: Kafka 4.4.0 (KRaft), Schema Registry 8.4.1, Debezium 3.7.1.Final, Postgres 16, Spark 4.2.0 (Scala 2.13, Java 17) with Delta 4.4.1, spark-sql-kafka 4.2.0 and hadoop-aws 3.5.0, Prometheus 3.16.0, Pushgateway 1.11.5, kafka-exporter 1.11.0, Grafana 13.3.1, Prefect 3.9.4, kafbat/kafka-ui 1.5.0, host Python 3.12 (managed by uv, locked in uv.lock).

Two Spark images were tested. The first pass (tables below) used an `apache/spark:4.2.0` build with `--packages` resolved at run time. The second pass used the slim image `ghcr.io/majlisofai/advanced-data-engineering-spark:4.2.0`, which has the jars baked in and no `--packages` (built locally from `spark/Dockerfile`; not yet pulled from the registry). See "Slim Spark image" below.

## Per-lab stacks (`make up` / `make up LAB=NN`, then `make smoke`)

Each profile was brought up alone with core (`make down` between runs) so that each run fit in 8 GB.

| Stack | `make smoke` | Approx. container memory |
|---|---|---|
| core (`make up`) | Kafka UI, Schema Registry, Spark master, MinIO, both topics ok; Delta smoke PASS | — |
| core + cdc (`make up LAB=08`) | core + Kafka Connect + Postgres ok; Delta smoke PASS | 2.2 GiB |
| core + monitoring (`make up LAB=10`) | core + Prometheus, Pushgateway, kafka-exporter, Grafana ok; Delta smoke PASS | 2.5 GiB |
| core + orchestration (`make up LAB=11`) | core + Prefect ok; Delta smoke PASS | 2.0 GiB |

With only the core stack up, `make score` runs to completion and prints `metrics not pushed …` instead of failing, because the Pushgateway is not running.

## Functional checks

| Check | Result |
|---|---|
| `make test` | 15 passed |
| `docker compose config -q`, Python compile of all jobs, `promtool check config` | pass; 3 alert rules |
| `npx @devcontainers/cli read-configuration` | `.devcontainer/devcontainer.json` parses |
| Producer → Kafka 4.4 (idempotent producer) | Payments published; console consumer with `--formatter-property print.key=true --formatter-property print.partition=true` shows key and partition |
| `make lag-consume` / `make lag` | Group `lab-lag-demo` commits offsets and shows up in `kafka-consumer-groups --describe`. After `make down` and `make up LAB=10`, lag 3 and 9 appears both in `make lag` and as `kafka_consumergroup_lag` in kafka-exporter; Prometheus targets `kafka` and `pushgateway` are `up`; `make score` pushes to the Pushgateway |
| Schema Registry 8.4.1 | JSON Schema `schemas/payment_event.json` registered under `airport.transactions-value` (id 1), then deleted |
| `make generate`, `make bronze` | 72,000 payments, 2,000 passengers; uploaded to MinIO |
| `make smoke-delta` (Spark 4.2 + Delta 4.4.1 on MinIO) | PASS: write, replaceWhere, time travel, MERGE, Change Data Feed |
| `make silver`, `features`, `score`, `evaluate` (ANSI mode on) | silver 72,000 / rejected 0; 60 feature days; 2026-11-19: 1,208 payments, 11 alerts, precision 0.6364, recall 0.28 |
| Streaming (`make stream-start`, producer in attack scenario) | Alerts written to `airport.fraud-alerts`; scores to `gold/stream_scores`. One hand-made message with `event_timestamp: "not-a-time"` went to `quarantine/stream_transactions` as `BAD_TIMESTAMP` and the query kept running |
| CDC: `make cdc-setup`, `block`/`unblock` 42, `make blocklist`, `blocklist-changes` | Connector and task `RUNNING` on 3.7.1; events carry `__op` and `__deleted` (`delete.tombstone.handling.mode=rewrite`); active blocklist = 1 (passenger 7) |
| `make pipeline DATE=2026-11-19` (Prefect 3.9.4) | All 6 tasks Completed; quality gate passed (precision 0.6364) |

## Slim Spark image (`ghcr.io/majlisofai/advanced-data-engineering-spark:4.2.0`)

Run with `make up LAB=08` and no `spark_ivy` volume. The container runs as `spark` (uid 185) in `/home/spark`, and `/opt/spark/bin/spark-submit` is present.

| Check | Result |
|---|---|
| `make smoke` | All core and cdc services ok; Delta smoke PASS. No jar download, so the Spark step took seconds |
| `make silver`, `features`, `score`, `evaluate` | Same as the first pass: 72,000 / 0 rejected; 60 days; 1,208 payments, 11 alerts, precision 0.6364, recall 0.28. With the monitoring profile down, `score` prints `metrics not pushed` and exits 0 |
| CDC | Connector and task `RUNNING`; after `block PASSENGER=99`, `make blocklist` gives 2 active passengers |
| Streaming | Fresh checkpoints, attack scenario: 12 alerts on `airport.fraud-alerts`, 0 errors in the log. A message with `event_timestamp: "31/02/2026 25:00"` went to `quarantine/stream_transactions` as `BAD_TIMESTAMP`, and the query kept running |

## Fixes made during this run

- Kafka Connect: image default heap (`-Xmx2G`) exceeded the 1 GB `mem_limit` and the container restarted during plugin scanning. Set `HEAP_OPTS=-Xms256m -Xmx640m` and `CONNECT_PLUGIN_DISCOVERY=service_load`.
- Schema Registry 8.x image has no `curl`; the health check now uses `python3`.
- Kafka wrote to `/tmp/kraft-combined-logs` inside the container, not to the `kafka_data` volume, so `make down` lost every topic, event and offset. `KAFKA_LOG_DIRS=/var/lib/kafka/data` fixes this; data now survives `make down` / `make up`.
- Spark: jars are baked into the image, so `--packages` and the `spark_ivy` volume are gone. (In the first pass, Spark 4 put the Ivy cache under `$HOME/.ivy2.5.2`, with `HOME=/nonexistent`.)
- `make up --wait` failed on the one-shot `kafka-init` exit; `spark-master` now depends on `kafka-init` completing.
- Streaming parse uses `try_to_timestamp` and quarantines null keys or bad timestamps, as the batch silver job does.

## Not covered and known limits

- Full Lab 10 alert firing (`ConsumerLagHigh` pending for 2 minutes) and Grafana dashboard building were not run end to end.
- `make automate` (Prefect serve) was not run; `make pipeline` was.
- If Kafka data is wiped but `checkpoints` in MinIO are kept (for example, deleting only the Kafka volume), the stream stops with `KafkaIllegalStateException: Some data may have been lost`. `make clean` wipes both. To reset only the stream, delete `checkpoints/fraud_*` in MinIO.
- `make up-all` (every profile together) was not run on 8 GB; use 12 GB for it.
- Codespaces (one Alpine 3.22 container, no Docker; `scripts/codespace.sh`): built and run locally on arm64 (Codespaces is x86_64). Checked: `uv sync --locked` builds confluent-kafka 2.10.0 against Alpine's librdkafka 2.10.0, unit tests pass; `up` (core), `up cdc`, `up monitoring`, `up orchestration` all healthy and `scripts/services.sh smoke` ok for all 13 services; the labs' codespace commands ran for bronze upload, Spark/Delta smoke, silver, features, batch scoring (`mapInPandas` on Alpine's pandas 2.2.3 / pyarrow 19.0.1), evaluate + `cp`, the streaming scorer with the producer (alerts published), Lab 08 CDC (connector RUNNING, `blocklist_from_cdc.py`), Lab 10 targets and Grafana health, and the Lab 11 flow end to end; `stop`/`start`/`down` work. The first `up` of the core stack downloads about 1.3 GB (23 minutes on the test connection; Kafka comes from the Apache CDN). Not yet run in a real codespace.
