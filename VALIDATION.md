# Lab review and validation

The runtime review used Docker Desktop on arm64 with about 8 GB allocated. Kafka, Postgres, Debezium, MinIO, and the Prefect API ran in Compose. Spark 3.5.3 ran both in local mode and on the Compose master/worker, using the Apache base image with Delta 3.1.0 and the Kafka/S3A libraries staged separately. This exercised the jobs and live services without relying on the final pandas/PyArrow image build or the standard `--packages` download path.

| Check | Result |
|---|---|
| `make test`, Python compile, `docker compose config -q` | 15 unit tests pass; Python and Compose validation pass |
| Terminal simulator → Kafka | Published live payments, consumed one, and validated its JSON schema and key |
| `make generate` and `make bronze` | 72,000 generated payments; MinIO bucket creation and bronze uploads passed |
| Spark/Delta on MinIO | Isolated smoke test passed in local and standalone worker modes for writes, `replaceWhere`, time travel, `MERGE`, and Change Data Feed |
| Silver, features, quarantine | 72,000 accepted payments; 60 feature snapshot days; all 4 injected bad records rejected with the expected reasons |
| Postgres → Debezium → Kafka → Spark → Delta on MinIO | Blocklist counts 1 → 2 → 1; CDF insert/delete and time travel to the two-row version passed. Replaying the same source events also produced no duplicate CDF rows in the file-backed check |
| Masked dataset | Local Delta run produced 72,000 masked rows with no direct identifiers |
| Prefect | Local server responded; the flow ran its tasks in order with stubbed `make` calls and passed/failed the precision gate as expected |
| Prometheus | `promtool` accepted the configuration and all 3 alert rules |

Still to verify using the normal Compose setup: final Spark image build and default dependency resolution, PyArrow-dependent batch and streaming scoring, live Prefect execution of Spark jobs, and the Schema Registry exercise. In this review, the image rebuild hit a PyPI read timeout for PyArrow and the Schema Registry image was still downloading. Test these before delivery.

```bash
make setup
make up
make smoke-delta
make batch
make stream-start
make stream-logs                   # wait for both queries to start
make stream-produce DURATION=30
# Verify alerts/checkpoints before stopping the stream.
make stream-stop
make pipeline
```

Use a fresh lab environment for the expected initial counts in Lab 08; a previous review or practice run leaves Kafka events, Delta versions, and CDC state behind. `make clean` deletes lab volumes and generated data, so use it only when that reset is intended.
