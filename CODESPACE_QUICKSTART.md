# GitHub Codespace Quick Start

## Overview

This lab runs in a lightweight Alpine Linux container with no Docker. Each service (Kafka, Spark, MinIO, etc.) runs as a process managed by `scripts/codespace.sh`.

## First Time Setup

The codespace environment is configured automatically:

1. **Container creation** runs `uv sync --locked`
   - Installs Python 3.12 (from `.python-version`)
   - Installs all dependencies into `.venv` (including PySpark)
   - Opens in ~5 minutes

2. **Starting services** (first time per lab):
   ```bash
   scripts/codespace.sh up                 # core stack (Kafka, Spark, MinIO, etc.)
   # or for specific labs:
   scripts/codespace.sh up cdc             # Lab 08: add Postgres + Kafka Connect
   scripts/codespace.sh up monitoring      # Lab 10: add Prometheus + Grafana
   scripts/codespace.sh up orchestration   # Lab 11: add Prefect
   scripts/codespace.sh up all             # all services
   ```

   - **First run**: installs service binaries (~1.3 GB), takes 5-10 minutes
   - **Subsequent runs**: skips install, just starts services (~30 seconds)
   - **Spark optimization**: automatically uses PySpark from `.venv` (via `uv sync`)

## Service Management

```bash
scripts/codespace.sh status                 # list running services
scripts/codespace.sh logs <service>         # tail service logs
scripts/codespace.sh start <service>        # start one service
scripts/codespace.sh stop <service>         # stop one service
scripts/codespace.sh restart <service>      # restart one service
scripts/codespace.sh down                   # stop all services (keep data)
scripts/codespace.sh clean                  # stop all services + delete data
```

## Accessing Services

Open the **Ports** tab in Codespace to see live URLs:

| Service | Port | URL | Credentials |
|---------|------|-----|-------------|
| Kafka UI | 8080 | http://localhost:8080 | — |
| Schema Registry | 8081 | http://localhost:8081 | — |
| Spark Master UI | 8082 | http://localhost:8082 | — |
| MinIO Console | 9001 | http://localhost:9001 | minioadmin / minioadmin |
| Spark App UI | 4040 | http://localhost:4040 | (during job execution) |
| Prometheus | 9090 | http://localhost:9090 | — |
| Grafana | 3000 | http://localhost:3000 | admin / admin |
| Prefect | 4200 | http://localhost:4200 | — |

Or print URLs programmatically:
```bash
scripts/services.sh urls
```

## Running Lab Commands

**Examples:**

```bash
# Run unit tests (no services needed)
PYTHONPATH=src uv run python -m pytest tests -q

# Generate sample data to Kafka
PYTHONPATH=src uv run python -m src.generate --count 1000

# Submit a Spark job (runs locally on spark-master)
/opt/spark/bin/spark-submit /opt/spark-apps/smoke_delta.py

# Check health of all running services
scripts/services.sh smoke

# View Kafka topics
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# List MinIO buckets
mc ls local/
```

**Key paths:**

| Path | Purpose |
|------|---------|
| `/opt/spark/bin/spark-submit` | Spark job submission |
| `/opt/spark-apps` | Spark job files (from `spark/jobs/`) |
| `/data` | Sample input data (from `data/`) |
| `/opt/kafka/bin` | Kafka CLI tools |

## Troubleshooting

### Services won't start
```bash
# Check status
scripts/codespace.sh status

# View logs
scripts/codespace.sh logs kafka
scripts/codespace.sh logs spark-master

# Full clean rebuild
scripts/codespace.sh clean
scripts/codespace.sh up core
```

### Spark fails with "pyspark not found"
```bash
# Ensure venv is set up
uv sync --locked

# Restart Spark
scripts/codespace.sh restart spark-master
```

### MinIO connection issues
```bash
# Check if MinIO is running
scripts/codespace.sh status | grep minio

# View MinIO logs
scripts/codespace.sh logs minio

# Restart MinIO
scripts/codespace.sh restart minio
```

### Ports already in use
Ports are only exposed if the service is running. Stop conflicting services:
```bash
scripts/codespace.sh down
```

## Performance Tips

1. **Reuse codespaces**: Each codespace maintains persistent service data, so services start much faster on second run
2. **Selective startup**: Only start the services needed for your lab
3. **Local testing**: Run unit tests locally without starting services:
   ```bash
   PYTHONPATH=src uv run python -m pytest tests -q
   ```

## Environment Variables

Useful variables for development:

```bash
# Run Python with project venv
source .venv/bin/activate
python -c "import pyspark; print(pyspark.__version__)"

# Or use uv to run with venv:
uv run python -c "import pyspark; print(pyspark.__version__)"

# Set Spark logging
export PYSPARK_LOG_LEVEL=DEBUG
/opt/spark/bin/spark-submit /opt/spark-apps/job.py
```

## See Also

- [README.md](README.md) — Full documentation
- [SPARK_INSTALLATION_WORKAROUND.md](SPARK_INSTALLATION_WORKAROUND.md) — Technical details on Spark setup
- `scripts/codespace.sh` — Full service management script
- `scripts/services.sh` — Health checks and URL display
