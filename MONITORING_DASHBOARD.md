# Monitoring Dashboard Setup

## Quick Start

Start the monitoring stack with Grafana, Prometheus, Pushgateway, and Kafka Exporter:

```bash
# Start monitoring services (includes core services)
scripts/codespace.sh up monitoring

# View dashboard URLs
scripts/services.sh urls

# Health check
scripts/services.sh smoke
```

Access Grafana dashboard: **http://localhost:3000** (admin / admin)

## Stack Components

### Core Monitoring Services

| Service | Port | Purpose | URL |
|---------|------|---------|-----|
| **Prometheus** | 9090 | Time-series metrics database | http://localhost:9090 |
| **Grafana** | 3000 | Dashboards and visualization | http://localhost:3000 |
| **Pushgateway** | 9091 | Metrics collection from batch jobs | http://localhost:9091 |
| **Kafka Exporter** | 9308 | Kafka consumer lag metrics | http://localhost:9308/metrics |

### Dependent Services (also started)

The monitoring profile includes all core services:
- Kafka (messages)
- Schema Registry (schemas)
- Kafka UI (Kafka visualization)
- MinIO (object storage)
- Spark Master & Worker (compute)

## Configuration Files

```
monitoring/
├── prometheus/
│   └── prometheus.yml       # Prometheus scrape config and alert rules
└── grafana/
    └── provisioning/
        ├── dashboards/      # Dashboard definitions
        └── datasources/     # Prometheus datasource config
```

### Prometheus Config

Located at `monitoring/prometheus/prometheus.yml`. Key settings:

```yaml
# Scrape interval (how often to collect metrics)
global:
  scrape_interval: 15s
  evaluation_interval: 15s

# Data retention
  tsdb:
    retention: 2d

# Scrape targets
scrape_configs:
  - job_name: 'kafka'       # Kafka exporter
  - job_name: 'pushgateway' # Spark job metrics
  - job_name: 'prometheus'  # Prometheus self-monitoring
```

### Grafana Setup

Default credentials: **admin** / **admin**

Grafana is pre-configured with:
- **Datasource**: Prometheus (http://prometheus:9090)
- **Dashboards**: Auto-provisioned from `monitoring/grafana/provisioning/dashboards/`

## Metrics Available

### Kafka Metrics (from kafka-exporter)

```
kafka_consumergroup_lag          # Consumer lag per group/topic/partition
kafka_brokers                     # Number of brokers
kafka_topic_partitions           # Partitions per topic
kafka_controller_active_controller_count  # Active controller
```

### Prometheus Metrics (self-monitoring)

```
prometheus_tsdb_symbol_table_size_bytes    # Database size
prometheus_tsdb_compactions_total          # Compaction count
up                                         # Service up/down status
```

### Spark Metrics (via Pushgateway)

Spark jobs can push metrics to `http://pushgateway:9091/metrics/job/<job_name>`:

```
spark_executor_deserialize_time    # Deserialization time
spark_shuffle_read_bytes           # Shuffle read bytes
spark_input_records_read           # Records processed
```

## Lab 10: Observability

This stack is used in Lab 10. Key tasks:

```bash
# Check targets in Prometheus
curl http://localhost:9090/api/v1/targets

# Query metrics via Prometheus
curl 'http://localhost:9090/api/v1/query?query=up'

# View Kafka consumer lag
curl 'http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag'

# Run a Spark job and push metrics
PYTHONPATH=src uv run python -m src.score --mode stream

# See metrics in Grafana dashboard
# http://localhost:3000 → Dashboards → Lab 10
```

## Troubleshooting

### Grafana not showing dashboards

1. Check datasource connection:
   - Grafana UI → Configuration → Data Sources → Prometheus
   - Should show "✓" if connected

2. Verify Prometheus is healthy:
   ```bash
   curl http://localhost:9090/-/ready
   ```

3. Reload dashboards:
   ```bash
   scripts/codespace.sh restart grafana
   ```

### Prometheus not scraping targets

1. Check target status:
   - http://localhost:9090/targets

2. View scrape errors:
   - http://localhost:9090/alerts

3. Check config:
   ```bash
   cat monitoring/prometheus/prometheus.yml
   ```

4. Restart Prometheus:
   ```bash
   scripts/codespace.sh restart prometheus
   ```

### Kafka metrics missing

1. Verify kafka-exporter is running:
   ```bash
   scripts/codespace.sh status | grep kafka-exporter
   ```

2. Test exporter directly:
   ```bash
   curl http://localhost:9308/metrics | grep kafka_
   ```

3. Check Kafka broker connectivity:
   ```bash
   /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092
   ```

### Pushgateway not receiving metrics

1. Verify Pushgateway is running:
   ```bash
   curl http://localhost:9091/-/ready
   ```

2. Push test metric:
   ```bash
   curl -X POST http://localhost:9091/metrics/job/test/instance/test \
     -d 'test_metric 42'
   ```

3. Query in Prometheus:
   ```bash
   curl 'http://localhost:9090/api/v1/query?query=test_metric'
   ```

## Useful Queries

### Prometheus PromQL Examples

```promql
# Service health
up{job="kafka"}                    # Is Kafka exporter up?
up{job="prometheus"}               # Is Prometheus up?

# Kafka consumer lag
kafka_consumergroup_lag{group="lab-lag-demo"}

# Spark metrics (when jobs are running)
rate(spark_input_records_read[1m]) # Records/second

# Alert rules
ALERTS{severity="warning"}         # Active warnings
```

## Alerting

Alert rules are defined in `monitoring/prometheus/prometheus.yml` under `rule_files`.

Common alerts:

| Alert | Trigger | Action |
|-------|---------|--------|
| `ConsumerLagHigh` | Consumer lag > threshold | Check job status |
| `PrometheusDown` | Prometheus scrape failure | Restart Prometheus |
| `KafkaDown` | Kafka broker down | Check Kafka logs |

View active alerts:
- http://localhost:9090/alerts
- Grafana: Alerting → Alert Rules

## Service Management

```bash
# Start/stop/restart monitoring services
scripts/codespace.sh start prometheus
scripts/codespace.sh restart grafana
scripts/codespace.sh stop kafka-exporter

# View logs
scripts/codespace.sh logs prometheus
scripts/codespace.sh logs grafana

# Full monitoring stack
scripts/codespace.sh up monitoring
scripts/codespace.sh down      # keeps data
scripts/codespace.sh clean     # deletes data
```

## Performance Considerations

- **Prometheus retention**: Set to 2 days by default (`--storage.tsdb.retention.time=2d`)
- **Scrape interval**: 15 seconds (affects load and latency)
- **Grafana memory**: ~100-200 MB
- **Pushgateway retention**: Metrics expire after 10 minutes of inactivity

For long-running labs, consider:
```bash
# Reduce retention to free disk space
# Edit monitoring/prometheus/prometheus.yml
# and restart: scripts/codespace.sh restart prometheus
```

## See Also

- [README.md](README.md) — Full lab documentation
- [CODESPACE_QUICKSTART.md](CODESPACE_QUICKSTART.md) — General codespace guide
- Lab 10 — Observability lab details
- Prometheus docs: https://prometheus.io/docs/
- Grafana docs: https://grafana.com/docs/grafana/latest/
