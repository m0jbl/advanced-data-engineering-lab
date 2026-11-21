#!/usr/bin/env bash
# URLs and health checks for the services that are running now (core and any started profiles).
# Usage: scripts/services.sh urls | smoke
set -euo pipefail
mode=${1:?usage: services.sh urls|smoke}
running=$(docker compose --profile '*' ps --services --status running)

# service | label | URL shown to learners | URL checked by smoke
table='kafka-ui|Kafka UI|http://localhost:8080|http://localhost:8080
schema-registry|Schema Registry|http://localhost:8081|http://localhost:8081/subjects
spark-master|Spark master UI|http://localhost:8082   (application UI http://localhost:4040 during a job)|http://localhost:8082
minio|MinIO console|http://localhost:9001   (minioadmin / minioadmin)|http://localhost:9000/minio/health/live
kafka-connect|Kafka Connect|http://localhost:8083|http://localhost:8083/connectors
prefect-server|Prefect UI|http://localhost:4200|http://localhost:4200/api/health
prometheus|Prometheus|http://localhost:9090|http://localhost:9090/-/ready
pushgateway|Pushgateway|http://localhost:9091|http://localhost:9091/-/ready
kafka-exporter|Kafka exporter|http://localhost:9308/metrics|http://localhost:9308/metrics
grafana|Grafana|http://localhost:3000|http://localhost:3000/api/health'

failed=0
while IFS='|' read -r service label url check; do
  grep -qx "$service" <<<"$running" || continue
  if [[ $mode == urls ]]; then
    printf '%-16s %s\n' "$label" "$url"
  elif curl -fsS -o /dev/null --max-time 5 "$check"; then
    echo "ok   $label  $check"
  else
    echo "FAIL $label  $check"; failed=1
  fi
done <<<"$table"

if [[ $mode == smoke ]]; then
  grep -qx kafka <<<"$running" || { echo "FAIL kafka is not running: run docker compose up -d --wait"; exit 1; }
  topics=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list)
  for t in airport.transactions airport.fraud-alerts; do
    grep -qx "$t" <<<"$topics" && echo "ok   topic $t" || { echo "FAIL topic $t"; failed=1; }
  done
  if grep -qx postgres <<<"$running"; then
    docker compose exec -T postgres pg_isready -U postgres -d airport >/dev/null && echo "ok   Postgres" \
      || { echo "FAIL Postgres"; failed=1; }
  fi
  exit $failed
fi
