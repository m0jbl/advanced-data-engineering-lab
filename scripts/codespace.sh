#!/usr/bin/env bash
# The lab stack in a GitHub Codespace: every service runs as a process in this one container, no Docker.
# On a laptop use Docker Compose instead (docker-compose.yml); this script is for the codespace only.
#
#   scripts/codespace.sh up [cdc|monitoring|orchestration|all]   install on first use, start, wait until healthy
#   scripts/codespace.sh down                                     stop every service, keep the data
#   scripts/codespace.sh clean                                    stop every service and delete its data
#   scripts/codespace.sh status                                   running services
#   scripts/codespace.sh logs <service> | restart <service> | stop <service> | start <service>
#
# `up` with no stack starts the core (Kafka, Schema Registry, Kafka UI, MinIO, Spark), like `docker compose up -d --wait`.
# Service names, host names, ports and paths match the compose file, so the lab commands differ only in the
# `docker compose exec <service>` prefix: /opt/kafka/bin, /opt/spark/bin/spark-submit /opt/spark-apps/<job>.py,
# mc ... local/..., psql -U postgres -d airport.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
DATA=/var/lib/lab   # service data (kept by `down`, deleted by `clean`)
LOGS=/var/log/lab
RUN=/run/lab
ARCH=$(uname -m); case $ARCH in x86_64) GOARCH=amd64 ;; aarch64) GOARCH=arm64 ;; *) echo "unsupported $ARCH" >&2; exit 1 ;; esac

# Versions: the same as the images in docker-compose.yml (pushgateway: latest published binary)
KAFKA=4.4.0; CONFLUENT=8.4.1; KAFKA_UI=1.5.0; SPARK=4.2.0; DELTA=4.4.1; HADOOP=3.5.0; AWS_SDK=2.35.4
DEBEZIUM=3.7.1.Final; PROMETHEUS=3.16.0; PUSHGATEWAY=1.11.4; KAFKA_EXPORTER=1.11.0; GRAFANA=13.3.1

CORE="kafka schema-registry kafka-ui minio spark-master spark-worker"
CDC="postgres kafka-connect"
MONITORING="prometheus pushgateway kafka-exporter grafana"
ORCHESTRATION="prefect-server"
ALL="$CORE $CDC $MONITORING $ORCHESTRATION"
HOSTS="kafka schema-registry kafka-ui minio spark-master spark-worker postgres kafka-connect prometheus pushgateway kafka-exporter grafana prefect-server"

say() { printf '\033[1m%s\033[0m\n' "$*"; }
fetch() { curl -fsSL --retry 3 -o "$2" "$1"; }   # url file
apk_add() { apk add --no-cache -q "$@"; }

# ---------- install (once, on first `up` of a stack) ----------
install_java() { command -v java >/dev/null || { say "Installing Java 21"; apk_add openjdk21-jre-headless; }; }

install_kafka() {
  [ -x /opt/kafka/bin/kafka-server-start.sh ] && return
  install_java; say "Installing Kafka $KAFKA"
  # the CDN hosts current releases and is fast; the archive keeps every release but is slow
  fetch "https://dlcdn.apache.org/kafka/$KAFKA/kafka_2.13-$KAFKA.tgz" /tmp/kafka.tgz \
    || fetch "https://archive.apache.org/dist/kafka/$KAFKA/kafka_2.13-$KAFKA.tgz" /tmp/kafka.tgz
  mkdir -p /opt/kafka && tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 && rm /tmp/kafka.tgz
}

install_schema_registry() {
  [ -x /opt/confluent/bin/schema-registry-start ] && return
  install_java; say "Installing Schema Registry $CONFLUENT (Confluent community, 440 MB)"
  fetch "https://packages.confluent.io/archive/${CONFLUENT%.*}/confluent-community-$CONFLUENT.tar.gz" /tmp/confluent.tgz
  mkdir -p /opt/confluent && tar -xzf /tmp/confluent.tgz -C /opt/confluent --strip-components=1 && rm /tmp/confluent.tgz
}

install_kafka_ui() {
  [ -f /opt/kafka-ui/kafka-ui.jar ] && [ -x /opt/kafka-ui/jre/bin/java ] && return
  # Kafka UI 1.5 is built for Java 25, which Alpine 3.22 does not package: Temurin's Alpine JRE, for Kafka UI only
  say "Installing Kafka UI $KAFKA_UI (with its own Java 25 runtime)"
  local jarch; case $ARCH in x86_64) jarch=x64 ;; aarch64) jarch=aarch64 ;; esac
  mkdir -p /opt/kafka-ui/jre
  fetch "https://api.adoptium.net/v3/binary/latest/25/ga/alpine-linux/$jarch/jre/hotspot/normal/eclipse" /tmp/jre25.tgz
  tar -xzf /tmp/jre25.tgz -C /opt/kafka-ui/jre --strip-components=1 && rm /tmp/jre25.tgz
  fetch "https://github.com/kafbat/kafka-ui/releases/download/v$KAFKA_UI/api-v$KAFKA_UI.jar" /opt/kafka-ui/kafka-ui.jar
}

install_minio() {
  command -v minio >/dev/null && command -v mc >/dev/null && return
  say "Installing MinIO and mc"
  apk_add minio minio-client
  command -v mc >/dev/null || ln -sf "$(command -v mcli)" /usr/local/bin/mc   # Alpine names the client mcli
}

install_spark() {
  local installed="/opt/spark-venv/.installed-$SPARK-$DELTA"
  [ -f "$installed" ] && [ -x /opt/spark/bin/spark-submit ] && return
  
  # Check if Spark is already installed in the project venv (from uv sync)
  local project_venv="$REPO/.venv"
  if [ -d "$project_venv" ]; then
    local pyspark_path; pyspark_path=$("$project_venv/bin/python" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))' 2>/dev/null) || true
    if [ -n "$pyspark_path" ]; then
      say "Using Spark $SPARK from project venv ($project_venv)"
      mkdir -p /opt/spark/conf
      ln -sfn "$pyspark_path" /opt/spark
      ln -sf "$REPO/spark/conf/spark-defaults.conf" /opt/spark/conf/spark-defaults.conf
      echo "export PYSPARK_PYTHON=$project_venv/bin/python" > /opt/spark/conf/spark-env.sh
      touch "$installed"
      
      # Try to bake jars if available, otherwise continue without them (some labs don't need Kafka/S3A)
      if [ -f "$REPO/spark/bake_jars.py" ]; then
        SPARK_HOME=/opt/spark LAB_SPARK_EXCLUDES="software.amazon.awssdk:bundle" \
        LAB_SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:$SPARK,io.delta:delta-spark_2.13:$DELTA,org.apache.hadoop:hadoop-aws:$HADOOP,software.amazon.awssdk:s3:$AWS_SDK,software.amazon.awssdk:s3-transfer-manager:$AWS_SDK,software.amazon.awssdk:sts:$AWS_SDK,software.amazon.awssdk:apache-client:$AWS_SDK" \
          "$project_venv/bin/python" "$REPO/spark/bake_jars.py" 2>/dev/null | awk '/^added/ {n++} END {if (n+0 > 0) print "  " n+0 " jars added"}' || true
      fi
      return
    fi
  fi
  
  # Fallback: install in isolated venv
  install_java; say "Installing Spark $SPARK with Delta $DELTA and the Kafka/S3A jars"
  # pandas, numpy, pyarrow from Alpine (pyarrow has no musl wheel): the versions the laptop Spark image pins
  apk_add py3-pandas py3-numpy py3-pyarrow py3-setuptools py3-wheel py3-importlib-metadata
  python3 -m venv --system-site-packages /opt/spark-venv
  # Install from hash-pinned release files without querying the PyPI index.
  /opt/spark-venv/bin/pip install --no-index --no-deps --no-build-isolation --timeout 60 \
    -r "$REPO/spark/codespace-requirements.txt"
  ln -sfn "$(/opt/spark-venv/bin/python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" /opt/spark
  SPARK_HOME=/opt/spark LAB_SPARK_EXCLUDES="software.amazon.awssdk:bundle" \
  LAB_SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:$SPARK,io.delta:delta-spark_2.13:$DELTA,org.apache.hadoop:hadoop-aws:$HADOOP,software.amazon.awssdk:s3:$AWS_SDK,software.amazon.awssdk:s3-transfer-manager:$AWS_SDK,software.amazon.awssdk:sts:$AWS_SDK,software.amazon.awssdk:apache-client:$AWS_SDK" \
    /opt/spark-venv/bin/python "$REPO/spark/bake_jars.py" | awk '/^added/ {n++} END {print "  " n+0 " jars added"}'
  mkdir -p /opt/spark/conf
  ln -sf "$REPO/spark/conf/spark-defaults.conf" /opt/spark/conf/spark-defaults.conf
  echo "export PYSPARK_PYTHON=/opt/spark-venv/bin/python" > /opt/spark/conf/spark-env.sh
  touch "$installed"
}

install_postgres() {
  command -v pg_ctl >/dev/null && return
  say "Installing Postgres 16"; apk_add postgresql16 postgresql16-client
}

install_kafka_connect() {
  [ -d /opt/debezium/debezium-connector-postgres ] && return
  install_kafka; say "Installing Debezium $DEBEZIUM (Postgres connector)"
  mkdir -p /opt/debezium
  fetch "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/$DEBEZIUM/debezium-connector-postgres-$DEBEZIUM-plugin.tar.gz" /tmp/debezium.tgz
  tar -xzf /tmp/debezium.tgz -C /opt/debezium && rm /tmp/debezium.tgz
}

install_gh_tarball() {   # name url dir binary
  [ -x "$3/$4" ] && return
  say "Installing $1"
  fetch "$2" /tmp/$1.tgz || { echo "  WARNING: Failed to download $1, skipping"; return 1; }
  mkdir -p "$3" && tar -xzf /tmp/$1.tgz -C "$3" --strip-components=1 && rm /tmp/$1.tgz || return 1
}
install_prometheus() { install_gh_tarball prometheus "https://github.com/prometheus/prometheus/releases/download/v$PROMETHEUS/prometheus-$PROMETHEUS.linux-$GOARCH.tar.gz" /opt/prometheus prometheus; }
install_pushgateway() { install_gh_tarball pushgateway "https://github.com/prometheus/pushgateway/releases/download/v$PUSHGATEWAY/pushgateway-$PUSHGATEWAY.linux-$GOARCH.tar.gz" /opt/pushgateway pushgateway; }
install_kafka_exporter() { install_gh_tarball kafka-exporter "https://github.com/danielqsj/kafka_exporter/releases/download/v$KAFKA_EXPORTER/kafka_exporter-$KAFKA_EXPORTER.linux-$GOARCH.tar.gz" /opt/kafka-exporter kafka_exporter; }
install_grafana() { install_gh_tarball grafana "https://dl.grafana.com/oss/release/grafana-$GRAFANA.linux-$GOARCH-musl.tar.gz" /opt/grafana bin/grafana; }

install_service() {
  case $1 in
    kafka) install_kafka ;; schema-registry) install_schema_registry ;; kafka-ui) install_kafka_ui ;;
    minio) install_minio ;; spark-master|spark-worker) install_spark ;;
    postgres) install_postgres ;; kafka-connect) install_kafka_connect ;;
    prometheus) install_prometheus ;; pushgateway) install_pushgateway ;;
    kafka-exporter) install_kafka_exporter ;; grafana) install_grafana ;; prefect-server) : ;;
  esac
}

# ---------- processes ----------
alive() { case $(ps -o stat= -p "$1" 2>/dev/null) in ''|Z*) return 1 ;; esac; }   # a zombie (exited, not reaped) is not alive
pid_of() { [ -f "$RUN/$1.pid" ] && alive "$(cat "$RUN/$1.pid")" && cat "$RUN/$1.pid"; }
running() { pid_of "$1" >/dev/null; }

launch() {   # service command...: background process in its own session, log in $LOGS/<service>.log
  local name=$1; shift
  running "$name" && return
  ( cd "$REPO" && exec setsid "$@" >"$LOGS/$name.log" 2>&1 ) &
  echo $! >"$RUN/$name.pid"
}

wait_for() {   # service check-command...: up to 3 minutes
  local name=$1; shift
  for _ in $(seq 90); do "$@" >/dev/null 2>&1 && { echo "  ok   $name"; return; }; running "$name" || break; sleep 2; done
  echo "  FAIL $name: see scripts/codespace.sh logs $name" >&2; tail -n 20 "$LOGS/$name.log" >&2 || true; return 1
}
http_ok() { curl -fsS -o /dev/null --max-time 3 "$1"; }

start_service() {
  local s=$1
  running "$s" && { echo "  ok   $s (already running)"; return; }
  case $s in
    kafka)
      mkdir -p "$DATA/kafka"
      cat >"$DATA/kafka.properties" <<EOF
node.id=1
process.roles=broker,controller
listeners=INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094
advertised.listeners=INTERNAL://kafka:9092,EXTERNAL://localhost:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
controller.quorum.voters=1@kafka:9093
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.dirs=$DATA/kafka
EOF
      [ -f "$DATA/kafka/meta.properties" ] || /opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c "$DATA/kafka.properties" >/dev/null
      launch kafka env KAFKA_HEAP_OPTS="-Xms256m -Xmx768m" /opt/kafka/bin/kafka-server-start.sh "$DATA/kafka.properties"
      wait_for kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092
      for t in airport.transactions airport.fraud-alerts; do
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1 >/dev/null
      done ;;
    schema-registry)
      printf 'listeners=http://0.0.0.0:8081\nhost.name=schema-registry\nkafkastore.bootstrap.servers=PLAINTEXT://kafka:9092\n' >"$DATA/schema-registry.properties"
      launch schema-registry env SCHEMA_REGISTRY_HEAP_OPTS="-Xms128m -Xmx256m" LOG_DIR="$LOGS/schema-registry" /opt/confluent/bin/schema-registry-start "$DATA/schema-registry.properties"
      wait_for schema-registry http_ok http://localhost:8081/subjects ;;
    kafka-ui)
      launch kafka-ui env SERVER_PORT=8080 KAFKA_CLUSTERS_0_NAME=airport-lab KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092 \
        KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://schema-registry:8081 /opt/kafka-ui/jre/bin/java -Xmx384m -jar /opt/kafka-ui/kafka-ui.jar
      wait_for kafka-ui http_ok http://localhost:8080 ;;
    minio)
      mkdir -p "$DATA/minio"
      launch minio env MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server "$DATA/minio" --address :9000 --console-address :9001
      wait_for minio http_ok http://localhost:9000/minio/health/live
      mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null
      for b in bronze silver gold checkpoints quarantine; do mc mb --ignore-existing "local/$b" >/dev/null; done ;;
    spark-master)
      launch spark-master env SPARK_NO_DAEMONIZE=1 /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host spark-master --port 7077 --webui-port 8082
      wait_for spark-master http_ok http://localhost:8082 ;;
    spark-worker)
      launch spark-worker env PYSPARK_PYTHON=/opt/spark-venv/bin/python /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --host spark-worker --cores 1 --memory 1024m --webui-port 8091 spark://spark-master:7077
      wait_for spark-worker sh -c "curl -fsS http://localhost:8082/json/ | grep -q '\"state\" : \"ALIVE\"'" ;;
    postgres)
      if [ ! -f "$DATA/postgres/PG_VERSION" ]; then
        mkdir -p "$DATA/postgres" && chown postgres:postgres "$DATA/postgres"
        su postgres -s /bin/sh -c "initdb -D $DATA/postgres -U postgres --auth=trust >/dev/null"
      fi
      mkdir -p /run/postgresql && chown postgres:postgres /run/postgresql
      launch postgres su postgres -s /bin/sh -c "exec postgres -D $DATA/postgres -c listen_addresses='*' -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10"
      wait_for postgres pg_isready -h localhost -U postgres
      psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='airport'" | grep -q 1 || psql -U postgres -qc "CREATE DATABASE airport"
      psql -U postgres -qc "ALTER USER postgres PASSWORD 'postgres'" ;;
    kafka-connect)
      cat >"$DATA/connect.properties" <<EOF
bootstrap.servers=kafka:9092
group.id=1
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-statuses
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
listeners=http://0.0.0.0:8083
rest.advertised.host.name=kafka-connect
plugin.path=/opt/debezium
plugin.discovery=service_load
EOF
      launch kafka-connect env KAFKA_HEAP_OPTS="-Xms256m -Xmx640m" LOG_DIR="$LOGS/kafka-connect" /opt/kafka/bin/connect-distributed.sh "$DATA/connect.properties"
      wait_for kafka-connect http_ok http://localhost:8083/connectors ;;
    prometheus)
      mkdir -p "$DATA/prometheus"
      launch prometheus /opt/prometheus/prometheus --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path="$DATA/prometheus" --storage.tsdb.retention.time=2d
      wait_for prometheus http_ok http://localhost:9090/-/ready ;;
    pushgateway)
      launch pushgateway /opt/pushgateway/pushgateway
      wait_for pushgateway http_ok http://localhost:9091/-/ready ;;
    kafka-exporter)
      launch kafka-exporter /opt/kafka-exporter/kafka_exporter --kafka.server=kafka:9092
      wait_for kafka-exporter http_ok http://localhost:9308/metrics ;;
    grafana)
      mkdir -p "$DATA/grafana"
      launch grafana env GF_SECURITY_ADMIN_USER=admin GF_SECURITY_ADMIN_PASSWORD=admin GF_AUTH_ANONYMOUS_ENABLED=true GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
        GF_PATHS_DATA="$DATA/grafana" GF_PATHS_LOGS="$LOGS" GF_PATHS_PROVISIONING="$REPO/monitoring/grafana/provisioning" \
        /opt/grafana/bin/grafana server --homepath /opt/grafana
      wait_for grafana http_ok http://localhost:3000/api/health ;;
    prefect-server)
      launch prefect-server env PREFECT_HOME="$DATA/prefect" PREFECT_SERVER_ANALYTICS_ENABLED=false \
        uv run --locked prefect server start --host 0.0.0.0 --port 4200
      wait_for prefect-server http_ok http://localhost:4200/api/health ;;
  esac
}

stop_service() {
  local pid; pid=$(pid_of "$1") || return 0
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 30); do alive "$pid" || break; sleep 1; done
  kill -KILL -- "-$pid" 2>/dev/null || true
  rm -f "$RUN/$1.pid"; echo "  stopped $1"
}

prepare() {   # host names, paths and directories the lab commands expect
  mkdir -p "$DATA" "$LOGS" "$RUN"
  grep -q ' kafka-ui ' /etc/hosts 2>/dev/null || echo "127.0.0.1 $HOSTS " >>/etc/hosts
  ln -sfn "$REPO/spark/jobs" /opt/spark-apps
  ln -sfn "$REPO/data" /opt/spark-data
  ln -sfn "$REPO/data" /data
  ln -sfn "$REPO/monitoring/prometheus" /etc/prometheus
}

services_of() {
  case ${1:-core} in
    core) echo "$CORE" ;; cdc) echo "$CORE $CDC" ;; monitoring) echo "$CORE $MONITORING" ;;
    orchestration) echo "$CORE $ORCHESTRATION" ;; all|'*') echo "$ALL" ;;
    *) echo "unknown stack '$1': core, cdc, monitoring, orchestration or all" >&2; exit 2 ;;
  esac
}

cmd=${1:-}; shift || true
case $cmd in
  up)
    prepare
    services=$(services_of "${1:-core}")
    for s in $services; do install_service "$s"; done
    say "Starting: $services"
    for s in $services; do start_service "$s"; done
    say "Ready. scripts/services.sh urls lists the UIs." ;;
  down)  for s in $(echo "$ALL" | tr ' ' '\n' | tac); do stop_service "$s"; done ;;
  clean) "$0" down; rm -rf "$DATA" "$LOGS"; echo "  deleted service data" ;;
  status) for s in $ALL; do running "$s" && echo "$s"; done; true ;;
  logs)  tail -n "${LINES:-100}" "$LOGS/${1:?service}.log" ;;
  restart) prepare; stop_service "${1:?service}"; start_service "$1" ;;
  stop)  stop_service "${1:?service}" ;;
  start) prepare; start_service "${1:?service}" ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
