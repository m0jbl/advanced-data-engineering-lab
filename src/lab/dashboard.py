"""Run the lab dashboard's fixed actions and print evidence from the live stack."""

import argparse
import csv
import json
import os
import runpy
import shlex
import subprocess
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Thread
from typing import Callable
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
SPARK = "/opt/spark/bin/spark-submit"
TRANSACTIONS = "airport.transactions"
ALERTS = "airport.fraud-alerts"
CDC_TOPIC = "fraud.public.passenger_blocklist"
CONNECTOR = "http://localhost:8083/connectors/blocklist-connector"
PREFECT_API = "http://localhost:4200/api"

STAGES = (
    ("setup", "Set up the stack", "Start the core services and verify that Spark has a worker and Kafka has the lab topics.", ("01",)),
    ("ingest", "Land the payments", "Generate airport payments, inspect the CSVs, and land inputs in MinIO bronze.", ("02", "03")),
    ("lakehouse", "Build the lakehouse", "Validate payments, build point-in-time features, and evaluate the fraud rules.", ("04", "05", "06")),
    ("stream", "Score live payments", "Run the streaming scorer and inspect bounded evidence from Kafka and MinIO.", ("07", "09")),
    ("operate", "Operate the pipeline", "Apply passenger CDC, check monitoring, and run the Prefect quality gate.", ("08", "10", "11")),
    ("serve", "Serve governed data", "Publish the analyst-safe Delta dataset and review the data contract.", ("12",)),
)


def emit(title, value):
    print(f"\n{title}\n{json.dumps(value, indent=2)}", flush=True)


def child_env(**extra):
    return {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONUNBUFFERED": "1", **extra}


def run(argv, *, env=None, stdin=None, timeout=None):
    print(f"\n$ {shlex.join(argv)}", flush=True)
    subprocess.run(argv, cwd=ROOT, env=env, stdin=stdin, timeout=timeout, check=True)


def fetch(url, *, body=None, method=None):
    request = Request(url, data=None if body is None else json.dumps(body).encode(),
                      headers={} if body is None else {"Content-Type": "application/json"}, method=method)
    with urlopen(request, timeout=8) as response:
        return response.read()


def get_json(url, **kwargs):
    return json.loads(fetch(url, **kwargs))


def inspect_files():
    columns = {
        "transactions.csv": ("event_timestamp", "airport", "merchant_category", "amount", "card_present"),
        "passengers.csv": ("account_age_days",),
        "labels.csv": (),
    }
    for name, sample_columns in columns.items():
        path = ROOT / "data" / "raw" / name
        with path.open(newline="", encoding="utf-8") as source:
            rows = csv.DictReader(source)
            if not rows.fieldnames or not set(sample_columns).issubset(rows.fieldnames):
                raise ValueError(f"Unexpected CSV header in {path.relative_to(ROOT)}")
            sample = [{key: row[key] for key in sample_columns} for _, row in zip(range(3), rows)] if sample_columns else []
            emit(str(path.relative_to(ROOT)), {
                "rows": len(sample) + sum(1 for _ in rows), "columns": rows.fieldnames, "sample": sample,
            })


def kafka_consumer():
    from confluent_kafka import Consumer

    # Assign partitions explicitly. Never join a lab group, commit offsets or auto-create a topic.
    return closing(Consumer({"bootstrap.servers": "localhost:9094", "group.id": "dashboard-read-only",
                             "enable.auto.commit": False, "enable.auto.offset.store": False,
                             "allow.auto.create.topics": False, "socket.timeout.ms": 5000}))


def watermarks(consumer, topic):
    from confluent_kafka import TopicPartition

    info = consumer.list_topics(timeout=6).topics.get(topic)
    if info is None or info.error:
        raise RuntimeError(f"Kafka topic {topic} is unavailable" + (f": {info.error}" if info else ""))
    return {partition: consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=4)
            for partition in sorted(info.partitions)}


def read_kafka(topic, sample_size=2):
    from confluent_kafka import KafkaException, TopicPartition

    with kafka_consumer() as consumer:
        offsets = watermarks(consumer, topic)
        consumer.assign([TopicPartition(topic, part, max(low, high - sample_size))
                         for part, (low, high) in offsets.items() if high > low])
        samples = []
        remaining = {part for part, (low, high) in offsets.items() if high > low}
        deadline = time.monotonic() + 8
        while remaining and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            part, offset = message.partition(), message.offset()
            high = offsets[part][1]
            if part not in remaining:
                continue
            if offset < high:
                value = message.value()
                try:
                    value = json.loads(value) if value is not None else None
                except ValueError:
                    value = value.decode(errors="replace")[:1000]
                samples.append({"partition": part, "offset": offset, "value": value})
            if offset >= high - 1:
                remaining.remove(part)
        emit(topic, {"offsets": [{"partition": part, "first": low, "next": high}
                                 for part, (low, high) in offsets.items()],
                     "sample": sorted(samples, key=lambda item: (item["partition"], item["offset"])),
                     "sample_complete": not remaining})
        if remaining:
            raise RuntimeError(f"Timed out reading the bounded sample for {topic}: partitions {sorted(remaining)}")


def stream_worker(work):
    """Run inside spark-submit; the shell sends a drain signal after the producer flushes."""
    # The wrapper file is temporary; keep both imports and the entry point on the mounted lab path.
    sys.path.insert(0, "/opt/spark-apps")
    from common import session

    spark = session("stream_fraud")
    finished = Event()
    failures = []

    def supervise():
        try:
            while not finished.is_set():
                queries = spark.streams.active
                if len(queries) == 3:
                    break
                finished.wait(0.2)
            else:
                return

            # .start() only starts the query thread. Drain once to resolve startingOffsets=latest
            # (or recover the prior checkpoint) before allowing the host to send payments.
            baseline = {}
            for query in queries:
                query.processAllAvailable()
                baseline[query.id] = max((p.batchId for p in query.recentProgress if p.numInputRows), default=-1)
            print("LAB_DASHBOARD_STREAM_READY", flush=True)

            while not (work / "drain").exists():
                if finished.wait(0.2):
                    raise RuntimeError("Streaming scorer terminated before the producer finished")
            results = []
            for query in queries:
                query.processAllAvailable()
                batches = [p for p in query.recentProgress if p.batchId > baseline[query.id] and p.numInputRows]
                if not query.isActive or not batches:
                    raise RuntimeError(f"Streaming query {query.id} did not process new Kafka rows")
                output_rows = [p.sink.numOutputRows for p in batches]
                results.append({"sink": batches[-1].sink.description,
                                "batches": [p.batchId for p in batches],
                                "input_rows": sum(p.numInputRows for p in batches),
                                "output_rows": sum(output_rows) if all(n >= 0 for n in output_rows) else None})
            print("Stream progress: " + json.dumps(results), flush=True)
        except Exception as exc:
            failures.append(exc)
        finally:
            for query in spark.streams.active:
                try:
                    query.stop()
                except Exception as exc:
                    failures.append(exc)

    worker = Thread(target=supervise)
    worker.start()
    try:
        runpy.run_path("/opt/spark-apps/stream_fraud.py", run_name="__main__")
    finally:
        finished.set()
        worker.join()
        spark.stop()
    if failures:
        raise failures[0]


# Compose exec does not forward host signals to the in-container command. Its stdin instead
# stays open for the lifetime of the action; EOF requests cancellation of only this Spark app.
# The local deadline is a second bound even if the caller disappears or Spark hangs.
STREAM_SHELL = r"""
set -eu
work=$(mktemp -d /tmp/lab-dashboard-stream.XXXXXXXX)
watcher=
trap 'if [[ -n $watcher ]]; then kill "$watcher" 2>/dev/null || true; fi; rm -rf -- "$work"' EXIT
printf '%s\n' "$1" > "$work/stream_driver.py"
exec 3<&0
timeout -k 15 540 /opt/spark/bin/spark-submit --conf spark.sql.streaming.stopTimeout=15s \
    "$work/stream_driver.py" "$work" &
driver=$!
trap 'kill -TERM "$driver" 2>/dev/null || true; wait "$driver" || true; exit 143' TERM INT
(
    if IFS= read -r command && [[ $command == drain ]]; then
        : > "$work/drain"
        IFS= read -r ignored || true
    fi
    kill -TERM "$driver" 2>/dev/null || true
) <&3 &
watcher=$!
wait "$driver"
"""


def stream_source():
    # The host uv environment deliberately has no PySpark. Send only the fixed worker to Spark.
    from inspect import getsource

    return ("import json, runpy, sys\nfrom pathlib import Path\nfrom threading import Event, Thread\n"
            + getsource(stream_worker) + "\nstream_worker(Path(sys.argv[1]))\n")


@dataclass(frozen=True)
class Action:
    id: str
    stage: str
    title: str
    detail: str
    kind: str
    command: str
    execute: Callable[[], None]

    def public(self):
        return {field: getattr(self, field) for field in ("id", "stage", "title", "detail", "kind", "command")}


class Runtime:
    def __init__(self, mode):
        self.mode = mode
        self.compose = mode == "compose"

    def service(self, service, *argv):
        if not self.compose:
            return argv
        profile = ("--profile", "cdc") if service == "postgres" else ()
        return ("docker", "compose", *profile, "exec", "-T", service, *argv)

    def start(self, profile=None):
        args = (profile,) if profile else ()
        if not self.compose:
            return ("scripts/codespace.sh", "up", *args)
        return ("docker", "compose", *(('--profile', *args) if args else ()), "up", "-d", "--wait")

    def spark(self, job, *args):
        return self.service("spark-master", SPARK, f"/opt/spark-apps/{job}.py", *args)

    def kafka(self, cli, *args):
        return self.service("kafka", f"/opt/kafka/bin/kafka-{cli}.sh", "--bootstrap-server", "kafka:9092", *args)

    def mc(self, *args):
        return self.service("minio-client", "mc", *args)

    def psql(self, *args):
        return self.service("postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "airport", *args)

    def listing(self, *prefixes):
        for prefix in prefixes:
            run(self.mc("ls", f"local/{prefix}/"), timeout=15)

    def check_services(self):
        status = ("docker", "compose", "--profile", "*", "ps", "--all") if self.compose else ("scripts/codespace.sh", "status")
        run(status, timeout=20)
        for topic in (TRANSACTIONS, ALERTS):
            run(self.kafka("topics", "--describe", "--topic", topic), timeout=20)
        fetch("http://localhost:9000/minio/health/live")
        print("MinIO health: OK", flush=True)
        emit("Schema Registry subjects", get_json("http://localhost:8081/subjects"))
        spark = get_json("http://localhost:8082/json/")
        workers = [{key: worker.get(key) for key in ("id", "host", "state", "cores", "memory")} for worker in spark["workers"]]
        emit("Spark master", {"status": spark.get("status"), "workers": workers,
                              "active_apps": [app.get("name") for app in spark.get("activeapps", [])],
                              "recent_apps": [app.get("name") for app in spark.get("completedapps", [])[:5]]})
        if not any(worker["state"] == "ALIVE" for worker in workers):
            raise RuntimeError("Spark does not have a live worker")

    def upload_bronze(self):
        for name in ("transactions", "passengers"):
            run(self.mc("cp", f"/data/raw/{name}.csv", f"local/bronze/{name}/{name}.csv"))
        self.inspect_bronze()

    def inspect_bronze(self):
        run(self.mc("ls", "--recursive", "local/bronze/transactions/"), timeout=15)
        run(self.mc("ls", "--recursive", "local/bronze/passengers/"), timeout=15)

    def score(self):
        run(self.spark("score_batch"), env=child_env(PUSHGATEWAY_URL="http://localhost:9091"))

    def evaluation_copy(self):
        src = "/tmp/airport-evaluation.json"
        return ("docker", "compose", "cp", f"spark-master:{src}") if self.compose else ("cp", src)

    def evaluate(self):
        run(self.spark("evaluate"))
        (ROOT / "data" / "reports").mkdir(parents=True, exist_ok=True)
        run((*self.evaluation_copy(), "data/reports/evaluation.json"))

    def run_stream(self):
        argv = self.service("spark-master", "bash", "-c", STREAM_SHELL, "dashboard-stream", stream_source())
        print("Starting the Spark streaming scorer; waiting for all three queries to resolve Kafka offsets.", flush=True)
        with subprocess.Popen(argv, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, errors="replace", bufsize=1, start_new_session=True) as driver:
            ready = Event()

            def forward_output():
                for line in driver.stdout:
                    print(line, end="", flush=True)
                    if line.rstrip() == "LAB_DASHBOARD_STREAM_READY":
                        ready.set()

            def require_success(code):
                if code:
                    raise subprocess.CalledProcessError(code, "Spark streaming scorer")

            logs = Thread(target=forward_output, daemon=True)
            logs.start()
            try:
                deadline = time.monotonic() + 330
                while not ready.wait(0.25):
                    code = driver.poll()
                    if code is not None:
                        require_success(code)
                        raise RuntimeError("Spark exited before its streaming queries were ready")
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Spark streaming startup exceeded 330 seconds")
                run((sys.executable, "-m", "lab.producer", "--seconds", "20"),
                    env=child_env(KAFKA_BOOTSTRAP_SERVERS="localhost:9094", TRANSACTIONS_PER_SECOND="5"), timeout=45)
                if driver.poll() is not None:
                    require_success(driver.returncode)
                    raise RuntimeError("Spark exited before the producer finished")
                print("Producer finished; draining and stopping the streaming queries.", flush=True)
                driver.stdin.write("drain\n")
                driver.stdin.flush()
                require_success(driver.wait(timeout=160))
            finally:
                try:
                    driver.stdin.close()
                except BrokenPipeError:
                    pass
                try:
                    driver.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    driver.kill()
                    driver.wait()
                logs.join(timeout=2)

    def inspect_kafka(self):
        for topic in (TRANSACTIONS, ALERTS):
            read_kafka(topic)
        run(self.kafka("consumer-groups", "--all-groups", "--describe"), timeout=20)

    def configure_cdc(self):
        run(self.kafka("topics", "--create", "--if-not-exists", "--topic", CDC_TOPIC,
                       "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1"), timeout=20)
        with (ROOT / "cdc" / "blocklist.sql").open() as sql:
            run(self.psql("-f", "-"), stdin=sql, timeout=20)
        config = json.loads((ROOT / "cdc" / "blocklist-connector.json").read_text())["config"]
        fetch(f"{CONNECTOR}/config", body=config, method="PUT")
        deadline = time.monotonic() + 60
        while True:
            try:
                status = get_json(f"{CONNECTOR}/status")
            except HTTPError as exc:
                # Connect can acknowledge the config before publishing its first status.
                if exc.code != 404 or time.monotonic() >= deadline:
                    raise
                time.sleep(2)
                continue
            states = [status["connector"]["state"], *(task["state"] for task in status["tasks"])]
            if len(states) > 1 and all(state == "RUNNING" for state in states):
                emit("Debezium connector ready", status)
                return
            if "FAILED" in states or time.monotonic() >= deadline:
                emit("Debezium connector status", status)
                raise RuntimeError("The blocklist connector and its task did not reach RUNNING")
            time.sleep(2)

    def change_demo_block(self, block):
        from confluent_kafka import KafkaException, TopicPartition

        with kafka_consumer() as consumer:
            offsets = watermarks(consumer, CDC_TOPIC)
            consumer.assign([TopicPartition(CDC_TOPIC, part, high) for part, (_, high) in offsets.items()])
            stamp = time.time_ns() // 1_000_000
            # Do not replace or unblock a passenger that another lab session owns.
            if block:
                sql = f"""INSERT INTO passenger_blocklist VALUES (42, 'dashboard_lab_demo', {stamp}, NULL)
                    ON CONFLICT (passenger_id) DO UPDATE SET blocked_at=EXCLUDED.blocked_at, unblocked_at=NULL
                    WHERE passenger_blocklist.reason=EXCLUDED.reason RETURNING blocked_at"""
                field = "blocked_at"
            else:
                sql = f"""UPDATE passenger_blocklist SET unblocked_at={stamp}
                    WHERE passenger_id=42 AND reason='dashboard_lab_demo' AND unblocked_at IS NULL
                    RETURNING unblocked_at"""
                field = "unblocked_at"
            print(f"Writing the dashboard {'block' if block else 'unblock'} example for passenger 42.", flush=True)
            result = subprocess.run(self.psql("-At", "-c", sql), cwd=ROOT, timeout=20, check=True,
                                    text=True, stdout=subprocess.PIPE)
            if str(stamp) not in result.stdout.splitlines():
                raise RuntimeError("No change: passenger 42 has no active dashboard demo row or is owned by another lab step")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                event = json.loads(message.value()) if message.value() else None
                if event and event.get("passenger_id") == 42 and event.get(field) == stamp:
                    emit("Observed Debezium event", {"partition": message.partition(), "offset": message.offset(), "value": event})
                    break
            else:
                raise RuntimeError("The Postgres update succeeded, but its Debezium event did not reach Kafka within 60 seconds")
        run(self.spark("blocklist_from_cdc"))

    def inspect_cdc(self):
        status = get_json(f"{CONNECTOR}/status")
        emit("Debezium connector", status)
        run(self.psql("-c", "SELECT passenger_id, reason, blocked_at, unblocked_at FROM passenger_blocklist "
                           "WHERE passenger_id IN (7, 42) ORDER BY passenger_id"), timeout=20)
        read_kafka(CDC_TOPIC)
        if status["connector"]["state"] != "RUNNING" or not status["tasks"] or any(
                task["state"] != "RUNNING" for task in status["tasks"]):
            raise RuntimeError("Debezium connector or task is not running")

    def run_prefect(self):
        os.environ.update(PREFECT_API_URL=PREFECT_API, PREFECT_LOGGING_COLORS="false",
                          PUSHGATEWAY_URL="http://localhost:9091")
        from flows import pipeline

        # The standalone flow detects Docker via PATH. Use the dashboard's explicit mode instead.
        pipeline.SPARK_SUBMIT = list(self.service("spark-master", SPARK))
        pipeline.MC = list(self.mc())
        pipeline.EVALUATION_COPY = [*self.evaluation_copy(), "data/reports/evaluation.json"]
        emit("Prefect quality gate passed", pipeline.payments_daily())

    def actions(self):
        def action(id, stage, title, detail, kind, execute, command=None):
            cli = f"PYTHONPATH=src uv run --locked python -m lab.dashboard run {id}"
            return Action(id, stage, title, detail, kind, command or cli, execute)

        def command(id, stage, title, detail, argv, *, kind="run", env=None):
            display = shlex.join(argv) if env is None else None
            return action(id, stage, title, detail, kind, lambda: run(argv, env=env), display)

        return (
            command("start-core", "setup", "Start core services", "Start Kafka, Schema Registry, MinIO and Spark; the first run may install or pull them.", self.start()),
            action("check-services", "setup", "Check services", "Read service status, Kafka partitions, Schema Registry subjects, MinIO health and Spark worker state.", "check", self.check_services),
            command("generate-data", "ingest", "Generate payment files", "Generate the historical transactions and passenger CSVs. Ground truth stays in a separate labels file.", ("uv", "run", "--locked", "python", "-m", "lab.generate"), env=child_env()),
            action("inspect-files", "ingest", "Inspect generated CSVs", "Count data rows and show a small sample without passenger IDs or ground-truth labels.", "check", inspect_files),
            action("upload-bronze", "ingest", "Upload to bronze", "Copy transactions and passengers to the two MinIO bronze prefixes. Do not upload labels.", "run", self.upload_bronze),
            action("inspect-bronze", "ingest", "List bronze objects", "Show the uploaded objects, sizes and timestamps in both bronze prefixes.", "check", self.inspect_bronze),
            command("build-silver", "lakehouse", "Validate and write silver", "Read all bronze days; report accepted and quarantined rows with reject reasons.", self.spark("silver_transactions")),
            command("build-features", "lakehouse", "Build 30-day features", "Build passenger snapshots from history strictly before each scoring date.", self.spark("features_30d")),
            action("score-batch", "lakehouse", "Score the latest day", "Write gold fraud scores, report payment and alert counts, and push metrics if monitoring is running.", "run", self.score),
            action("evaluate", "lakehouse", "Evaluate against labels", "Report precision and recall for the latest scored day, and save the JSON locally.", "run", self.evaluate),
            action("inspect-lakehouse", "lakehouse", "List Delta layer prefixes", "Inspect silver, quarantine, features, and batch-score object paths.", "check", lambda: self.listing("silver/transactions", "quarantine/transactions", "gold/passenger_features", "gold/fraud_scores")),
            action("inspect-evaluation", "lakehouse", "Read the evaluation report", "Read the saved JSON with payment and fraud counts, precision and recall.", "check", inspect_evaluation),
            command("delta-history", "lakehouse", "Read feature table history", "Show recent Delta versions, write types and operation parameters.", self.spark("history"), kind="check"),
            action("run-stream", "stream", "Run a 20-second live stream", "Wait for Spark queries, run the producer with the current lab scenario, then drain and stop the scorer.", "run", self.run_stream),
            action("inspect-kafka", "stream", "Inspect Kafka offsets and messages", "Read up to two recent messages per partition plus current group lag without committing offsets.", "check", self.inspect_kafka),
            action("inspect-stream", "stream", "List streaming outputs", "Find Delta stream scores, quarantine objects and the durable checkpoints in MinIO.", "check", lambda: self.listing("gold/stream_scores", "quarantine/stream_transactions", "checkpoints")),
            command("start-cdc", "operate", "Start CDC services", "Start the Postgres and Kafka Connect profile alongside the core stack.", self.start("cdc")),
            action("configure-cdc", "operate", "Configure Debezium", "Create the course CDC topic and blocklist table, and wait for the connector task to run.", "run", self.configure_cdc),
            action("block-demo", "operate", "Block demo passenger 42", "Write only the dashboard demo entry, confirm its event in Kafka and merge the active Delta blocklist.", "run", lambda: self.change_demo_block(True)),
            action("inspect-cdc", "operate", "Inspect Debezium and Kafka", "Read connector and task status, scoped source rows, CDC offsets and recent events.", "check", self.inspect_cdc),
            action("unblock-demo", "operate", "Unblock demo passenger 42", "Unblock only the dashboard entry, confirm its Kafka event and merge the change into Delta.", "run", lambda: self.change_demo_block(False)),
            command("merge-cdc", "operate", "Merge available CDC events", "Apply any existing Kafka blocklist events to Delta, including changes made outside this panel.", self.spark("blocklist_from_cdc")),
            command("inspect-cdc-changes", "operate", "Read the Delta change feed", "Inspect recorded inserts, updates and unblocks with commit versions and counts.", self.spark("blocklist_changes"), kind="check"),
            command("start-monitoring", "operate", "Start monitoring services", "Start Prometheus, Pushgateway, Kafka exporter and Grafana. Rerun batch scoring to push metrics.", self.start("monitoring")),
            action("inspect-monitoring", "operate", "Read live metrics and alert rules", "Inspect scrape targets, current batch metrics, Kafka lag and the alert rule states.", "check", inspect_monitoring),
            command("start-prefect", "operate", "Start Prefect", "Start the Prefect API and UI alongside the core stack.", self.start("orchestration")),
            action("run-prefect", "operate", "Run the daily Prefect flow", "Run the actual batch flow once. A failed precision gate returns a failed action.", "run", self.run_prefect),
            action("inspect-prefect", "operate", "Read recent Prefect runs", "Read recent server-side flow run states and timestamps.", "check", inspect_prefect),
            command("mask-payments", "serve", "Publish masked payments", "Remove direct identifiers, pseudonymise passenger IDs and write the analyst-safe Delta table.", self.spark("mask_payments")),
            action("inspect-governance", "serve", "Inspect the contract and masked output", "Read ownership, classification and retention rules, then list the published MinIO prefix.", "check", self.inspect_governance),
        )

    def inspect_governance(self):
        print((ROOT / "contracts" / "airport-payments.yaml").read_text(), end="", flush=True)
        self.listing("gold/transactions_masked")


def inspect_evaluation():
    report = ROOT / "data" / "reports" / "evaluation.json"
    emit(str(report.relative_to(ROOT)), json.loads(report.read_text()))


def inspect_monitoring():
    api = "http://localhost:9090/api/v1"
    targets = get_json(f"{api}/targets")["data"]["activeTargets"]
    emit("Prometheus targets", [{key: target.get(key) for key in ("scrapePool", "health", "lastScrape", "lastError")}
                                for target in targets])
    groups = get_json(f"{api}/rules")["data"]["groups"]
    emit("Prometheus rules", [{key: rule.get(key) for key in ("name", "health", "state", "lastError")}
                            for group in groups for rule in group["rules"]])
    for query in ("fraud_batch_payments", "fraud_batch_alerts", "sum(kafka_consumergroup_lag)",
                  'sum by (topic) (kafka_topic_partition_current_offset{topic=~"airport.transactions|airport.fraud-alerts"})'):
        series = get_json(f"{api}/query?{urlencode({'query': query})}")["data"]["result"]
        emit(query, series)
    emit("Grafana health", get_json("http://localhost:3000/api/health"))
    if not targets or any(target["health"] == "down" for target in targets):
        raise RuntimeError("Prometheus has no targets or reports an unhealthy scrape target")


def inspect_prefect():
    fetch(f"{PREFECT_API}/health")
    # Prefect's filter endpoint is a read-only query despite its HTTP method.
    rows = get_json(f"{PREFECT_API}/flow_runs/filter", body={"limit": 5, "sort": "START_TIME_DESC"}, method="POST")
    emit("Prefect flow runs", [{key: row.get(key) for key in ("id", "name", "state_type", "state_name", "start_time", "end_time")}
                              | {"message": (row.get("state") or {}).get("message")} for row in rows])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("catalog", help="print the stable, mode-specific action catalog")
    run_parser = commands.add_parser("run", help="execute one action from the fixed catalog")
    run_parser.add_argument("id")
    args = parser.parse_args()
    mode = os.environ.get("LAB_DASHBOARD_MODE", "codespace" if os.environ.get("CODESPACES") == "true" else "compose")
    if mode not in ("codespace", "compose"):
        parser.error("LAB_DASHBOARD_MODE must be 'codespace' or 'compose'")
    actions = Runtime(mode).actions()
    if args.command == "catalog":
        print(json.dumps({"mode": mode, "stages": [dict(id=id, title=title, summary=summary, labs=labs)
                                                   for id, title, summary, labs in STAGES],
                          "actions": [action.public() for action in actions]}))
        return 0

    selected = next((action for action in actions if action.id == args.id), None)
    if selected is None:
        run_parser.error(f"unknown action {args.id!r}")
    try:
        print(f"{selected.title} [{mode}]", flush=True)
        selected.execute()
        return 0
    except subprocess.CalledProcessError as exc:
        code = exc.returncode if exc.returncode > 0 else 128 - exc.returncode
        print(f"Action command failed (exit {code})", file=sys.stderr, flush=True)
        return code
    except subprocess.TimeoutExpired as exc:
        print(f"Action command timed out after {exc.timeout} seconds", file=sys.stderr, flush=True)
        return 124
    except KeyboardInterrupt:
        print("Action interrupted", file=sys.stderr, flush=True)
        return 130
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
