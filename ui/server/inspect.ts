import { execFile } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { promisify } from "node:util";
import type {
  EvaluationSnapshot,
  PipelineSnapshot,
  PipelineStage,
  ServiceProbe,
  StageId,
  StageStatus,
} from "../src/types/pipeline.ts";

const execFileAsync = promisify(execFile);
const REPO_ROOT = path.resolve(import.meta.dirname, "../..");

async function probeHttp(
  url: string,
  timeoutMs = 700,
): Promise<{ ok: boolean; status?: number; latencyMs: number; json?: unknown; text?: string }> {
  const start = performance.now();
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
    const latencyMs = Math.round(performance.now() - start);
    const ct = res.headers.get("content-type") || "";
    let json: unknown;
    let text: string | undefined;
    if (res.ok) {
      if (ct.includes("application/json")) {
        json = await res.json().catch(() => undefined);
      } else {
        text = await res.text().catch(() => undefined);
      }
    }
    return { ok: res.ok, status: res.status, latencyMs, json, text };
  } catch {
    return { ok: false, latencyMs: Math.round(performance.now() - start) };
  }
}

function probeTcp(host: string, port: number, timeoutMs = 600): Promise<{ ok: boolean; latencyMs: number }> {
  const start = performance.now();
  const { promise, resolve } = Promise.withResolvers<{ ok: boolean; latencyMs: number }>();
  const socket = net.createConnection({ host, port });
  const finish = (ok: boolean) => {
    socket.destroy();
    resolve({ ok, latencyMs: Math.round(performance.now() - start) });
  };
  socket.setTimeout(timeoutMs, () => finish(false));
  socket.on("connect", () => finish(true));
  socket.on("error", () => finish(false));
  return promise;
}

function countCsvRowsFast(filePath: string): { exists: boolean; rows: number; sizeMb: string; mtime?: string } {
  if (!fs.existsSync(filePath)) return { exists: false, rows: 0, sizeMb: "0.0" };
  const stat = fs.statSync(filePath);
  const buf = fs.readFileSync(filePath);
  let lines = 0;
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] === 10) lines++;
  }
  return {
    exists: true,
    rows: Math.max(0, lines - 1),
    sizeMb: (stat.size / (1024 * 1024)).toFixed(2),
    mtime: stat.mtime.toISOString(),
  };
}

function readScenario(): string {
  const p = path.join(REPO_ROOT, "control", "scenario");
  if (!fs.existsSync(p)) return "normal (default)";
  return fs.readFileSync(p, "utf8").trim() || "normal";
}

function readEvaluation(): EvaluationSnapshot | null {
  const p = path.join(REPO_ROOT, "data", "reports", "evaluation.json");
  if (!fs.existsSync(p)) return null;
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8")) as Record<string, unknown>;
    return {
      eventDate: String(raw.event_date ?? ""),
      payments: Number(raw.payments ?? 0),
      frauds: Number(raw.frauds ?? 0),
      alerts: Number(raw.alerts ?? 0),
      precision: Number(raw.precision ?? 0),
      recall: Number(raw.recall ?? 0),
    };
  } catch {
    return null;
  }
}

function readFraudRules(): { rules: { name: string; points: number }[]; threshold: number } {
  const p = path.join(REPO_ROOT, "spark", "jobs", "fraud_rules.py");
  if (!fs.existsSync(p)) return { rules: [], threshold: 50 };
  const src = fs.readFileSync(p, "utf8");
  const threshMatch = src.match(/ALERT_THRESHOLD\s*=\s*(\d+)/);
  const threshold = threshMatch ? Number(threshMatch[1]) : 50;
  const rules: { name: string; points: number }[] = [];
  const blockMatch = src.match(/RULE_POINTS\s*=\s*\{([\s\S]*?)\}/);
  if (blockMatch) {
    for (const m of blockMatch[1].matchAll(/"([A-Z0-9_]+)"\s*:\s*(\d+)/g)) {
      rules.push({ name: m[1], points: Number(m[2]) });
    }
  }
  return { rules, threshold };
}

function readPaymentSchema(): { required: string[]; airports: number; categories: number } {
  const p = path.join(REPO_ROOT, "schemas", "payment_event.json");
  if (!fs.existsSync(p)) return { required: [], airports: 0, categories: 0 };
  const schema = JSON.parse(fs.readFileSync(p, "utf8")) as {
    required?: string[];
    properties?: { airport?: { enum?: string[] }; merchant_category?: { enum?: string[] } };
  };
  return {
    required: schema.required ?? [],
    airports: schema.properties?.airport?.enum?.length ?? 0,
    categories: schema.properties?.merchant_category?.enum?.length ?? 0,
  };
}

function readContractSummary(): { dataset: string; qualityRules: string[]; bronzeDays: number; silverDays: number } {
  const p = path.join(REPO_ROOT, "contracts", "airport-payments.yaml");
  if (!fs.existsSync(p)) return { dataset: "airport.transactions", qualityRules: [], bronzeDays: 90, silverDays: 730 };
  const text = fs.readFileSync(p, "utf8");
  const qualityRules = [...text.matchAll(/rule:\s*([A-Z_]+)/g)].map((m) => m[1]);
  const bronze = Number(text.match(/bronze_days:\s*(\d+)/)?.[1] ?? 90);
  const silver = Number(text.match(/silver_days:\s*(\d+)/)?.[1] ?? 730);
  return { dataset: "airport.transactions", qualityRules, bronzeDays: bronze, silverDays: silver };
}

function readAlertRuleNames(): string[] {
  const p = path.join(REPO_ROOT, "monitoring", "prometheus", "alerts.yml");
  if (!fs.existsSync(p)) return [];
  const text = fs.readFileSync(p, "utf8");
  return [...text.matchAll(/-\s*alert:\s*(\S+)/g)].map((m) => m[1]);
}

async function probeDockerCompose(): Promise<{ dockerAvailable: boolean; runningServices: Set<string> }> {
  try {
    const { stdout } = await execFileAsync(
      "docker",
      ["compose", "--profile", "*", "ps", "--services", "--status", "running"],
      { cwd: REPO_ROOT, timeout: 1800 },
    );
    const services = new Set(
      stdout
        .split("\n")
        .map((s) => s.trim())
        .filter(Boolean),
    );
    return { dockerAvailable: true, runningServices: services };
  } catch {
    return { dockerAvailable: false, runningServices: new Set() };
  }
}

function stageStatusFromProbes(probes: ServiceProbe[], hasArtifacts: boolean): { status: StageStatus; label: string } {
  const liveProbes = probes.filter((p) => p.status !== "artifact-ready");
  const upCount = liveProbes.filter((p) => p.status === "up").length;
  if (liveProbes.length > 0 && upCount === liveProbes.length) {
    return { status: "healthy", label: "LIVE · HEALTHY" };
  }
  if (upCount > 0) {
    return { status: "partial", label: `PARTIAL · ${upCount}/${liveProbes.length} UP` };
  }
  if (hasArtifacts) {
    return { status: "ready", label: "READY · STACK STOPPED" };
  }
  return { status: "offline", label: "OFFLINE" };
}

export async function inspectPipeline(): Promise<PipelineSnapshot> {
  const [
    dockerInfo,
    kafkaTcp,
    kafkaUiHttp,
    schemaRegHttp,
    pgTcp,
    connectHttp,
    minioHttp,
    minioConsoleHttp,
    sparkMasterHttp,
    sparkAppHttp,
    prefectHttp,
    promHttp,
    promAlertsHttp,
    pushgwHttp,
    exporterHttp,
    grafanaHttp,
  ] = await Promise.all([
    probeDockerCompose(),
    probeTcp("127.0.0.1", 9094),
    probeHttp("http://127.0.0.1:8080"),
    probeHttp("http://127.0.0.1:8081/subjects"),
    probeTcp("127.0.0.1", 5432),
    probeHttp("http://127.0.0.1:8083/connectors"),
    probeHttp("http://127.0.0.1:9000/minio/health/live"),
    probeHttp("http://127.0.0.1:9001"),
    probeHttp("http://127.0.0.1:8082/json/"),
    probeHttp("http://127.0.0.1:4040/api/v1/applications"),
    probeHttp("http://127.0.0.1:4200/api/health"),
    probeHttp("http://127.0.0.1:9090/-/ready"),
    probeHttp("http://127.0.0.1:9090/api/v1/alerts"),
    probeHttp("http://127.0.0.1:9091/-/ready"),
    probeHttp("http://127.0.0.1:9308/metrics"),
    probeHttp("http://127.0.0.1:3000/api/health"),
  ]);

  const txCsv = countCsvRowsFast(path.join(REPO_ROOT, "data", "raw", "transactions.csv"));
  const paxCsv = countCsvRowsFast(path.join(REPO_ROOT, "data", "raw", "passengers.csv"));
  const lblCsv = countCsvRowsFast(path.join(REPO_ROOT, "data", "raw", "labels.csv"));
  const scenario = readScenario();
  const evaluation = readEvaluation();
  const { rules: fraudRules, threshold: alertThreshold } = readFraudRules();
  const schemaInfo = readPaymentSchema();
  const contractInfo = readContractSummary();
  const alertRuleNames = readAlertRuleNames();

  const subjects = Array.isArray(schemaRegHttp.json) ? schemaRegHttp.json.filter((s): s is string => typeof s === "string") : [];
  const connectors = Array.isArray(connectHttp.json) ? connectHttp.json.filter((c): c is string => typeof c === "string") : [];
  let sparkWorkers = 0;
  if (sparkMasterHttp.json && typeof sparkMasterHttp.json === "object" && "workers" in sparkMasterHttp.json) {
    const workers = sparkMasterHttp.json.workers;
    if (Array.isArray(workers)) {
      sparkWorkers = workers.filter(
        (w): w is { state: string } => Boolean(w && typeof w === "object" && "state" in w && w.state === "ALIVE"),
      ).length;
    }
  }
  const sparkApps = Array.isArray(sparkAppHttp.json) ? sparkAppHttp.json.length : 0;
  let activePromAlerts = 0;
  if (promAlertsHttp.json && typeof promAlertsHttp.json === "object" && "data" in promAlertsHttp.json) {
    const data = promAlertsHttp.json.data;
    if (data && typeof data === "object" && "alerts" in data && Array.isArray(data.alerts)) {
      activePromAlerts = data.alerts.length;
    }
  }

  // Stage 1: Sources & Ingestion
  const ingProbes: ServiceProbe[] = [
    {
      id: "batch-raw",
      name: "Batch Raw CSVs (Host)",
      profile: "host",
      status: txCsv.exists ? "artifact-ready" : "down",
      detail: txCsv.exists
        ? `${txCsv.rows.toLocaleString()} txns · ${paxCsv.rows.toLocaleString()} pax (${txCsv.sizeMb} MB)`
        : "Not generated yet (run lab.generate)",
    },
    {
      id: "kafka",
      name: "Kafka 4.4 KRaft",
      profile: "core",
      port: 9094,
      status: kafkaTcp.ok ? "up" : "down",
      latencyMs: kafkaTcp.latencyMs,
      detail: kafkaTcp.ok ? "Broker accepting EXTERNAL://localhost:9094" : "Container stopped",
    },
    {
      id: "kafka-ui",
      name: "Kafka UI (Kafbat)",
      profile: "core",
      port: 8080,
      url: "http://localhost:8080",
      status: kafkaUiHttp.ok ? "up" : "down",
      latencyMs: kafkaUiHttp.latencyMs,
      detail: kafkaUiHttp.ok ? "UI reachable at :8080" : "Container stopped",
    },
    {
      id: "debezium",
      name: "Postgres + Debezium CDC",
      profile: "cdc",
      port: 8083,
      url: "http://localhost:8083/connectors",
      status: pgTcp.ok && connectHttp.ok ? "up" : "down",
      latencyMs: connectHttp.latencyMs,
      detail: connectHttp.ok
        ? `Connectors: ${connectors.length ? connectors.join(", ") : "none registered"}`
        : "Optional --profile cdc stopped",
    },
  ];
  const ingState = stageStatusFromProbes(
    ingProbes.filter((p) => p.profile !== "cdc"),
    txCsv.exists,
  );

  // Stage 2: Schema & Data Contract
  const contractProbes: ServiceProbe[] = [
    {
      id: "payment-schema",
      name: "schemas/payment_event.json",
      profile: "repo",
      status: schemaInfo.required.length > 0 ? "artifact-ready" : "down",
      detail: `${schemaInfo.required.length} required fields · ${schemaInfo.airports} airports · ${schemaInfo.categories} categories`,
    },
    {
      id: "schema-registry",
      name: "Schema Registry 8.4",
      profile: "core",
      port: 8081,
      url: "http://localhost:8081/subjects",
      status: schemaRegHttp.ok ? "up" : "down",
      latencyMs: schemaRegHttp.latencyMs,
      detail: schemaRegHttp.ok
        ? `Subjects: ${subjects.length ? subjects.join(", ") : "0 registered yet"}`
        : "Container stopped",
    },
  ];
  const contractState = stageStatusFromProbes(contractProbes, schemaInfo.required.length > 0);

  // Stage 3: Lakehouse & Quality (Bronze -> Silver + Quarantine)
  const lakeProbes: ServiceProbe[] = [
    {
      id: "minio",
      name: "MinIO S3 Lakehouse",
      profile: "core",
      port: 9000,
      url: "http://localhost:9001",
      status: minioHttp.ok ? "up" : "down",
      latencyMs: minioHttp.latencyMs,
      detail: minioHttp.ok
        ? `Health live (${minioConsoleHttp.ok ? "console :9001 up" : "API :9000 up"})`
        : "Container stopped",
    },
    {
      id: "spark-master",
      name: "Spark 4.2 + Delta 4.4",
      profile: "core",
      port: 8082,
      url: "http://localhost:8082",
      status: sparkMasterHttp.ok ? "up" : "down",
      latencyMs: sparkMasterHttp.latencyMs,
      detail: sparkMasterHttp.ok
        ? `${sparkWorkers} worker(s) ALIVE · ${sparkApps} active app(s)`
        : "Container stopped",
    },
  ];
  const lakeState = stageStatusFromProbes(lakeProbes, contractInfo.qualityRules.length > 0);

  // Stage 4: Features, Scoring & CDC MERGE
  const scoreProbes: ServiceProbe[] = [
    {
      id: "spark-app",
      name: "Spark App / Stream UI",
      profile: "core",
      port: 4040,
      url: "http://localhost:4040",
      status: sparkAppHttp.ok ? "up" : "down",
      latencyMs: sparkAppHttp.latencyMs,
      detail: sparkAppHttp.ok ? `${sparkApps} running Spark job/stream` : "Idle (opens during spark-submit)",
    },
    {
      id: "evaluation-report",
      name: "data/reports/evaluation.json",
      profile: "host",
      status: evaluation ? "artifact-ready" : "down",
      detail: evaluation
        ? `${evaluation.eventDate}: ${evaluation.alerts}/${evaluation.payments} alerts · P=${(evaluation.precision * 100).toFixed(1)}% R=${(evaluation.recall * 100).toFixed(1)}%`
        : "No evaluation.json yet",
    },
  ];
  const scoreState = stageStatusFromProbes(
    sparkMasterHttp.ok ? [lakeProbes[1]] : scoreProbes.filter((p) => p.id === "spark-app"),
    Boolean(evaluation) || fraudRules.length > 0,
  );

  // Stage 5: Serve, Observe, Orchestrate & Govern
  const serveProbes: ServiceProbe[] = [
    {
      id: "prefect",
      name: "Prefect 3.9 Server",
      profile: "orchestration",
      port: 4200,
      url: "http://localhost:4200",
      status: prefectHttp.ok ? "up" : "down",
      latencyMs: prefectHttp.latencyMs,
      detail: prefectHttp.ok ? "API healthy at :4200" : "Optional --profile orchestration stopped",
    },
    {
      id: "prometheus",
      name: "Prometheus + Pushgateway",
      profile: "monitoring",
      port: 9090,
      url: "http://localhost:9090/alerts",
      status: promHttp.ok ? "up" : "down",
      latencyMs: promHttp.latencyMs,
      detail: promHttp.ok
        ? `Ready · ${activePromAlerts} active alerts · Pushgateway ${pushgwHttp.ok ? "up" : "down"}`
        : "Optional --profile monitoring stopped",
    },
    {
      id: "grafana",
      name: "Grafana 13.3 + Exporter",
      profile: "monitoring",
      port: 3000,
      url: "http://localhost:3000",
      status: grafanaHttp.ok ? "up" : "down",
      latencyMs: grafanaHttp.latencyMs,
      detail: grafanaHttp.ok
        ? `UI :3000 healthy · kafka-exporter ${exporterHttp.ok ? "up" : "down"}`
        : "Optional --profile monitoring stopped",
    },
  ];
  const serveState = stageStatusFromProbes(serveProbes, alertRuleNames.length > 0);

  const stages: PipelineStage[] = [
    {
      id: "ingestion",
      order: 1,
      title: "Sources & Ingestion",
      kicker: "STAGE 01 · EVENT & BATCH LANDING",
      labs: "Labs 01, 02, 08",
      summary:
        "Payment simulator and batch generator emit 10-airport transactions; Kafka partitions by passenger_id while Postgres + Debezium stream blocklist changes.",
      status: ingState.status,
      statusLabel: ingState.label,
      inputs: ["lab.generate (60d batch)", "lab.producer (live stream)", "Postgres passenger_blocklist"],
      outputs: ["data/raw/*.csv", "Kafka airport.transactions (3P)", "Debezium CDC topic"],
      probes: ingProbes,
      metrics: [
        {
          label: "Generated Txns",
          value: txCsv.exists ? txCsv.rows.toLocaleString() : "0",
          hint: txCsv.exists ? `${txCsv.sizeMb} MB CSV` : "Run lab.generate",
          tone: txCsv.exists ? "good" : "warn",
        },
        {
          label: "Passengers / Labels",
          value: paxCsv.exists ? `${paxCsv.rows.toLocaleString()} / ${lblCsv.rows.toLocaleString()}` : "None",
          hint: "labels.csv stays out of bronze",
        },
        {
          label: "Live Scenario",
          value: scenario,
          hint: "control/scenario",
          tone: scenario.includes("attack") ? "bad" : scenario.includes("drift") ? "warn" : "default",
        },
      ],
      checks: [
        { name: "Raw transactions CSV present", passed: txCsv.exists, detail: `${txCsv.rows.toLocaleString()} rows` },
        { name: "Kafka broker reachable (:9094)", passed: kafkaTcp.ok, detail: kafkaTcp.ok ? `${kafkaTcp.latencyMs} ms` : "Offline" },
        { name: "Debezium CDC optional profile", passed: connectHttp.ok, detail: `${connectors.length} connector(s)` },
      ],
      commands: [
        { label: "Start core stack", command: "docker compose up -d --wait" },
        { label: "Generate 60d batch", command: "PYTHONPATH=src uv run python -m lab.generate" },
        { label: "Stream live payments (20s)", command: "PYTHONPATH=src uv run python -m lab.producer --seconds 20" },
      ],
    },
    {
      id: "contract",
      order: 2,
      title: "Schema & Contract",
      kicker: "STAGE 02 · VALIDATION GATE",
      labs: "Labs 03, 09",
      summary:
        "Closed JSON Schema contract enforces 10 required fields, positive amounts, and enumerated airports/merchants under BACKWARD compatibility.",
      status: contractState.status,
      statusLabel: contractState.label,
      inputs: ["schemas/payment_event.json", "contracts/airport-payments.yaml", "Producer payloads"],
      outputs: ["Registered subject v1/v2", "Pytest contract suite", "HTTP 409 breaking rejection"],
      probes: contractProbes,
      metrics: [
        { label: "Required Fields", value: String(schemaInfo.required.length), hint: "additionalProperties: false", tone: "good" },
        { label: "Domain Enums", value: `${schemaInfo.airports} airports · ${schemaInfo.categories} cats` },
        {
          label: "Registry Subjects",
          value: schemaRegHttp.ok ? String(subjects.length) : "Offline",
          hint: "http://localhost:8081",
          tone: schemaRegHttp.ok ? "good" : "default",
        },
      ],
      checks: [
        { name: "JSON Schema contract loaded", passed: schemaInfo.required.length === 10, detail: "10 required fields" },
        { name: "Schema Registry API (:8081)", passed: schemaRegHttp.ok, detail: subjects.join(", ") || "Waiting for start" },
      ],
      commands: [
        { label: "Run contract & rule unit tests", command: "PYTHONPATH=src uv run python -m pytest tests -q" },
        { label: "List registered subjects", command: "curl -fsS http://localhost:8081/subjects" },
      ],
    },
    {
      id: "lakehouse",
      order: 3,
      title: "Lakehouse & Quality",
      kicker: "STAGE 03 · BRONZE → SILVER + QUARANTINE",
      labs: "Labs 04, 07",
      summary:
        "Spark lands raw CSVs in MinIO bronze, enforces 6 critical quality checks, routes bad rows to Delta quarantine with reject_reason, and writes idempotent silver days.",
      status: lakeState.status,
      statusLabel: lakeState.label,
      inputs: ["s3a://bronze/transactions/", "s3a://bronze/passengers/"],
      outputs: ["s3a://silver/transactions/ (Delta)", "s3a://quarantine/transactions/"],
      probes: lakeProbes,
      metrics: [
        { label: "Quality Rules", value: `${contractInfo.qualityRules.length || 6} checks`, hint: "NULL_KEY, AMOUNT, AIRPORT, HOUR...", tone: "good" },
        { label: "Spark Workers", value: sparkMasterHttp.ok ? `${sparkWorkers} ALIVE` : "Offline", tone: sparkWorkers > 0 ? "good" : "default" },
        { label: "Write Mode", value: "replaceWhere", hint: "Idempotent UTC day partitions" },
      ],
      checks: [
        { name: "MinIO S3 health (:9000)", passed: minioHttp.ok, detail: minioHttp.ok ? "Healthy" : "Offline" },
        { name: "Spark master + worker (:8082)", passed: sparkMasterHttp.ok && sparkWorkers > 0, detail: `${sparkWorkers} alive` },
        { name: "Contract quality rules defined", passed: contractInfo.qualityRules.length >= 4, detail: contractInfo.qualityRules.join(", ") },
      ],
      commands: [
        {
          label: "Upload raw CSVs to bronze",
          command: "docker compose exec -T minio-client mc cp /data/raw/transactions.csv local/bronze/transactions/transactions.csv",
        },
        {
          label: "Build silver Delta + quarantine",
          command: "docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/silver_transactions.py",
        },
      ],
    },
    {
      id: "scoring",
      order: 4,
      title: "Features, Scoring & CDC",
      kicker: "STAGE 04 · POINT-IN-TIME + FRAUD RULES",
      labs: "Labs 05, 06, 08",
      summary:
        "Point-in-time passenger features (event_date < as_of), Debezium-merged silver/blocklist, and unified pandas rules (score ≥ 50) in batch and streaming.",
      status: scoreState.status,
      statusLabel: scoreState.label,
      inputs: ["s3a://silver/transactions/", "s3a://silver/blocklist/", "kafka: airport.transactions"],
      outputs: ["s3a://gold/passenger_features/", "s3a://gold/fraud_scores/", "airport.fraud-alerts"],
      probes: scoreProbes,
      metrics: [
        { label: "Active Rules / Threshold", value: `${fraudRules.length} rules · ≥${alertThreshold} pts`, hint: "Unified batch + stream" },
        {
          label: "Latest Precision / Recall",
          value: evaluation ? `${(evaluation.precision * 100).toFixed(1)}% / ${(evaluation.recall * 100).toFixed(1)}%` : "Not run",
          hint: evaluation ? `Day ${evaluation.eventDate} (${evaluation.alerts} alerts)` : "Run evaluate.py",
          tone: evaluation && evaluation.precision >= 0.4 ? "good" : "default",
        },
        { label: "Streaming Scorer", value: sparkAppHttp.ok ? `${sparkApps} active` : "Idle", hint: "Spark UI :4040" },
      ],
      checks: [
        { name: "Unified rules loaded", passed: fraudRules.length === 7, detail: `${fraudRules.length} rules in fraud_rules.py` },
        { name: "Evaluation report present", passed: Boolean(evaluation), detail: evaluation ? `${evaluation.payments} payments on ${evaluation.eventDate}` : "Missing" },
        { name: "Spark streaming UI (:4040)", passed: sparkAppHttp.ok, detail: sparkAppHttp.ok ? "Active" : "No running stream" },
      ],
      commands: [
        { label: "Build point-in-time features", command: "docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/features_30d.py" },
        { label: "Score latest day + evaluate", command: "docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/score_batch.py" },
        {
          label: "Start live streaming scorer",
          command: "docker compose exec -d spark-master bash -c '/opt/spark/bin/spark-submit /opt/spark-apps/stream_fraud.py > /tmp/stream-fraud.log 2>&1'",
        },
      ],
    },
    {
      id: "serve",
      order: 5,
      title: "Serve, Observe & Govern",
      kicker: "STAGE 05 · ALERTS, GATES & ANALYST GOLD",
      labs: "Labs 10, 11, 12",
      summary:
        "Serves live fraud alerts, pseudonymised gold/transactions_masked (SHA-256 + salt), Prefect daily orchestration with precision gate, and Prometheus/Grafana alerts.",
      status: serveState.status,
      statusLabel: serveState.label,
      inputs: ["s3a://gold/fraud_scores/", "s3a://silver/transactions/", "Pushgateway + Kafka Exporter"],
      outputs: ["gold/transactions_masked/", "Prefect 06:00 deployment", "Grafana + Alert rules"],
      probes: serveProbes,
      metrics: [
        { label: "Prometheus Rules", value: `${alertRuleNames.length} alerts`, hint: alertRuleNames.join(", ") },
        { label: "Retention Policy", value: `${contractInfo.bronzeDays}d bronze / ${contractInfo.silverDays}d silver` },
        {
          label: "Ops Profiles Up",
          value: `${serveProbes.filter((p) => p.status === "up").length}/${serveProbes.length}`,
          hint: "Prefect / Prometheus / Grafana",
          tone: serveProbes.some((p) => p.status === "up") ? "good" : "default",
        },
      ],
      checks: [
        { name: "Prometheus alert rules configured", passed: alertRuleNames.length >= 3, detail: alertRuleNames.join(", ") },
        { name: "Prefect server (:4200)", passed: prefectHttp.ok, detail: prefectHttp.ok ? "Healthy" : "Stopped" },
        { name: "Prometheus + Grafana (:9090/:3000)", passed: promHttp.ok && grafanaHttp.ok, detail: promHttp.ok ? "Scraping" : "Stopped" },
      ],
      commands: [
        { label: "Build masked analyst dataset", command: "docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/mask_payments.py" },
        { label: "Run Prefect daily pipeline", command: "PREFECT_API_URL=http://localhost:4200/api uv run python flows/pipeline.py" },
        { label: "Start monitoring stack", command: "docker compose --profile monitoring up -d --wait" },
      ],
    },
  ];

  const healthyCount = stages.filter((s) => s.status === "healthy").length;
  const partialOrReady = stages.filter((s) => s.status === "partial" || s.status === "ready").length;
  let overallStatus: StageStatus = "offline";
  if (healthyCount === stages.length) overallStatus = "healthy";
  else if (healthyCount > 0 || stages.some((s) => s.status === "partial")) overallStatus = "partial";
  else if (partialOrReady > 0) overallStatus = "ready";

  return {
    checkedAt: new Date().toISOString(),
    repoRoot: REPO_ROOT,
    scenario,
    dockerAvailable: dockerInfo.dockerAvailable,
    runningContainers: dockerInfo.runningServices.size,
    overallStatus,
    evaluation,
    fraudRules,
    alertThreshold,
    stages,
  };
}

export function buildDiagnosticReport(snapshot: PipelineSnapshot, query: string, stageFilter?: StageId): {
  headline: string;
  targetStage: StageId | "all";
  findings: string[];
  markdown: string;
} {
  const q = query.toLowerCase();
  const matchedStage: StageId | "all" =
    stageFilter ??
    (q.includes("ingest") || q.includes("kafka") || q.includes("producer") || q.includes("cdc") || q.includes("stage 1")
      ? "ingestion"
      : q.includes("schema") || q.includes("contract") || q.includes("registry") || q.includes("stage 2")
        ? "contract"
        : q.includes("lakehouse") || q.includes("minio") || q.includes("bronze") || q.includes("silver") || q.includes("quarantine") || q.includes("stage 3")
          ? "lakehouse"
          : q.includes("scor") || q.includes("feature") || q.includes("rule") || q.includes("leak") || q.includes("eval") || q.includes("stage 4")
            ? "scoring"
            : q.includes("serve") || q.includes("prefect") || q.includes("prom") || q.includes("grafana") || q.includes("govern") || q.includes("mask") || q.includes("stage 5")
              ? "serve"
              : "all");

  const targets = matchedStage === "all" ? snapshot.stages : snapshot.stages.filter((s) => s.id === matchedStage);
  const lines: string[] = [];
  const findings: string[] = [];

  if (matchedStage === "all") {
    lines.push(
      `Checked all **5 workflow stages** (Ingestion -> Serve) at \`${new Date(snapshot.checkedAt).toLocaleTimeString()}\`.`,
      `- **Containers running:** ${snapshot.runningContainers} (${snapshot.dockerAvailable ? "Docker responsive" : "Docker daemon unreachable"})`,
      `- **Live scenario:** \`${snapshot.scenario}\``,
    );
    if (snapshot.evaluation) {
      const ev = snapshot.evaluation;
      lines.push(
        `- **Latest evaluation (${ev.eventDate}):** ${ev.alerts}/${ev.payments} alerts (${ev.frauds} frauds), precision **${(ev.precision * 100).toFixed(1)}%**, recall **${(ev.recall * 100).toFixed(1)}%**.`,
      );
    }
    lines.push("");
    for (const s of targets) {
      const upProbes = s.probes.filter((p) => p.status === "up").length;
      const readyProbes = s.probes.filter((p) => p.status === "artifact-ready").length;
      const summaryLine = `**${s.order}. ${s.title}** — \`${s.statusLabel}\` (${upProbes} live, ${readyProbes} host artifacts)`;
      lines.push(`- ${summaryLine}`);
      findings.push(`${s.title}: ${s.statusLabel}`);
    }
    if (snapshot.runningContainers === 0) {
      lines.push(
        "",
        "Core Docker services are currently stopped, while local generated data (`data/raw/*.csv`) and evaluation artifacts (`data/reports/evaluation.json`) are intact. Start the core stack with `docker compose up -d --wait` to light up live probes.",
      );
    }
  } else {
    const s = targets[0];
    lines.push(`### Stage ${s.order}: ${s.title} (${s.statusLabel})`, "", s.summary, "");
    for (const p of s.probes) {
      const icon = p.status === "up" ? "LIVE" : p.status === "artifact-ready" ? "READY" : "DOWN";
      lines.push(`- **[${icon}] ${p.name}**${p.port ? ` (:${p.port})` : ""}: ${p.detail}`);
      findings.push(`${p.name}: ${icon}`);
    }
    if (s.id === "scoring" && snapshot.fraudRules.length) {
      lines.push(
        "",
        `**Active rules (alert >= ${snapshot.alertThreshold} pts):** ` +
          snapshot.fraudRules.map((r) => `\`${r.name}\` (${r.points})`).join(", "),
      );
    }
    lines.push("", "**Next command:**", `\`${s.commands[0]?.command ?? "docker compose up -d --wait"}\``);
  }

  return {
    headline:
      matchedStage === "all"
        ? `Pipeline check complete (${snapshot.runningContainers} containers up)`
        : `Stage ${targets[0].order} · ${targets[0].title}: ${targets[0].statusLabel}`,
    targetStage: matchedStage,
    findings,
    markdown: lines.join("\n"),
  };
}
