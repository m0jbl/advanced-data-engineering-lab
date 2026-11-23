import { execFile, spawn } from "node:child_process";
import type { IncomingMessage, ServerResponse } from "node:http";
import path from "node:path";
import { promisify } from "node:util";

const root = path.resolve(import.meta.dirname, "../..");
const python = ["run", "--locked", "python", "-u", "-m", "lab.dashboard"];
const options = { cwd: root, env: { ...process.env, PYTHONPATH: path.join(root, "src") } };
const maxOutput = 48 * 1024;
const maxJobs = 24;
const maxRuntime = 25 * 60 * 1000;

type Catalog = { actions: { id: string }[] };
const { CODESPACE_NAME, GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN } = process.env;
const forwardedHost = CODESPACE_NAME && GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN
  ? `${CODESPACE_NAME}-${process.env.PORT ?? 3030}.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}`
  : undefined;
type Job = {
  id: string;
  action: string;
  state: "running" | "succeeded" | "failed";
  output: string;
  exitCode: number | null;
};

let catalog: Promise<Catalog> | undefined;
let activePid: number | undefined;
let sequence = 0;
const jobs: Job[] = [];
const exec = promisify(execFile);

function loadCatalog(): Promise<Catalog> {
  return (catalog ??= exec("uv", [...python, "catalog"], { ...options, timeout: 30_000 })
    .then(({ stdout }) => JSON.parse(stdout) as Catalog)
    .catch((error: unknown) => {
      catalog = undefined;
      throw error;
    }));
}

function signalGroup(pid: number | undefined, signal: NodeJS.Signals) {
  if (pid === undefined) return;
  try {
    process.kill(-pid, signal);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") console.error(error);
  }
}

process.on("exit", () => signalGroup(activePid, "SIGTERM"));
for (const [signal, code] of [["SIGINT", 130], ["SIGTERM", 143]] as const) {
  process.once(signal, () => process.exit(code));
}

function start(action: string): Job {
  const job: Job = { id: String(++sequence), action, state: "running", output: "", exitCode: null };
  const child = spawn("uv", [...python, "run", action], { ...options, detached: true, stdio: ["ignore", "pipe", "pipe"] });
  activePid = child.pid;
  jobs.unshift(job);
  if (jobs.length > maxJobs) jobs.pop();

  const append = (text: string) => {
    job.output = (job.output + text).slice(-maxOutput);
  };
  child.stdout.setEncoding("utf8").on("data", append);
  child.stderr.setEncoding("utf8").on("data", append);
  child.once("error", (error) => append(`\n${error.message}\n`));
  let killTimer: NodeJS.Timeout | undefined;
  const timeout = setTimeout(() => {
    append("\nStopped: this action exceeded 25 minutes.\n");
    signalGroup(child.pid, "SIGTERM");
    killTimer = setTimeout(() => signalGroup(child.pid, "SIGKILL"), 5000);
  }, maxRuntime);
  child.once("close", (code, signal) => {
    clearTimeout(timeout);
    clearTimeout(killTimer);
    if (signal) append(`\nStopped by ${signal}.\n`);
    job.exitCode = code;
    job.state = code === 0 ? "succeeded" : "failed";
    activePid = undefined;
  });
  return job;
}

function json(res: ServerResponse, status: number, value: unknown) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(value));
}

export async function control(req: IncomingMessage, res: ServerResponse, route: string) {
  try {
    const host = req.headers.host;
    if (!host || (!/^(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/i.test(host) && host !== forwardedHost)) {
      json(res, 403, { error: "Open the local or privately forwarded Codespace dashboard" });
      return;
    }
    if (req.method === "GET" && route === "/api/control") {
      json(res, 200, { ...await loadCatalog(), jobs });
      return;
    }
    if (req.method !== "POST" || route !== "/api/control/run") {
      json(res, 404, { error: "Unknown control route" });
      return;
    }
    const site = req.headers["sec-fetch-site"];
    if (site && site !== "same-origin" && site !== "none") {
      json(res, 403, { error: "Open the dashboard from its own origin to run a step" });
      return;
    }
    if (req.headers["content-type"]?.split(";")[0] !== "application/json") {
      json(res, 415, { error: "Send an application/json request" });
      return;
    }
    let body = "";
    req.setEncoding("utf8");
    for await (const chunk of req.iterator({ destroyOnReturn: false })) {
      body += chunk;
      if (body.length > 1024) {
        req.resume();
        json(res, 413, { error: "Request must contain only an action ID" });
        return;
      }
    }
    let action: unknown;
    try {
      action = JSON.parse(body)?.action;
    } catch {
      json(res, 400, { error: "Invalid JSON" });
      return;
    }
    const available = await loadCatalog();
    if (typeof action !== "string" || !available.actions.some((entry) => entry.id === action)) {
      json(res, 400, { error: "Unknown lab action" });
    } else if (jobs[0]?.state === "running") {
      json(res, 409, { error: "Wait for the current lab action to finish" });
    } else {
      json(res, 202, start(action));
    }
  } catch (error) {
    console.error(error);
    json(res, 500, { error: "Could not load the Python lab controller. Run uv sync --locked from the lab root." });
  }
}
