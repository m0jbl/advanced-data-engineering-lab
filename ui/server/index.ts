import { createUIMessageStream, pipeUIMessageStreamToResponse } from "ai";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import type { PipelineUIMessage, StageId } from "../src/types/pipeline.ts";
import { buildDiagnosticReport, inspectPipeline } from "./inspect.ts";

const PORT = Number(process.env.PORT ?? 3030);
const DIST_DIR = path.resolve(import.meta.dirname, "../dist");

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".json": "application/json; charset=utf-8",
};

async function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

function extractLatestUserPrompt(body: unknown): { text: string; stageId?: StageId } {
  if (!body || typeof body !== "object") return { text: "Check all stages" };
  let stageId: StageId | undefined;
  if ("stageId" in body && typeof body.stageId === "string") {
    const candidate = body.stageId;
    if (
      candidate === "ingestion" ||
      candidate === "contract" ||
      candidate === "lakehouse" ||
      candidate === "scoring" ||
      candidate === "serve"
    ) {
      stageId = candidate;
    }
  }
  if (!("messages" in body) || !Array.isArray(body.messages)) {
    return { text: "Check all stages", stageId };
  }
  const messages = body.messages;
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg && typeof msg === "object" && "role" in msg && msg.role === "user") {
      if ("parts" in msg && Array.isArray(msg.parts)) {
        const textParts = msg.parts
          .filter((p): p is { type: "text"; text: string } => Boolean(p && typeof p === "object" && "type" in p && p.type === "text" && "text" in p && typeof p.text === "string"))
          .map((p) => p.text);
        if (textParts.length) return { text: textParts.join("\n"), stageId };
      }
      if ("content" in msg && typeof msg.content === "string") {
        return { text: msg.content, stageId };
      }
    }
  }
  return { text: "Check all stages", stageId };
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

const server = http.createServer(async (req, res) => {
  const urlPath = (req.url ?? "/").split("?")[0];

  if (req.method === "GET" && urlPath === "/api/pipeline") {
    const snapshot = await inspectPipeline();
    sendJson(res, 200, snapshot);
    return;
  }

  if (req.method === "POST" && urlPath === "/api/pipeline/check") {
    const snapshot = await inspectPipeline();
    sendJson(res, 200, snapshot);
    return;
  }

  if (req.method === "POST" && urlPath === "/api/chat") {
    try {
      const body = await readJsonBody(req);
      const { text: promptText, stageId } = extractLatestUserPrompt(body);

      const stream = createUIMessageStream<PipelineUIMessage>({
        async execute({ writer }) {
          const startedAt = performance.now();
          writer.write({ type: "start" });

          const probeId = `probe-${Date.now()}`;
          writer.write({
            type: "data-notification",
            data: { message: "Probing pipeline stages and local artifacts...", level: "info" },
            transient: true,
          });

          writer.write({
            type: "data-stage-probe",
            id: probeId,
            data: {
              stageId: stageId ?? "all",
              phase: "checking",
              status: "ready",
              headline: "Running live TCP/HTTP & artifact checks...",
              findings: [],
              durationMs: 0,
            },
          });

          const snapshot = await inspectPipeline();
          const report = buildDiagnosticReport(snapshot, promptText, stageId);
          const durationMs = Math.round(performance.now() - startedAt);

          // Reconcile the in-flight probe data part by reusing the same ID
          writer.write({
            type: "data-stage-probe",
            id: probeId,
            data: {
              stageId: report.targetStage,
              phase: "complete",
              status:
                report.targetStage === "all"
                  ? snapshot.overallStatus
                  : (snapshot.stages.find((s) => s.id === report.targetStage)?.status ?? snapshot.overallStatus),
              headline: report.headline,
              findings: report.findings,
              durationMs,
            },
          });

          writer.write({
            type: "data-pipeline-snapshot",
            id: `snap-${Date.now()}`,
            data: snapshot,
          });

          const textId = `diag-${Date.now()}`;
          writer.write({ type: "text-start", id: textId });
          const tokens = report.markdown.match(/\S+\s*/g) ?? [report.markdown];
          for (const tok of tokens) {
            writer.write({ type: "text-delta", id: textId, delta: tok });
            const { promise, resolve } = Promise.withResolvers<void>();
            setTimeout(resolve, 8);
            await promise;
          }
          writer.write({ type: "text-end", id: textId });

          writer.write({
            type: "data-notification",
            data: {
              message: `Check completed in ${durationMs} ms`,
              level: snapshot.overallStatus === "healthy" ? "success" : "info",
            },
            transient: true,
          });

          writer.write({ type: "finish" });
        },
        onError: (err) => (err instanceof Error ? err.message : "Pipeline probe failed"),
      });

      await pipeUIMessageStreamToResponse({ response: res, stream });
    } catch (err) {
      sendJson(res, 500, { error: err instanceof Error ? err.message : "Chat stream error" });
    }
    return;
  }

  // Static serving of built Vite assets (when running `npm start`)
  if (fs.existsSync(DIST_DIR)) {
    const safeRel = path.normalize(urlPath === "/" ? "/index.html" : urlPath).replace(/^(\.\.(\/|\\|$))+/, "");
    let filePath = path.join(DIST_DIR, safeRel);
    if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
      filePath = path.join(DIST_DIR, "index.html");
    }
    if (fs.existsSync(filePath)) {
      const ext = path.extname(filePath);
      res.writeHead(200, { "Content-Type": MIME[ext] ?? "application/octet-stream" });
      fs.createReadStream(filePath).pipe(res);
      return;
    }
  }

  sendJson(res, 404, { error: "Not found" });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`Pipeline API listening on http://127.0.0.1:${PORT}`);
});
