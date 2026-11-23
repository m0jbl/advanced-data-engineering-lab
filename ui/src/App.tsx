import { RefreshCw, Server, Sparkles } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { ControlPanel } from "./ControlPanel";
import { AiStageAssistant } from "./components/AiStageAssistant";
import { StageInspector } from "./components/StageInspector";
import { StatusBadge } from "./components/StatusBadge";
import { WorkflowCanvas } from "./components/WorkflowCanvas";
import type { PipelineSnapshot, StageId } from "./types/pipeline";
import "./control-panel.css";

function PipelineDiagnostics() {
  const [snapshot, setSnapshot] = useState<PipelineSnapshot | null>(null);
  const [selectedStageId, setSelectedStageId] = useState<StageId>("ingestion");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [pendingStageCheck, setPendingStageCheck] = useState<{ stageId: StageId; nonce: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadPipeline = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    try {
      const res = await fetch("/api/pipeline");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = (await res.json()) as PipelineSnapshot;
      setSnapshot(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to reach pipeline API");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    loadPipeline(false);
  }, [loadPipeline]);

  const selectedStage = snapshot?.stages.find((s) => s.id === selectedStageId) ?? snapshot?.stages[0];

  return (
    <div className="bg-slate-950 text-slate-100">
      <div className="space-y-5 px-4 py-5 sm:px-6">
        <header className="flex flex-wrap items-center justify-between gap-4 border-b border-slate-700 pb-4">
          <div>
            <div className="flex items-center gap-2.5">
              <span className="inline-flex h-6 items-center rounded-md bg-orange-500/15 px-2 font-mono text-[11px] font-semibold text-orange-300 ring-1 ring-orange-500/30">
                AIRPORT PAYMENTS PIPELINE
              </span>
              {snapshot && <StatusBadge status={snapshot.overallStatus} />}
            </div>
            <h2 className="mt-1.5 text-lg font-semibold tracking-tight text-white sm:text-xl">
              Pipeline diagnostics
            </h2>
            <p className="text-xs text-slate-400">
              Live stage diagnostics across Kafka, Schema Registry, MinIO Delta Lakehouse, Spark Fraud Rules, and
              Prefect / Prometheus.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            {snapshot && (
              <div className="flex items-center gap-3 rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-xs">
                <div className="flex items-center gap-1.5 text-slate-300">
                  <Server className="h-3.5 w-3.5 text-slate-400" />
                  <span className="font-mono font-semibold">{snapshot.runningContainers}</span>
                  <span className="text-slate-500">containers</span>
                </div>
                <span className="h-3.5 w-px bg-slate-800" />
                <div className="text-slate-400">
                  Scenario: <span className="font-mono text-slate-200">{snapshot.scenario}</span>
                </div>
              </div>
            )}

            <button
              type="button"
              onClick={() => loadPipeline(true)}
              disabled={refreshing}
              className="inline-flex items-center gap-1.5 rounded-xl border border-slate-700 bg-slate-900 px-3 py-2 text-xs font-medium text-slate-200 transition hover:border-orange-500/60 hover:text-white disabled:opacity-50"
            >
              <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin text-orange-400" : ""}`} />
              Refresh Probes
            </button>

            <button
              type="button"
              onClick={() => setPendingStageCheck({ stageId: selectedStageId, nonce: Date.now() })}
              className="inline-flex items-center gap-1.5 rounded-xl bg-orange-500 px-3.5 py-2 text-xs font-semibold text-slate-950 transition hover:bg-orange-400"
            >
              <Sparkles className="h-3.5 w-3.5" />
              AI Check Stage 0{selectedStage?.order ?? 1}
            </button>
          </div>
        </header>

        {error && (
          <div role="alert" className="rounded-xl border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-xs text-rose-200">
            Could not load pipeline status ({error}). Check the lab server and select Refresh Probes.
          </div>
        )}

        {loading && !snapshot ? (
          <div className="rounded-2xl border border-slate-800 bg-slate-900/50 p-10 text-center text-sm text-slate-400">
            Probing workflow stages...
          </div>
        ) : snapshot ? (
          <>
            <WorkflowCanvas
              stages={snapshot.stages}
              selectedStageId={selectedStageId}
              onSelectStage={setSelectedStageId}
              onCheckStage={(id) => setPendingStageCheck({ stageId: id, nonce: Date.now() })}
              isChecking={refreshing}
            />

            <div className="grid grid-cols-1 gap-5 lg:grid-cols-12">
              <div className="lg:col-span-8">
                {selectedStage && (
                  <StageInspector
                    stage={selectedStage}
                    evaluation={snapshot.evaluation}
                    fraudRules={snapshot.fraudRules}
                    alertThreshold={snapshot.alertThreshold}
                  />
                )}
              </div>
              <div className="lg:col-span-4">
                <AiStageAssistant
                  onSnapshotUpdate={setSnapshot}
                  onSelectStage={setSelectedStageId}
                  pendingStageCheck={pendingStageCheck}
                />
              </div>
            </div>
          </>
        ) : null}
      </div>
    </div>
  );
}

export default function App() {
  const [diagnosticsOpened, setDiagnosticsOpened] = useState(false);

  return (
    <main className="lab-control">
      <ControlPanel />
      <details
        className="lab-diagnostics"
        onToggle={({ currentTarget }) => {
          if (currentTarget.open) setDiagnosticsOpened(true);
        }}
      >
        <summary>Pipeline diagnostics and assistant</summary>
        {diagnosticsOpened && <PipelineDiagnostics />}
      </details>
    </main>
  );
}
