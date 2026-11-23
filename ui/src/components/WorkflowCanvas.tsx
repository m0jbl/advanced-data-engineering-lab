import { Activity, ArrowRight, Database, FileCheck2, Radio, ShieldCheck, Sparkles } from "lucide-react";
import type { PipelineStage, StageId } from "../types/pipeline";
import { StatusBadge } from "./StatusBadge";

const STAGE_ICONS: Record<StageId, typeof Radio> = {
  ingestion: Radio,
  contract: FileCheck2,
  lakehouse: Database,
  scoring: Activity,
  serve: ShieldCheck,
};

interface WorkflowCanvasProps {
  stages: PipelineStage[];
  selectedStageId: StageId;
  onSelectStage: (id: StageId) => void;
  onCheckStage: (id: StageId) => void;
  isChecking: boolean;
}

export function WorkflowCanvas({
  stages,
  selectedStageId,
  onSelectStage,
  onCheckStage,
  isChecking,
}: WorkflowCanvasProps) {
  return (
    <section className="rounded-2xl border border-slate-800/90 bg-slate-900/60 p-4 shadow-xl shadow-black/30 backdrop-blur">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-400">
            End-to-End Pipeline Topology · Ingestion to Serve
          </p>
          <h2 className="text-sm font-medium text-slate-200">
            Click any stage to inspect probes, quality gates, and artifacts
          </h2>
        </div>
        <div className="flex items-center gap-3 text-[11px] text-slate-400">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-emerald-400" /> Live
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-sky-400" /> Disk Artifact Ready
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-slate-600" /> Container Stopped
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-5">
        {stages.map((stage, idx) => {
          const Icon = STAGE_ICONS[stage.id];
          const selected = stage.id === selectedStageId;
          const borderAccent = selected
            ? "border-orange-500/80 ring-1 ring-orange-500/40 bg-slate-900"
            : "border-slate-800 bg-slate-950/60 hover:border-slate-700 hover:bg-slate-900/80";

          return (
            <div key={stage.id} className="relative flex items-stretch">
              <div
                role="button"
                tabIndex={0}
                onClick={() => onSelectStage(stage.id)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onSelectStage(stage.id);
                  }
                }}
                className={`group flex w-full cursor-pointer flex-col justify-between rounded-xl border p-3.5 text-left transition ${borderAccent}`}
              >
                <div>
                  <div className="mb-2.5 flex items-center justify-between gap-2">
                    <div className="flex items-center gap-2">
                      <div
                        className={`flex h-7 w-7 items-center justify-center rounded-lg border ${
                          selected
                            ? "border-orange-500/50 bg-orange-500/15 text-orange-300"
                            : "border-slate-700/80 bg-slate-900 text-slate-300"
                        }`}
                      >
                        <Icon className="h-3.5 w-3.5" />
                      </div>
                      <span className="font-mono text-[11px] font-semibold text-slate-400">
                        0{stage.order}
                      </span>
                    </div>
                    <span className="rounded bg-slate-800/90 px-1.5 py-0.5 font-mono text-[10px] text-slate-400">
                      {stage.labs}
                    </span>
                  </div>

                  <h3 className="text-[14px] font-semibold tracking-tight text-slate-100">
                    {stage.title}
                  </h3>
                  <p className="mt-1 line-clamp-2 text-[11.5px] leading-relaxed text-slate-400">
                    {stage.summary}
                  </p>
                </div>

                <div className="mt-3 space-y-2.5 border-t border-slate-800/80 pt-2.5">
                  <StatusBadge status={stage.status} label={stage.statusLabel} />

                  <div className="space-y-1">
                    {stage.probes.map((probe) => (
                      <div key={probe.id} className="flex items-center justify-between gap-2 text-[11px]">
                        <span className="flex min-w-0 items-center gap-1.5 text-slate-300">
                          <span
                            className={`h-1.5 w-1.5 shrink-0 rounded-full ${
                              probe.status === "up"
                                ? "bg-emerald-400"
                                : probe.status === "artifact-ready"
                                  ? "bg-sky-400"
                                  : "bg-slate-600"
                            }`}
                          />
                          <span className="truncate">{probe.name}</span>
                        </span>
                        <span className="shrink-0 font-mono text-[10px] text-slate-500">
                          {probe.latencyMs !== undefined && probe.status === "up"
                            ? `${probe.latencyMs}ms`
                            : probe.port
                              ? `:${probe.port}`
                              : probe.status === "artifact-ready"
                                ? "disk"
                                : "off"}
                        </span>
                      </div>
                    ))}
                  </div>

                  <div className="flex items-center justify-between pt-1">
                    <span className="font-mono text-[10px] text-slate-500">
                      {stage.checks.filter((c) => c.passed).length}/{stage.checks.length} checks ok
                    </span>
                    <button
                      type="button"
                      disabled={isChecking}
                      onClick={(e) => {
                        e.stopPropagation();
                        onSelectStage(stage.id);
                        onCheckStage(stage.id);
                      }}
                      className="inline-flex items-center gap-1 rounded-md border border-slate-700/80 bg-slate-900 px-2 py-1 text-[10.5px] font-medium text-slate-200 transition hover:border-orange-500/60 hover:text-orange-200 disabled:opacity-50"
                    >
                      <Sparkles className="h-3 w-3 text-orange-400" />
                      Check
                    </button>
                  </div>
                </div>
              </div>

              {idx < stages.length - 1 && (
                <div className="hidden items-center justify-center px-0.5 lg:flex" aria-hidden="true">
                  <div className="flex flex-col items-center">
                    <ArrowRight
                      className={`h-4 w-4 ${
                        stage.status === "healthy"
                          ? "text-emerald-400"
                          : stage.status === "partial" || stage.status === "ready"
                            ? "text-orange-400/80"
                            : "text-slate-700"
                      }`}
                    />
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </section>
  );
}
