import { Check, CheckCircle2, Copy, ExternalLink, Terminal, XCircle } from "lucide-react";
import { useState } from "react";
import type { EvaluationSnapshot, PipelineStage } from "../types/pipeline";
import { StatusBadge } from "./StatusBadge";

interface StageInspectorProps {
  stage: PipelineStage;
  evaluation: EvaluationSnapshot | null;
  fraudRules: { name: string; points: number }[];
  alertThreshold: number;
}

export function StageInspector({ stage, evaluation, fraudRules, alertThreshold }: StageInspectorProps) {
  const [copiedCmd, setCopiedCmd] = useState<string | null>(null);

  const copyCommand = async (cmd: string) => {
    await navigator.clipboard.writeText(cmd);
    setCopiedCmd(cmd);
    setTimeout(() => setCopiedCmd((prev) => (prev === cmd ? null : prev)), 1500);
  };

  return (
    <div className="flex flex-col gap-4 rounded-2xl border border-slate-800/90 bg-slate-900/60 p-5 shadow-xl shadow-black/30">
      <div className="flex flex-wrap items-start justify-between gap-3 border-b border-slate-800 pb-4">
        <div>
          <p className="font-mono text-[11px] font-semibold uppercase tracking-[0.14em] text-orange-400">
            {stage.kicker} · {stage.labs}
          </p>
          <h2 className="mt-1 text-xl font-semibold tracking-tight text-white">{stage.title}</h2>
          <p className="mt-1 text-sm text-slate-400">{stage.summary}</p>
        </div>
        <StatusBadge status={stage.status} label={stage.statusLabel} />
      </div>

      {/* Metrics Strip */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        {stage.metrics.map((m) => (
          <div key={m.label} className="rounded-xl border border-slate-800 bg-slate-950/70 p-3">
            <p className="text-[11px] font-medium uppercase tracking-wider text-slate-400">{m.label}</p>
            <p
              className={`mt-1 font-mono text-base font-semibold ${
                m.tone === "good"
                  ? "text-emerald-300"
                  : m.tone === "warn"
                    ? "text-amber-300"
                    : m.tone === "bad"
                      ? "text-rose-300"
                      : "text-slate-100"
              }`}
            >
              {m.value}
            </p>
            {m.hint && <p className="mt-0.5 text-[11px] text-slate-500">{m.hint}</p>}
          </div>
        ))}
      </div>

      {/* Data Flow I/O + Live Probes */}
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-3.5">
          <h3 className="mb-2.5 text-xs font-semibold uppercase tracking-wider text-slate-400">
            Live Service & Artifact Probes
          </h3>
          <div className="space-y-2">
            {stage.probes.map((probe) => (
              <div
                key={probe.id}
                className="flex items-start justify-between gap-3 rounded-lg border border-slate-800/80 bg-slate-900/70 px-3 py-2"
              >
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span
                      className={`h-2 w-2 rounded-full ${
                        probe.status === "up"
                          ? "bg-emerald-400"
                          : probe.status === "artifact-ready"
                            ? "bg-sky-400"
                            : "bg-slate-600"
                      }`}
                    />
                    <span className="text-xs font-semibold text-slate-100">{probe.name}</span>
                    <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[10px] text-slate-400">
                      {probe.profile}
                    </span>
                  </div>
                  <p className="mt-1 text-[11.5px] text-slate-400">{probe.detail}</p>
                </div>
                {probe.url && probe.status === "up" && (
                  <a
                    href={probe.url}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex shrink-0 items-center gap-1 rounded-md border border-slate-700 px-2 py-1 text-[11px] text-slate-200 hover:border-orange-500/60 hover:text-orange-200"
                  >
                    Open <ExternalLink className="h-3 w-3" />
                  </a>
                )}
              </div>
            ))}
          </div>
        </div>

        <div className="space-y-3 rounded-xl border border-slate-800 bg-slate-950/60 p-3.5">
          <div>
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
              Stage Verification Checks
            </h3>
            <div className="space-y-1.5">
              {stage.checks.map((c) => (
                <div key={c.name} className="flex items-center justify-between gap-2 text-xs">
                  <span className="flex items-center gap-2 text-slate-200">
                    {c.passed ? (
                      <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-emerald-400" />
                    ) : (
                      <XCircle className="h-3.5 w-3.5 shrink-0 text-slate-500" />
                    )}
                    {c.name}
                  </span>
                  <span className="font-mono text-[11px] text-slate-400">{c.detail}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 border-t border-slate-800/80 pt-3">
            <div>
              <p className="text-[10.5px] font-semibold uppercase tracking-wider text-slate-500">Inputs</p>
              <ul className="mt-1 space-y-1">
                {stage.inputs.map((i) => (
                  <li key={i} className="truncate font-mono text-[11px] text-slate-300">
                    → {i}
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <p className="text-[10.5px] font-semibold uppercase tracking-wider text-slate-500">Outputs</p>
              <ul className="mt-1 space-y-1">
                {stage.outputs.map((o) => (
                  <li key={o} className="truncate font-mono text-[11px] text-orange-200/90">
                    ← {o}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      </div>

      {/* Stage 4 extra detail: Fraud rules & evaluation.json */}
      {stage.id === "scoring" && (
        <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-3.5">
          <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Fraud Scoring Rules (fraud_rules.py · Alert at &ge; {alertThreshold} pts)
            </h3>
            {evaluation && (
              <span className="font-mono text-[11px] text-emerald-300">
                Latest eval ({evaluation.eventDate}): {evaluation.alerts}/{evaluation.payments} alerts · Precision{" "}
                {(evaluation.precision * 100).toFixed(1)}% · Recall {(evaluation.recall * 100).toFixed(1)}%
              </span>
            )}
          </div>
          <div className="flex flex-wrap gap-2">
            {fraudRules.map((r) => (
              <span
                key={r.name}
                className="inline-flex items-center gap-1.5 rounded-lg border border-slate-800 bg-slate-900 px-2.5 py-1 font-mono text-[11px] text-slate-200"
              >
                <span>{r.name}</span>
                <span className="rounded bg-orange-500/20 px-1.5 py-0.5 text-[10px] font-semibold text-orange-300">
                  +{r.points}
                </span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Copyable Lab Commands */}
      <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-3.5">
        <div className="mb-2 flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
          <Terminal className="h-3.5 w-3.5 text-orange-400" />
          Explicit Commands to Run This Stage
        </div>
        <div className="space-y-2">
          {stage.commands.map((c) => (
            <div
              key={c.command}
              className="flex items-center justify-between gap-3 rounded-lg border border-slate-800 bg-slate-900/90 px-3 py-2"
            >
              <div className="min-w-0">
                <p className="text-[11px] text-slate-400">{c.label}</p>
                <code className="block truncate font-mono text-xs text-slate-100">{c.command}</code>
              </div>
              <button
                type="button"
                onClick={() => copyCommand(c.command)}
                className="inline-flex shrink-0 items-center gap-1 rounded-md border border-slate-700 bg-slate-800 px-2 py-1 text-[11px] text-slate-200 hover:border-orange-500/60"
              >
                {copiedCmd === c.command ? <Check className="h-3 w-3 text-emerald-400" /> : <Copy className="h-3 w-3" />}
                {copiedCmd === c.command ? "Copied" : "Copy"}
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
