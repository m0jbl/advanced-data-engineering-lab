import type { StageStatus } from "../types/pipeline";

const STYLES: Record<StageStatus, { wrap: string; dot: string; text: string }> = {
  healthy: {
    wrap: "bg-emerald-500/12 border-emerald-500/35 text-emerald-300",
    dot: "bg-emerald-400 shadow-[0_0_8px_rgba(52,211,153,0.8)]",
    text: "Healthy",
  },
  partial: {
    wrap: "bg-amber-500/12 border-amber-500/35 text-amber-300",
    dot: "bg-amber-400 shadow-[0_0_8px_rgba(251,191,36,0.75)]",
    text: "Partial",
  },
  ready: {
    wrap: "bg-sky-500/12 border-sky-500/35 text-sky-300",
    dot: "bg-sky-400",
    text: "Artifacts Ready",
  },
  offline: {
    wrap: "bg-slate-800/80 border-slate-700 text-slate-400",
    dot: "bg-slate-500",
    text: "Offline",
  },
};

export function StatusBadge({ status, label }: { status: StageStatus; label?: string }) {
  const s = STYLES[status];
  return (
    <span className={`inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full border px-2.5 py-0.5 text-[11px] font-medium tracking-wide ${s.wrap}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${s.dot}`} />
      <span>{label ?? s.text}</span>
    </span>
  );
}
