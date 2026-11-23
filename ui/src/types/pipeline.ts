import type { UIMessage } from "ai";

export type StageId =
  | "ingestion"
  | "contract"
  | "lakehouse"
  | "scoring"
  | "serve";

export type StageStatus = "healthy" | "partial" | "ready" | "offline";

export interface ServiceProbe {
  id: string;
  name: string;
  profile: string;
  port?: number;
  url?: string;
  status: "up" | "down" | "artifact-ready";
  latencyMs?: number;
  detail: string;
}

export interface StageMetric {
  label: string;
  value: string;
  hint?: string;
  tone?: "default" | "good" | "warn" | "bad";
}

export interface StageCommand {
  label: string;
  command: string;
  note?: string;
}

export interface PipelineStage {
  id: StageId;
  order: number;
  title: string;
  kicker: string;
  labs: string;
  summary: string;
  status: StageStatus;
  statusLabel: string;
  inputs: string[];
  outputs: string[];
  probes: ServiceProbe[];
  metrics: StageMetric[];
  checks: { name: string; passed: boolean; detail: string }[];
  commands: StageCommand[];
}

export interface EvaluationSnapshot {
  eventDate: string;
  payments: number;
  frauds: number;
  alerts: number;
  precision: number;
  recall: number;
}

export interface PipelineSnapshot {
  checkedAt: string;
  repoRoot: string;
  scenario: string;
  dockerAvailable: boolean;
  runningContainers: number;
  overallStatus: StageStatus;
  evaluation: EvaluationSnapshot | null;
  fraudRules: { name: string; points: number }[];
  alertThreshold: number;
  stages: PipelineStage[];
}

export interface StageProbeUpdate {
  stageId: StageId | "all";
  phase: "checking" | "complete";
  status: StageStatus;
  headline: string;
  findings: string[];
  durationMs: number;
}

export type PipelineUIMessage = UIMessage<
  never,
  {
    "pipeline-snapshot": PipelineSnapshot;
    "stage-probe": StageProbeUpdate;
    notification: {
      message: string;
      level: "info" | "warning" | "error" | "success";
    };
  }
>;
