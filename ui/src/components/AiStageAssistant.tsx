import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport } from "ai";
import { Bot, Send, Sparkles, Square, TerminalSquare, Trash2 } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { PipelineSnapshot, PipelineUIMessage, StageId } from "../types/pipeline";
import { StatusBadge } from "./StatusBadge";

interface AiStageAssistantProps {
  onSnapshotUpdate: (snapshot: PipelineSnapshot) => void;
  onSelectStage: (stageId: StageId) => void;
  pendingStageCheck: { stageId: StageId; nonce: number } | null;
}

const QUICK_PROMPTS: { label: string; prompt: string }[] = [
  { label: "Check all stages", prompt: "Check all workflow stages from ingestion to serve" },
  { label: "Ingestion & Kafka", prompt: "Inspect Stage 1 ingestion, Kafka, and CDC" },
  { label: "Schema & Contract", prompt: "Check Stage 2 Schema Registry and payment contract" },
  { label: "Lakehouse & Quality", prompt: "Check Stage 3 MinIO, Spark silver, and quarantine" },
  { label: "Scoring & Rules", prompt: "Check Stage 4 fraud rules, streaming, and evaluation" },
  { label: "Serve & Monitoring", prompt: "Check Stage 5 Prefect, Prometheus, Grafana, and masking" },
];

function renderInlineFormatting(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return (
        <strong key={i} className="font-semibold text-white">
          {part.slice(2, -2)}
        </strong>
      );
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code key={i} className="rounded bg-slate-900 px-1 py-0.5 font-mono text-[11px] text-orange-200">
          {part.slice(1, -1)}
        </code>
      );
    }
    return part;
  });
}

function FormattedMarkdown({ content }: { content: string }) {
  const lines = content.split("\n");
  return (
    <div className="space-y-1.5 leading-relaxed">
      {lines.map((line, i) => {
        const trimmed = line.trim();
        if (!trimmed) return <div key={i} className="h-1" />;
        if (trimmed.startsWith("### ")) {
          return (
            <h4 key={i} className="font-semibold text-white">
              {trimmed.slice(4)}
            </h4>
          );
        }
        if (trimmed.startsWith("- ")) {
          return (
            <div key={i} className="flex gap-1.5 pl-1">
              <span className="text-orange-400">•</span>
              <span>{renderInlineFormatting(trimmed.slice(2))}</span>
            </div>
          );
        }
        return <p key={i}>{renderInlineFormatting(trimmed)}</p>;
      })}
    </div>
  );
}

export function AiStageAssistant({
  onSnapshotUpdate,
  onSelectStage,
  pendingStageCheck,
}: AiStageAssistantProps) {
  const [input, setInput] = useState("");
  const [toast, setToast] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  const transport = useMemo(() => new DefaultChatTransport({ api: "/api/chat" }), []);

  const { messages, sendMessage, status, stop, setMessages, error } = useChat<PipelineUIMessage>({
    transport,
    onData: (dataPart) => {
      if (dataPart.type === "data-pipeline-snapshot") {
        onSnapshotUpdate(dataPart.data);
      } else if (dataPart.type === "data-notification") {
        setToast(dataPart.data.message);
      }
    },
  });

  useEffect(() => {
    if (!pendingStageCheck) return;
    sendMessage(
      { text: `Run live diagnostic check for stage: ${pendingStageCheck.stageId}` },
      { body: { stageId: pendingStageCheck.stageId } },
    );
  }, [pendingStageCheck, sendMessage]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, status]);

  const busy = status === "submitted" || status === "streaming";

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed || busy) return;
    sendMessage({ text: trimmed });
    setInput("");
  };

  return (
    <aside className="flex h-[680px] flex-col rounded-2xl border border-slate-800/90 bg-slate-900/70 shadow-xl shadow-black/30">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
        <div className="flex items-center gap-2.5">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg border border-orange-500/40 bg-orange-500/15 text-orange-300">
            <Bot className="h-4 w-4" />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-white">AI SDK UI · Stage Inspector</h2>
            <p className="text-[11px] text-slate-400">useChat + streamed data-stage-probe parts</p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => setMessages([])}
          disabled={messages.length === 0 || busy}
          title="Clear transcript"
          className="rounded-lg border border-slate-800 p-1.5 text-slate-400 hover:border-slate-700 hover:text-slate-200 disabled:opacity-40"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </button>
      </div>

      {/* Transient notification pill */}
      {toast && (
        <div className="border-b border-slate-800/80 bg-slate-950/70 px-4 py-1.5 font-mono text-[11px] text-sky-300">
          {toast}
        </div>
      )}

      {/* Quick Prompts */}
      <div className="flex flex-wrap gap-1.5 border-b border-slate-800/80 px-3 py-2.5">
        {QUICK_PROMPTS.map((q) => (
          <button
            key={q.label}
            type="button"
            disabled={busy}
            onClick={() => sendMessage({ text: q.prompt })}
            className="inline-flex items-center gap-1 rounded-full border border-slate-700/80 bg-slate-950/70 px-2.5 py-1 text-[11px] text-slate-300 transition hover:border-orange-500/60 hover:text-orange-200 disabled:opacity-50"
          >
            <Sparkles className="h-2.5 w-2.5 text-orange-400" />
            {q.label}
          </button>
        ))}
      </div>

      {/* Streamed Messages */}
      <div className="min-h-0 flex-1 space-y-3 overflow-y-auto px-4 py-3 text-xs">
        {messages.length === 0 && (
          <div className="flex h-full flex-col items-center justify-center text-center text-slate-400">
            <TerminalSquare className="mb-2 h-7 w-7 text-slate-600" />
            <p className="font-medium text-slate-300">Inspect any workflow stage live</p>
            <p className="mt-1 max-w-[260px] text-[11px] text-slate-500">
              Uses Vercel AI SDK UI (<code className="text-slate-300">useChat</code>) to stream live service probes and
            </p>
          </div>
        )}

        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex flex-col gap-2 ${message.role === "user" ? "items-end" : "items-start"}`}
          >
            {message.parts.map((part, idx) => {
              if (part.type === "data-stage-probe") {
                return (
                  <div
                    key={`${message.id}-probe-${idx}`}
                    className="w-full rounded-xl border border-slate-800 bg-slate-950/90 p-3"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-mono text-[11px] font-semibold text-slate-200">
                        {part.data.headline}
                      </span>
                      <StatusBadge status={part.data.status} />
                    </div>
                    {part.data.findings.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-1">
                        {part.data.findings.map((f) => (
                          <span
                            key={f}
                            className="rounded bg-slate-900 px-1.5 py-0.5 font-mono text-[10px] text-slate-300"
                          >
                            {f}
                          </span>
                        ))}
                      </div>
                    )}
                    <div className="mt-2 flex items-center justify-between font-mono text-[10px] text-slate-500">
                      <span>{part.data.phase === "checking" ? "Probing..." : `Completed in ${part.data.durationMs}ms`}</span>
                      {part.data.stageId !== "all" && (
                        <button
                          type="button"
                          onClick={() => onSelectStage(part.data.stageId as StageId)}
                          className="text-orange-400 hover:underline"
                        >
                          Focus stage &rarr;
                        </button>
                      )}
                    </div>
                  </div>
                );
              }

              if (part.type === "text") {
                return (
                  <div
                    key={`${message.id}-text-${idx}`}
                    className={`max-w-full rounded-xl px-3 py-2 ${
                      message.role === "user"
                        ? "border border-orange-500/40 bg-orange-500/20 text-orange-100"
                        : "border border-slate-700/60 bg-slate-800/70 text-slate-200"
                    }`}
                  >
                    {message.role === "assistant" ? (
                      <FormattedMarkdown content={part.text} />
                    ) : (
                      <span className="whitespace-pre-wrap">{part.text}</span>
                    )}
                  </div>
                );
              }
              return null;
            })}
          </div>
        ))}
        {error && (
          <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 p-2.5 text-[11px] text-rose-200">
            Stream error: {error.message}
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input Bar */}
      <form onSubmit={handleSubmit} className="border-t border-slate-800 p-3">
        <div className="flex items-center gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={busy}
            placeholder="Ask about Kafka, Schema, Spark, CDC, or Serve..."
            className="w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-xs text-slate-100 placeholder:text-slate-500 focus:border-orange-500 focus:outline-none disabled:opacity-60"
          />
          {busy ? (
            <button
              type="button"
              onClick={() => stop()}
              className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-xl border border-rose-500/50 bg-rose-500/15 text-rose-200 hover:bg-rose-500/25"
              title="Stop stream"
            >
              <Square className="h-3.5 w-3.5" />
            </button>
          ) : (
            <button
              type="submit"
              disabled={!input.trim()}
              className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-orange-500 text-slate-950 transition hover:bg-orange-400 disabled:opacity-40"
              title="Send"
            >
              <Send className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </form>
    </aside>
  );
}
