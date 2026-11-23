import { useEffect, useLayoutEffect, useRef, useState } from "react";

const symbolUrl = new URL("./majlis-symbol.svg", import.meta.url).href;
const pollInterval = 1000;

type Stage = { id: string; title: string; summary: string; labs: string[] };
type Action = {
  id: string;
  stage: string;
  title: string;
  detail: string;
  kind: "run" | "check";
  command: string;
};
type Job = {
  id: string;
  action: string;
  state: "running" | "succeeded" | "failed";
  output: string;
  exitCode: number | null;
};
type ControlSnapshot = {
  mode: "codespace" | "compose";
  stages: Stage[];
  actions: Action[];
  jobs: Job[];
};

const jobLabels = { running: "Running", succeeded: "Succeeded", failed: "Failed" };

async function jsonResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as { error?: string } | null;
    throw new Error(body?.error ?? `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

function numberLabel(index: number) {
  return String(index + 1).padStart(2, "0");
}

function JobStatus({ job }: { job: Job | undefined }) {
  return (
    <span className="lab-job-status" data-state={job?.state}>
      <span aria-hidden="true">{job?.state === "succeeded" ? "✓" : job?.state === "failed" ? "×" : "·"}</span>
      {job ? jobLabels[job.state] : "Not run"}
    </span>
  );
}

function CommandOutput({ job }: { job: Job }) {
  const outputRef = useRef<HTMLPreElement>(null);
  const shouldFollow = useRef(job.state === "running");

  useLayoutEffect(() => {
    const output = outputRef.current;
    if (output && shouldFollow.current) output.scrollTop = output.scrollHeight;
  }, [job.output]);

  return (
    <pre
      ref={outputRef}
      className="lab-output"
      role="region"
      aria-label="Command output"
      tabIndex={0}
      onScroll={({ currentTarget }) => {
        shouldFollow.current = currentTarget.scrollHeight - currentTarget.clientHeight - currentTarget.scrollTop < 24;
      }}
    >
      {job.output || (job.state === "running" ? "Waiting for command output…" : "The command produced no output.")}
    </pre>
  );
}

export function ControlPanel() {
  const [control, setControl] = useState<ControlSnapshot | null>(null);
  const [selectedStageId, setSelectedStageId] = useState("setup");
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(true);
  const [startingAction, setStartingAction] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [startError, setStartError] = useState<string | null>(null);

  const runningJob = control?.jobs.find((job) => job.state === "running");
  const hasRunningJob = Boolean(runningJob);

  useEffect(() => {
    if (!refreshing && !hasRunningJob) return;

    const controller = new AbortController();
    const { signal } = controller;
    let timer: number;

    async function readStatus() {
      let follow = hasRunningJob;
      try {
        const response = await fetch("/api/control", { signal, cache: "no-store" });
        const next = await jsonResponse<ControlSnapshot>(response);
        if (signal.aborted) return;
        follow = next.jobs.some((job) => job.state === "running");
        setControl((current) => (current ? { ...current, jobs: next.jobs } : next));
        setLoadError(null);
      } catch (error) {
        if (!signal.aborted) setLoadError(error instanceof Error ? error.message : "Connection unavailable");
      } finally {
        if (!signal.aborted) {
          setRefreshing(false);
          if (follow) timer = window.setTimeout(readStatus, pollInterval);
        }
      }
    }

    timer = window.setTimeout(readStatus, refreshing ? 0 : pollInterval);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [refreshing, hasRunningJob]);

  const actionsBlocked = refreshing || startingAction !== null || hasRunningJob;

  async function startAction(action: Action) {
    if (actionsBlocked) return;
    setStartingAction(action.id);
    setStartError(null);
    try {
      const response = await fetch("/api/control/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: action.id }),
      });
      const job = await jsonResponse<Job>(response);
      setSelectedJobId(job.id);
      setControl((current) => current && { ...current, jobs: [job, ...current.jobs] });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Connection unavailable";
      setStartError(`Could not start ${action.title}: ${message}`);
      setRefreshing(true);
    } finally {
      setStartingAction(null);
    }
  }

  const stages = control?.stages ?? [];
  const actions = control?.actions ?? [];
  const jobs = control?.jobs ?? [];
  const stage = stages.find(({ id }) => id === selectedStageId) ?? stages[0];
  const viewedJob = jobs.find(({ id }) => id === selectedJobId) ?? runningJob ?? jobs[0];
  const viewedAction = actions.find(({ id }) => id === viewedJob?.action);
  const runningTitle = actions.find(({ id }) => id === runningJob?.action)?.title;
  const runtime = control
    ? control.mode === "codespace" ? "Codespaces · Single container" : "Laptop · Docker Compose"
    : refreshing ? "Connecting…" : "Unavailable";

  return (
    <>
      <header className="lab-sheet-header">
        <div className="lab-title">
          <p className="lab-kicker">Advanced data engineering <span aria-hidden="true">/</span> Airport payments</p>
          <h1>Lab control panel</h1>
          <p>Run the airport payments workflow and inspect the output from each step.</p>
        </div>
        <div className="lab-title-block">
          <div className="lab-brand-lockup">
            <img src={symbolUrl} alt="" width={40} height={40} />
            <div>
              <span>The Majlis of AI</span>
              <span className="lab-brand-line">Academy</span>
            </div>
          </div>
          <dl className="lab-environment">
            <div>
              <dt>Runtime</dt>
              <dd>{runtime}</dd>
            </div>
            <div>
              <dt>Sheet</dt>
              <dd className="lab-sheet-id">T1.0</dd>
            </div>
          </dl>
        </div>
      </header>

      <section className="lab-workflow" aria-labelledby="lab-workflow-title">
        <div className="lab-section-heading">
          <h2 id="lab-workflow-title">Workflow stages</h2>
          <button
            type="button"
            className="lab-button"
            aria-disabled={refreshing || startingAction !== null}
            aria-busy={refreshing}
            onClick={() => {
              if (!refreshing && startingAction === null) setRefreshing(true);
            }}
          >
            Refresh status
          </button>
        </div>
        {loadError && <p className="lab-message lab-error" role="alert">Could not refresh control status: {loadError}</p>}
        {control && (
          <nav aria-label="Workflow stages">
            <ol className="lab-stage-strip">
              {stages.map((item, index) => (
                <li key={item.id}>
                  <button
                    type="button"
                    aria-pressed={item.id === stage?.id}
                    aria-label={item.title}
                    aria-describedby={`lab-stage-labs-${item.id}`}
                    aria-controls="lab-stage-panel"
                    onClick={() => setSelectedStageId(item.id)}
                  >
                    <span className="lab-stage-number" aria-hidden="true">{numberLabel(index)}</span>
                    <span className="lab-stage-title">{item.title}</span>
                    <span className="lab-stage-labs" id={`lab-stage-labs-${item.id}`}>
                      {item.labs.length === 1 ? "Lab" : "Labs"} {item.labs.join(", ")}
                    </span>
                  </button>
                </li>
              ))}
            </ol>
            <p className="lab-scroll-hint">Scroll the strip to see all stages.</p>
          </nav>
        )}
        {!control && refreshing && <p className="lab-message" role="status">Reading lab control status…</p>}
        {startError && <p className="lab-message lab-error" role="alert">{startError}</p>}
      </section>

      {stage && (
        <div className="lab-workbench">
          <section className="lab-actions" id="lab-stage-panel" aria-labelledby="lab-actions-title">
            <div className="lab-stage-heading">
              <p className="lab-kicker">
                Stage {numberLabel(stages.indexOf(stage))} <span aria-hidden="true">/</span>
                {" "}{stage.labs.length === 1 ? "Lab" : "Labs"} {stage.labs.join(", ")}
              </p>
              <h2 id="lab-actions-title">{stage.title}</h2>
              <p>{stage.summary}</p>
            </div>
            <p className="lab-activity" role="status">
              {runningTitle ? `Running: ${runningTitle}. Wait until it finishes to start another step.` : "Run the steps in order."}
            </p>
            <ol className="lab-action-list" role="list">
              {actions.filter((action) => action.stage === stage.id).map((action, index) => {
                const job = jobs.find((item) => item.action === action.id);
                const verb = action.kind === "run" ? "Run" : "Check";
                return (
                  <li key={action.id} className="lab-action">
                    <span className="lab-action-number" aria-hidden="true">{numberLabel(index)}</span>
                    <div className="lab-action-content">
                      <div className="lab-action-heading">
                        <h3>{action.title}</h3>
                        <JobStatus job={job} />
                      </div>
                      <p>{action.detail}</p>
                      <code className="lab-command">{action.command}</code>
                      <div className="lab-action-controls">
                        <button
                          type="button"
                          className="lab-button lab-button-primary"
                          aria-label={`${verb}: ${action.title}`}
                          aria-disabled={actionsBlocked}
                          onClick={() => void startAction(action)}
                        >
                          {startingAction === action.id ? "Starting…" : verb}
                        </button>
                        {job && (
                          <a className="lab-output-link" href="#run-output" onClick={() => setSelectedJobId(job.id)}>
                            View output<span className="lab-visually-hidden">: {action.title}</span>
                          </a>
                        )}
                      </div>
                    </div>
                  </li>
                );
              })}
            </ol>
          </section>

          <section className="lab-console" aria-labelledby="run-output">
            <div className="lab-console-header">
              <div>
                <p className="lab-kicker">Execution log</p>
                <h2 id="run-output" tabIndex={-1}>Run output</h2>
              </div>
              <div className="lab-result" role="status" aria-atomic="true">
                {viewedJob && (
                  <>
                    <JobStatus job={viewedJob} />
                    {viewedJob.exitCode !== null && <span className="lab-exit-code">Exit code {viewedJob.exitCode}</span>}
                  </>
                )}
              </div>
            </div>
            {viewedJob ? (
              <>
                <label className="lab-history">
                  <span>Run history</span>
                  <select value={viewedJob.id} onChange={(event) => setSelectedJobId(event.target.value)}>
                    {jobs.map((job) => (
                      <option key={job.id} value={job.id}>
                        {actions.find(({ id }) => id === job.action)?.title ?? job.action} · {jobLabels[job.state]} · {job.id.slice(-8)}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="lab-console-terminal">
                  {viewedAction && <code className="lab-console-command"><span aria-hidden="true">$ </span>{viewedAction.command}</code>}
                  <CommandOutput key={viewedJob.id} job={viewedJob} />
                </div>
              </>
            ) : (
              <div className="lab-console-empty">
                <p>No runs yet</p>
                <p>Run or check a step to see its output here.</p>
              </div>
            )}
          </section>
        </div>
      )}
    </>
  );
}
