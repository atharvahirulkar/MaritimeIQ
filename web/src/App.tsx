import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Navigate, Route, Routes, useNavigate, useParams } from "react-router-dom";
import { api, type Job, type Status, type Step } from "./api";
import { AgentFeed, useFeed } from "./components/AgentFeed";
import { Note } from "./components/ui";
import { PAGES } from "./pages";

export function useStatus() {
  return useQuery({
    queryKey: ["status"],
    queryFn: () => api.get<Status>("/status"),
    refetchInterval: (q) => (q.state.data?.job ? 1500 : 10000),
  });
}

export default function App() {
  const status = useStatus();
  if (status.isLoading) return <div className="p-10 text-muted">Connecting to MaritimeIQ API…</div>;
  if (status.error || !status.data)
    return <div className="p-10 text-danger">API unavailable: {String(status.error)}. Start it with `make api`.</div>;
  return (
    <Routes>
      <Route path="/" element={<Navigate to={`/${status.data.resume_step}`} replace />} />
      <Route path="/:step" element={<Shell status={status.data} />} />
    </Routes>
  );
}

function Rail({ status, current }: { status: Status; current: string }) {
  const navigate = useNavigate();
  const extra: Record<string, string> = {
    hitl: `${status.gaps.semantic + status.gaps.data} open`,
    connect: `${status.gaps.entity} open`,
  };
  return (
    <nav className="relative space-y-1.5">
      <div className="absolute top-2 bottom-2 left-[9px] w-0.5 bg-gradient-to-b from-teal via-brass/40 to-transparent" />
      {status.steps.map((s) => {
        const active = s.id === current;
        const state = active ? "Current" : !s.unlocked ? "Locked" : s.stale ? "Needs re-run" : "Ready";
        return (
          <button
            key={s.id}
            disabled={!s.unlocked && !active}
            onClick={() => navigate(`/${s.id}`)}
            className={`relative block w-full cursor-pointer rounded border-l-[3px] py-2 pr-2 pl-5 text-left text-sm transition disabled:cursor-not-allowed ${
              active
                ? "border-brass bg-brass/10 ring-1 ring-hairline"
                : s.unlocked
                  ? "border-ok bg-ok/5 hover:bg-ok/10"
                  : "border-muted/25 bg-panel/50 opacity-60"
            }`}
          >
            <div className="font-semibold">{s.label}</div>
            <div className={`text-xs ${s.stale && !active ? "text-brass" : "text-muted"}`}>
              {s.act !== "side" ? `Act ${s.act} · ` : ""}
              {state}
              {s.unlocked && extra[s.id] ? ` · ${extra[s.id]}` : ""}
            </div>
          </button>
        );
      })}
    </nav>
  );
}

function ContinueBar({ step, status, onDone }: { step: Step; status: Status; onDone: (next: string) => void }) {
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [jobId, setJobId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const plan = step.continue;
  const idx = status.steps.findIndex((s) => s.id === step.id);
  const nextLabel = status.steps.find((s) => s.id === plan.next)?.label;

  const job = useQuery({
    queryKey: ["job", jobId],
    queryFn: () => api.get<Job>(`/jobs/${jobId}`),
    enabled: !!jobId,
    refetchInterval: (q) => (q.state.data?.status === "running" ? 1200 : false),
  });

  useEffect(() => {
    const j = job.data;
    if (!j || j.status === "running") return;
    setJobId(null);
    qc.invalidateQueries();
    if (j.status === "failed") setError(`${j.label} failed: ${j.error}`);
    else if (plan.next) onDone(plan.next);
  }, [job.data, qc, onDone, plan.next]);

  const running = !!jobId || !!status.job;
  const start = async (force: boolean) => {
    setError(null);
    if (!plan.next) return;
    if (!plan.gate_ok) return;
    if (!(plan.will_run || force) || !plan.stage) return onDone(plan.next);
    try {
      const j = await api.post<Job>(`/stages/${plan.stage}/run`);
      setJobId(j.id);
      qc.invalidateQueries({ queryKey: ["status"] });
    } catch (e) {
      setError(String((e as Error).message));
    }
  };

  const elapsed = status.job ? Math.round(Date.now() / 1000 - status.job.started) : 0;
  return (
    <div className="mt-8 border-t border-hairline pt-4">
      {error && <Note tone="error">{error}</Note>}
      {running && (
        <Note tone="warn">
          <span className="mr-2 inline-block h-2 w-2 animate-pulse rounded-full bg-brass" />
          {status.job?.label ?? "Stage"} running{elapsed ? ` · ${elapsed}s` : ""} — follow progress in the Agent Feed.
        </Note>
      )}
      <div className="flex flex-wrap items-center gap-3">
        <button className="btn" disabled={idx <= 0 || running} onClick={() => navigate(`/${status.steps[idx - 1].id}`)}>
          Back
        </button>
        <button className="btn-primary" disabled={!plan.next || running || !plan.gate_ok} onClick={() => start(false)}>
          Continue
        </button>
        {plan.can_rerun && !plan.will_run && (
          <button className="btn" disabled={running || !plan.gate_ok} onClick={() => start(true)}>
            Re-run {plan.stage_label}
          </button>
        )}
        <span className="text-sm text-muted">
          {!plan.next
            ? "End of the walkthrough."
            : !plan.gate_ok
              ? `Blocked: ${plan.gate_reason}.`
              : plan.will_run
                ? `Continue runs ${plan.stage_label}, then opens ${nextLabel}.`
                : `Continue opens ${nextLabel}.`}
        </span>
      </div>
    </div>
  );
}

function ResetButton() {
  const [confirm, setConfirm] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const qc = useQueryClient();
  const navigate = useNavigate();
  const run = async () => {
    try {
      const r = await api.post<{ walk_step: string; bronze_tables: number; backup: string }>("/reset");
      setMsg(`Reset done. Bronze kept (${r.bronze_tables} tables); previous state saved to ${r.backup}.`);
      setConfirm(false);
      await qc.invalidateQueries();
      navigate(`/${r.walk_step}`);
    } catch (e) {
      setMsg(String((e as Error).message));
    }
  };
  return (
    <div className="mt-6 border-t border-hairline pt-4 text-sm">
      {!confirm ? (
        <button className="btn w-full" onClick={() => setConfirm(true)}>Hard Reset</button>
      ) : (
        <div className="space-y-2">
          <p className="text-xs text-muted">
            Clears Silver, Gold, and all context (moved to a backup folder). Keeps <code>data/</code> and Bronze.
          </p>
          <div className="flex gap-2">
            <button className="btn-primary flex-1" onClick={run}>Confirm</button>
            <button className="btn flex-1" onClick={() => setConfirm(false)}>Cancel</button>
          </div>
        </div>
      )}
      {msg && <p className="mt-2 text-xs text-muted">{msg}</p>}
    </div>
  );
}

function Shell({ status }: { status: Status }) {
  const { step: stepId = "sources" } = useParams();
  const navigate = useNavigate();
  const [showFeed, setShowFeed] = useState(() => localStorage.getItem("mq-feed") === "1");
  const events = useFeed();
  const step = status.steps.find((s) => s.id === stepId);
  const qc = useQueryClient();
  const lastJob = useRef<string | null>(null);

  useEffect(() => {
    const id = status.job?.id ?? null;
    if (lastJob.current && !id) qc.invalidateQueries();
    lastJob.current = id;
  }, [status.job, qc]);

  useEffect(() => {
    if (step) api.put("/session", { walk_step: step.id }).catch(() => undefined);
  }, [step]);
  useEffect(() => localStorage.setItem("mq-feed", showFeed ? "1" : "0"), [showFeed]);

  if (!step) return <Navigate to={`/${status.resume_step}`} replace />;
  const Page = PAGES[step.id];
  const g = status.gaps;

  return (
    <div className="mx-auto flex min-h-full max-w-[1600px] flex-col px-5 py-4">
      <header className="panel mb-3 flex items-center justify-between border-b-2 border-b-brass px-5 py-3">
        <span className="font-display text-3xl font-bold tracking-wide">MaritimeIQ</span>
        <div className="flex items-center gap-4 text-xs text-muted">
          <span className={`chip ${status.llm ? "" : "border-brass/50 bg-brass/10"}`}>LLM {status.llm ? "on" : "off"}</span>
          <label className="flex cursor-pointer items-center gap-2">
            <input type="checkbox" checked={showFeed} onChange={(e) => setShowFeed(e.target.checked)} />
            Agent Feed
          </label>
        </div>
      </header>
      <div className="mb-1 h-1.5 overflow-hidden rounded bg-bg2">
        <div className="h-full bg-gradient-to-r from-teal to-brass transition-all" style={{ width: `${status.progress.pct}%` }} />
      </div>
      <p className="mb-4 text-xs text-muted">
        Activation {status.progress.pct}% ({status.progress.done}/{status.progress.total} milestones) · open gaps: {g.semantic}{" "}
        semantic · {g.data} data · {g.entity} entity
      </p>
      <div className="flex flex-1 gap-5">
        <aside className="w-56 shrink-0">
          <Rail status={status} current={step.id} />
          <ResetButton />
        </aside>
        <main className="min-w-0 flex-1">
          <Page status={status} />
          <ContinueBar key={step.id} step={step} status={status} onDone={(next) => navigate(`/${next}`)} />
        </main>
        {showFeed && (
          <aside className="sticky top-4 h-[calc(100vh-7rem)] w-80 shrink-0">
            <AgentFeed events={events} />
          </aside>
        )}
      </div>
    </div>
  );
}
