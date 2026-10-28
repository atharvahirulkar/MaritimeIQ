export type Gaps = { semantic: number; data: number; entity: number };

export type ContinuePlan = {
  stage: string | null;
  next: string | null;
  will_run: boolean;
  can_rerun: boolean;
  stage_label: string | null;
  gate_ok: boolean;
  gate_reason: string;
};

export type Step = {
  id: string;
  label: string;
  act: string;
  blurb: string;
  unlocked: boolean;
  stale: boolean;
  continue: ContinuePlan;
};

export type Job = {
  id: string;
  kind: string;
  label: string;
  status: "running" | "done" | "failed";
  started: number;
  finished: number | null;
  error: string | null;
};

export type Status = {
  steps: Step[];
  progress: { pct: number; done: number; total: number };
  gaps: Gaps;
  done: Record<string, boolean>;
  resume_step: string;
  job: Job | null;
  llm: boolean;
};

export type GapItem = {
  id: string;
  kind: "semantic" | "data" | "entity";
  table: string;
  subject: string;
  issue: string;
  suggestion: string;
  score: number;
  grade: string | null;
  rationale: string;
  edit_hint: string;
  count: number | null;
};

export type Row = Record<string, unknown>;
export type Graph = {
  nodes: { id: string; kind: string }[];
  edges: { id: string; source: string; target: string; label: string; type?: string }[];
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      detail = (await res.json()).detail ?? detail;
    } catch {
      /* non-JSON error body */
    }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) }),
  put: <T>(path: string, body: unknown) => request<T>(path, { method: "PUT", body: JSON.stringify(body) }),
};

export const fmt = (v: unknown): string => {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return v.toLocaleString();
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
};
