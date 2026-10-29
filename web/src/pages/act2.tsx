import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api, type Graph, type Row, type Status } from "../api";
import { ModelGraph } from "../components/ModelGraph";
import { DataTable, Empty, Kpis, Loading, Note, TablePreview, Tabs, Title } from "../components/ui";
import { cleanupTable, useCleanup } from "./act1";

type Finding = {
  id: string;
  title: string;
  why: string;
  grain?: string;
  sql: string;
  source: string;
  preview: Row[];
  selected: boolean;
  rank?: number;
  headline?: string;
  impact?: string;
};
type Insights = { findings?: Finding[]; ranked_by?: string; skipped_unsupported?: string[] };

export function InsightsPage(_: { status: Status }) {
  const qc = useQueryClient();
  const q = useQuery({ queryKey: ["insights"], queryFn: () => api.get<Insights>("/insights") });
  const [open, setOpen] = useState<string | null>(null);
  const save = useMutation({
    mutationFn: (ids: string[]) => api.put("/insights/selection", { ids }),
    onSuccess: () => qc.invalidateQueries(),
  });
  if (q.isLoading) return <Loading />;
  const findings = q.data?.findings ?? [];
  if (!findings.length) return <Empty>Discovery has not run yet.</Empty>;
  const selected = findings.filter((f) => f.selected).map((f) => f.id);
  const toggle = (id: string) =>
    save.mutate(selected.includes(id) ? selected.filter((s) => s !== id) : [...selected, id]);
  return (
    <>
      <Title title="Insights" sub="The Discovery agent investigated Silver — choose the findings to promote into Gold" />
      <Kpis
        items={[
          ["Findings", findings.length],
          ["Selected For Gold", selected.length],
          ["Ranked By", q.data?.ranked_by ?? "—"],
          ["Skipped (No Data)", q.data?.skipped_unsupported?.length ?? 0],
        ]}
      />
      <div className="space-y-3">
        {findings.map((f) => (
          <div key={f.id} className={`panel p-4 ${f.selected ? "border-l-4 border-l-brass" : "opacity-75"}`}>
            <div className="flex items-start gap-4">
              <input
                type="checkbox"
                className="mt-1.5 h-4 w-4 cursor-pointer accent-[#c9a227]"
                checked={f.selected}
                disabled={save.isPending}
                onChange={() => toggle(f.id)}
                title="Promote to Gold"
              />
              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-semibold">
                    {f.rank ? `${f.rank}. ` : ""}
                    {f.title}
                  </span>
                  <span className="chip">{f.source}</span>
                  {f.grain && <span className="chip">{f.grain}</span>}
                </div>
                {f.headline && <p className="mt-1 text-sm">{f.headline}</p>}
                <p className="mt-1 text-xs text-muted">{f.impact || f.why}</p>
                <button className="mt-2 cursor-pointer text-xs text-teal hover:underline" onClick={() => setOpen(open === f.id ? null : f.id)}>
                  {open === f.id ? "Hide evidence" : "Show evidence"}
                </button>
                {open === f.id && (
                  <div className="mt-2 space-y-2">
                    <DataTable rows={f.preview} maxHeight={260} />
                    <pre className="panel overflow-auto p-3 text-xs">{f.sql}</pre>
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
      <Note>
        Continue runs the <b>Gold builder</b>: dimensions, facts, a wide port-call view, and one aggregate table per selected
        finding.
      </Note>
    </>
  );
}

type Catalog = {
  tables?: { table_name: string; kind: string; grain?: string; description?: string; join_keys?: Record<string, string> }[];
  relationships?: { from: string; to: string }[];
  errors?: string[];
  graph: Graph;
};

export function Gold(_: { status: Status }) {
  const q = useQuery({ queryKey: ["gold"], queryFn: () => api.get<Catalog>("/gold/catalog") });
  const cleanup = useCleanup();
  if (q.isLoading) return <Loading />;
  const tables = q.data?.tables ?? [];
  if (!tables.length) return <Empty>Gold has not been built yet.</Empty>;
  const kinds = tables.map((t) => t.kind);
  const n = (k: string) => kinds.filter((x) => x === k).length;
  return (
    <>
      <Title title="Gold" sub="The AI-ready form of the data: documented dims, facts, aggregates, and their join keys" />
      <Kpis items={[["Dimensions", n("dimension")], ["Facts", n("fact")], ["Finding Aggregates", n("aggregate")], ["Views", n("view")]]} />
      {(q.data?.errors ?? []).map((e) => <Note key={e} tone="warn">{e}</Note>)}
      <Tabs
        tabs={[
          [
            "Mart",
            <>
              <DataTable
                rows={tables.map((t) => ({
                  Table: t.table_name,
                  Kind: t.kind,
                  Grain: t.grain,
                  Description: t.description,
                  Joins: Object.keys(t.join_keys ?? {}).join(", "),
                }))}
              />
              <TablePreview layer="gold" tables={tables.map((t) => t.table_name.replace("gold.", ""))} />
            </>,
          ],
          [
            "Dimensional Model",
            <>
              <ModelGraph graph={q.data!.graph} />
              <div className="mt-3">
                <DataTable rows={(q.data?.relationships ?? []).map((r) => ({ From: r.from, To: r.to }))} />
              </div>
            </>,
          ],
          [
            "Bronze → Gold",
            cleanup.data ? (
              <div className="space-y-4">
                <DataTable
                  rows={cleanup.data.journey.map((j) => ({ Aspect: j.aspect, "Bronze (messy)": j.bronze, Activated: j.activated }))}
                />
                <DataTable rows={cleanupTable(cleanup.data.rows)} />
              </div>
            ) : (
              <Loading />
            ),
          ],
        ]}
      />
    </>
  );
}

type AskResult = {
  ok: boolean;
  blocked?: boolean;
  refused?: boolean;
  message?: string;
  answer_layer?: string;
  answer_source?: string;
  sql?: string;
  explanation?: string;
  rows?: Row[];
  judge?: { score: number; judge_source: string; rationale: string };
  trace?: { layer: string; answered: boolean; reason?: string }[];
};

const EXAMPLES = [
  "Which 5 ports had the longest average anchorage wait (days) in 2021?",
  "How many equipment reassignments did each carrier record?",
  "Who are the top 5 shippers by TEU shipped?",
  "Which vessels changed name or MMSI most often?",
  "What is the weather forecast for Rotterdam next Tuesday?",
];

export function Ask({ status }: { status: Status }) {
  const insights = useQuery({ queryKey: ["insights"], queryFn: () => api.get<Insights>("/insights") });
  const [question, setQuestion] = useState(EXAMPLES[0]);
  const ask = useMutation({ mutationFn: (q: string) => api.post<AskResult>("/ask", { question: q }) });
  const examples = [...(insights.data?.findings ?? []).slice(0, 4).map((f) => `${f.title}?`), ...EXAMPLES];
  const r = ask.data;
  return (
    <>
      <Title title="Ask" sub="Text-to-SQL scoped to activated data: Gold first, then Silver + semantics, otherwise refused" />
      {!status.llm && <Note tone="warn">No LLM configured — Ask only answers questions that match a discovered finding.</Note>}
      <div className="mb-2 flex flex-wrap gap-2">
        {examples.map((e) => (
          <button key={e} className="chip cursor-pointer hover:border-brass" onClick={() => setQuestion(e)}>
            {e}
          </button>
        ))}
      </div>
      <textarea className="input h-24" value={question} onChange={(e) => setQuestion(e.target.value)} />
      <button className="btn-primary mt-2" disabled={ask.isPending || !question.trim()} onClick={() => ask.mutate(question)}>
        {ask.isPending ? "Planning, validating, executing, judging…" : "Ask"}
      </button>
      {ask.error && <Note tone="error">{String(ask.error.message)}</Note>}
      {r && (
        <div className="mt-4">
          {r.blocked ? (
            <Note tone="error">Blocked by governance guard: {r.message}</Note>
          ) : !r.ok ? (
            <Note tone="warn">{r.message ?? "This question cannot be answered from the activated data."}</Note>
          ) : (
            <>
              <div className="mb-2 flex flex-wrap gap-2">
                <span className="chip border-ok/50 bg-ok/10">Answered from {r.answer_layer}</span>
                <span className="chip">Source: {r.answer_source}</span>
                {r.judge && (
                  <span className="chip border-brass/50 bg-brass/10">
                    Judge {r.judge.score.toFixed(2)} · {r.judge.judge_source}
                  </span>
                )}
              </div>
              {r.explanation && <p className="mb-2 text-sm">{r.explanation}</p>}
              <DataTable rows={r.rows ?? []} />
              <pre className="panel mt-3 overflow-auto p-3 text-xs">{r.sql}</pre>
              {r.judge?.rationale && <p className="mt-2 text-xs text-muted">Judge: {r.judge.rationale}</p>}
            </>
          )}
          {!!r.trace?.length && (
            <div className="mt-3">
              <h3 className="mb-1 text-sm text-muted">Cascade trace</h3>
              <DataTable rows={r.trace.map((t) => ({ Layer: t.layer, Answered: t.answered ? "yes" : "no", Reason: t.reason ?? "—" }))} />
            </div>
          )}
        </div>
      )}
    </>
  );
}
