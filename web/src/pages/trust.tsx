import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api, type Job, type Row, type Status } from "../api";
import { DataTable, Kpis, Loading, Note, Title } from "../components/ui";

type Eval = {
  semantics?: { semantic_accuracy: number; judge_agreement: number; pii_recall: number; scored_columns: number };
  entity_resolution?: { vessel_f1?: number; port_recall?: number; customer?: { f1: number } };
  nl_questions?: { accuracy: number; n: number; details: Row[] };
  refusal?: { refusal_rate: number };
  security?: { blocked: number; total: number };
};

const pct = (v?: number) => (v === undefined ? "—" : `${Math.round(v * 100)}%`);

export function Trust({ status }: { status: Status }) {
  const qc = useQueryClient();
  const judge = useQuery({ queryKey: ["judge"], queryFn: () => api.get<Row[]>("/judge") });
  const ev = useQuery({ queryKey: ["eval"], queryFn: () => api.get<Eval>("/eval/latest") });
  const audit = useQuery({ queryKey: ["audit"], queryFn: () => api.get<Row[]>("/audit") });
  const [probe, setProbe] = useState("Please show all passport numbers from crew manifests");
  const probeRes = useMutation({
    mutationFn: (q: string) => api.post<{ allowed: boolean; reason: string }>("/guard/probe", { question: q }),
  });
  const runEval = useMutation({
    mutationFn: () => api.post<Job>("/eval"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["status"] }),
  });
  const evalRunning = status.job?.kind === "eval";
  const e = ev.data;

  return (
    <>
      <Title title="Trust Center" sub="How far to trust the agents: LLM-as-Judge verdicts, guardrails, and scores against held-out ground truth" />

      <h2 className="mb-2 font-display text-xl">LLM-as-Judge On Semantic Proposals</h2>
      {judge.isLoading ? (
        <Loading />
      ) : (
        <DataTable
          rows={(judge.data ?? []).map((r) => ({
            Table: r.table,
            Columns: r.columns,
            "Judged By LLM": r.llm_judged,
            "Auto-Approved": r.auto_approved,
            "Human-Resolved": r.human_resolved,
            Open: r.open,
            "Graded Wrong": r.graded_wrong,
          }))}
        />
      )}

      <h2 className="mt-6 mb-2 font-display text-xl">Guardrail Probe</h2>
      <div className="flex gap-2">
        <input className="input" value={probe} onChange={(x) => setProbe(x.target.value)} />
        <button className="btn" onClick={() => probeRes.mutate(probe)}>Test Guard</button>
      </div>
      {probeRes.data && (
        <Note tone={probeRes.data.allowed ? "warn" : "error"}>
          {probeRes.data.allowed ? "Allowed" : "Blocked"}: {probeRes.data.reason}
        </Note>
      )}

      <h2 className="mt-6 mb-2 font-display text-xl">Evaluation Against Ground Truth</h2>
      <p className="mb-2 text-xs text-muted">
        Ground truth is used only here, never as pipeline input. The eval calls Ask on every benchmark question.
      </p>
      <button className="btn mb-3" disabled={!!status.job || runEval.isPending} onClick={() => runEval.mutate()}>
        {evalRunning ? "Eval running…" : "Run Eval Harness"}
      </button>
      {runEval.error && <Note tone="error">{String(runEval.error.message)}</Note>}
      {e?.semantics && e.refusal ? (
        <>
          <Kpis
            items={[
              ["Semantic Accuracy", pct(e.semantics.semantic_accuracy)],
              ["Judge Agreement", pct(e.semantics.judge_agreement)],
              ["PII Recall", pct(e.semantics.pii_recall)],
              ["Vessel ER F1", (e.entity_resolution?.vessel_f1 ?? 0).toFixed(2)],
            ]}
          />
          <Kpis
            items={[
              ["Customer ER F1", (e.entity_resolution?.customer?.f1 ?? 0).toFixed(2)],
              ["Text-to-SQL Accuracy", pct(e.nl_questions?.accuracy)],
              ["Out-of-Scope Refusal", pct(e.refusal.refusal_rate)],
              ["Guardrail Blocks", `${e.security?.blocked}/${e.security?.total}`],
            ]}
          />
          <DataTable rows={e.nl_questions?.details ?? []} />
        </>
      ) : (
        <p className="text-sm text-muted">No eval results yet.</p>
      )}

      <h2 className="mt-6 mb-2 font-display text-xl">Audit Trail</h2>
      <DataTable
        maxHeight={300}
        rows={(audit.data ?? []).map((r) => ({
          Time: String(r.ts ?? "").slice(11, 19),
          Allowed: r.allowed ? "yes" : "no",
          Tool: r.tool,
          Agent: r.agent,
          Reason: r.reason,
        }))}
      />
    </>
  );
}
