import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { api, fmt, type Graph, type Row, type Status } from "../api";
import { HitlGrid } from "../components/HitlGrid";
import { ModelGraph } from "../components/ModelGraph";
import { DataTable, Empty, Kpis, Loading, Note, TablePreview, Tabs, Title } from "../components/ui";

type Table = { name: string; rows?: number };

const useTables = (layer: string, counts = false) =>
  useQuery({
    queryKey: ["tables", layer, counts],
    queryFn: () => api.get<Table[]>(`/tables/${layer}?counts=${counts}`),
  });

export function Sources(_: { status: Status }) {
  const q = useQuery({
    queryKey: ["sources"],
    queryFn: () =>
      api.get<{ source: string; files: number; formats: string; size_mb: number; bronze: string[] }[]>("/sources"),
  });
  return (
    <>
      <Title title="Sources" sub="Pre-Bronze: the raw feeds exactly as they arrive — CSV, JSON, XML, fixed-width, EDI, Excel" />
      {q.isLoading ? (
        <Loading />
      ) : (
        <DataTable
          rows={(q.data ?? []).map((r) => ({
            Source: r.source,
            Files: r.files,
            Formats: r.formats,
            "Size (MB)": r.size_mb,
            "Landed In Bronze": r.bronze.join(", ") || "—",
          }))}
        />
      )}
      <p className="mt-2 text-xs text-muted">
        Source root <code>data/hackathon/sources/</code> is read-only and never modified.
      </p>
      <Note>
        Continue runs the <b>Ingestion agent</b>: it parses every format and lands each feed as a raw Bronze Delta table
        with lineage columns. Nothing is cleaned yet.
      </Note>
    </>
  );
}

export function Bronze(_: { status: Status }) {
  const q = useTables("bronze", true);
  const tables = q.data ?? [];
  const total = tables.reduce((a, t) => a + (t.rows ?? 0), 0);
  return (
    <>
      <Title title="Bronze" sub="Raw landed tables — duplicated, inconsistent, undocumented. Not usable by AI yet." />
      {q.isLoading ? (
        <Loading />
      ) : !tables.length ? (
        <Empty>No Bronze tables yet. Go back to Sources and Continue.</Empty>
      ) : (
        <>
          <Kpis items={[["Bronze Tables", tables.length], ["Raw Rows", total.toLocaleString()], ["Documented Fields", 0]]} />
          <DataTable rows={tables.map((t) => ({ Table: t.name, Rows: t.rows }))} />
          <TablePreview layer="bronze" tables={tables.map((t) => t.name)} />
        </>
      )}
      <Note>
        Continue runs the <b>Context agent</b>: it profiles every column and asks the LLM what each field means, its type,
        PII class, code values, and join hints. The <b>LLM-as-Judge</b> then grades each proposal.
      </Note>
    </>
  );
}

type Column = {
  name: string;
  meaning?: string;
  data_type_hint?: string;
  pii?: boolean;
  pii_class?: string | null;
  code_map?: Record<string, string>;
  grade?: string;
  score?: number;
  judge_rationale?: string;
  status?: string;
};
type Semantics = {
  proposals: Record<string, { columns: Column[]; joins?: { left: string; right_entity: string }[] }>;
  learning: { source1?: string; source1_needs_review?: number; source2?: string; source2_needs_review?: number };
};

export function SemanticsPage(_: { status: Status }) {
  const q = useQuery({ queryKey: ["semantics"], queryFn: () => api.get<Semantics>("/semantics") });
  const [table, setTable] = useState<string | null>(null);
  if (q.isLoading) return <Loading />;
  const proposals = q.data?.proposals ?? {};
  const names = Object.keys(proposals).sort();
  if (!names.length) return <Empty>Semantics have not run yet.</Empty>;
  const cols = names.flatMap((t) => proposals[t].columns ?? []);
  const count = (s: string) => cols.filter((c) => c.status === s).length;
  const pick = table ?? names[0];
  const l = q.data!.learning;
  return (
    <>
      <Title title="Semantics" sub="LLM-proposed field meanings, graded by an LLM-as-Judge that only sees masked profiles" />
      <Kpis
        items={[
          ["Fields", cols.length],
          ["Auto-Approved", count("auto_approved")],
          ["Needs Review", count("needs_review")],
          ["Human-Resolved", count("human_approved") + count("rejected")],
        ]}
      />
      {l.source1 && (
        <p className="mb-3 text-xs text-muted">
          Context learning: <code>{l.source1}</code> needed {l.source1_needs_review} reviews; <code>{l.source2}</code>{" "}
          (proposed with those corrections as context) needed {l.source2_needs_review}.
        </p>
      )}
      <select className="input mb-3 max-w-sm" value={pick} onChange={(e) => setTable(e.target.value)}>
        {names.map((n) => <option key={n}>{n}</option>)}
      </select>
      <DataTable
        maxHeight={520}
        rows={(proposals[pick].columns ?? []).map((c) => ({
          Field: c.name,
          Meaning: c.meaning,
          Type: c.data_type_hint,
          PII: c.pii_class ?? (c.pii ? "yes" : ""),
          Codes: Object.entries(c.code_map ?? {})
            .slice(0, 4)
            .map(([k, v]) => `${k}=${v}`)
            .join(", "),
          Judge: `${c.grade ?? "—"} · ${(c.score ?? 0).toFixed(2)}`,
          "Judge Rationale": c.judge_rationale,
          Status: (c.status ?? "").replace("_", " "),
        }))}
      />
      {!!proposals[pick].joins?.length && (
        <p className="mt-2 text-xs text-muted">
          Join hints: {proposals[pick].joins!.map((j) => `${j.left} → ${j.right_entity}`).join(" · ")}
        </p>
      )}
      <Note>
        Continue runs the <b>Quality agent</b>: it scans Bronze for duplicates, sparse columns, port values that don't
        resolve, unmapped event codes, and invalid IMOs.
      </Note>
    </>
  );
}

type QualityTable = {
  rows: number;
  duplicates: number;
  sparse_columns: { column: string; null_pct: number }[];
  port_columns: string[];
  port_values: { resolved: number; unresolved: number };
  unmapped_codes: string[];
  invalid_imo: number;
};

export function Quality(_: { status: Status }) {
  const q = useQuery({
    queryKey: ["quality"],
    queryFn: () => api.get<{ summary?: Record<string, number>; tables?: Record<string, QualityTable> }>("/quality"),
  });
  if (q.isLoading) return <Loading />;
  const s = q.data?.summary;
  if (!s) return <Empty>Quality scan has not run yet.</Empty>;
  return (
    <>
      <Title title="Quality" sub="What is broken in Bronze — auto-fixable issues vs gaps that need a human" />
      <Kpis
        items={[
          ["Duplicate Rows", fmt(s.duplicates)],
          ["Unresolved Port Values", s.unresolved_port_values],
          ["Unmapped Codes", s.unmapped_codes],
          ["Gaps For You", s.gaps_raised],
        ]}
      />
      <DataTable
        rows={Object.entries(q.data?.tables ?? {}).map(([t, r]) => ({
          Table: t,
          Rows: r.rows,
          Duplicates: r.duplicates,
          "Sparse Columns": r.sparse_columns.map((c) => `${c.column} (${Math.round(c.null_pct * 100)}%)`).join(", ") || "—",
          "Port Columns": r.port_columns.join(", ") || "—",
          "Port Rows Unresolved": r.port_values?.unresolved ?? 0,
          "Unmapped Codes": r.unmapped_codes.join(", ") || "—",
          "Invalid IMO": r.invalid_imo,
        }))}
      />
      <Note>
        <b>Auto-fixed in Silver:</b> duplicates, timestamp formats and time zones, IMO formatting, null-island positions,
        superseded B/L amendments, PII masking.
        <br />
        <b>Needs you:</b> port references and event codes the agents could not resolve with confidence, and field meanings
        the Judge flagged.
      </Note>
    </>
  );
}

export function Hitl(_: { status: Status }) {
  return (
    <>
      <Title title="HITL Gaps" sub="Guide the agents where they are unsure — your answers become mappings replayed on every run" />
      <HitlGrid kinds={["semantic", "data"]} />
    </>
  );
}

export function Connect(_: { status: Status }) {
  const q = useTables("silver", true);
  const masters = (q.data ?? []).filter((t) => /^master_|_aliases$|_map$/.test(t.name));
  const n = (name: string) => masters.find((t) => t.name === name)?.rows ?? 0;
  return (
    <>
      <Title title="Connect" sub="Entity resolution links the same vessel, port, and customer across disparate sources" />
      {q.isLoading ? (
        <Loading />
      ) : !masters.length ? (
        <Empty>Entity resolution has not run yet.</Empty>
      ) : (
        <>
          <Kpis
            items={[
              ["Vessel Masters", fmt(n("master_vessel"))],
              ["Port Masters", fmt(n("master_port"))],
              ["Customer Masters", fmt(n("master_customer"))],
              ["Vessel Aliases", fmt(n("vessel_aliases"))],
            ]}
          />
          <h2 className="mb-2 font-display text-xl">Uncertain Links — Your Call</h2>
          <HitlGrid kinds={["entity"]} />
          <TablePreview layer="silver" tables={masters.map((t) => t.name)} initial="master_vessel" />
        </>
      )}
    </>
  );
}

type Cleanup = {
  rows: { table: string; source: string; bronze_rows: number; silver_rows: number; fixes: Row; remaining: Row }[];
  journey: Row[];
};
const kv = (o: Row) =>
  Object.entries(o)
    .map(([k, v]) => `${k.replaceAll("_", " ")}: ${fmt(v)}`)
    .join(", ") || "—";

export const cleanupTable = (rows: Cleanup["rows"]) =>
  rows.map((r) => ({
    "Silver Table": r.table,
    "Bronze Source": r.source,
    "Bronze Rows": r.bronze_rows,
    "Silver Rows": r.silver_rows,
    "Fixes Applied": kv(r.fixes),
    "Still Open": kv(r.remaining),
  }));

export function useCleanup() {
  return useQuery({ queryKey: ["cleanup"], queryFn: () => api.get<Cleanup>("/cleanup") });
}

export function Silver(_: { status: Status }) {
  const cleanup = useCleanup();
  const tables = useTables("silver");
  const model = useQuery({
    queryKey: ["silver-model"],
    queryFn: () =>
      api.get<{
        graph: Graph;
        relationships: { from: string; to: string; type: string }[];
        tables: Record<string, { source?: string; description?: string; columns?: Record<string, string> }>;
      }>("/silver/model"),
  });
  const [pick, setPick] = useState<string | null>(null);
  if (cleanup.isLoading) return <Loading />;
  if (!cleanup.data?.rows.length) return <Empty>Silver has not been built yet.</Empty>;
  const names = (tables.data ?? []).map((t) => t.name);
  const current = pick ?? names[0];
  const meta = model.data?.tables[current ?? ""];
  return (
    <>
      <Title title="Silver" sub="Clean, typed, PII-masked tables with a semantic layer that connects them" />
      <Tabs
        tabs={[
          [
            "Cleanup Report",
            <>
              <DataTable rows={cleanupTable(cleanup.data.rows)} />
              <p className="mt-2 text-xs text-muted">Every fix the Transform agent applied, and what is still open.</p>
            </>,
          ],
          [
            "Tables",
            <>
              <select className="input mb-2 max-w-sm" value={current} onChange={(e) => setPick(e.target.value)}>
                {names.map((n) => <option key={n}>{n}</option>)}
              </select>
              {meta?.description && (
                <p className="mb-2 text-xs text-muted">
                  {meta.description} — from <code>bronze.{meta.source}</code>
                </p>
              )}
              {current && <TablePreview key={current} layer="silver" tables={[current]} />}
              {meta?.columns && (
                <DataTable rows={Object.entries(meta.columns).map(([c, d]) => ({ Column: c, Meaning: d || "—" }))} />
              )}
            </>,
          ],
          [
            "Semantic Layer",
            model.data ? (
              <>
                <ModelGraph graph={model.data.graph} />
                <div className="mt-3">
                  <DataTable rows={model.data.relationships.map((r) => ({ From: r.from, To: r.to, Type: r.type }))} />
                </div>
              </>
            ) : (
              <Loading />
            ),
          ],
        ]}
      />
      <Note>
        Continue runs the <b>Discovery agent</b>: it investigates Silver for the metrics that matter most to the enterprise
        and ranks them.
      </Note>
    </>
  );
}
