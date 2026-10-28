import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";
import { api, fmt, type Row } from "../api";

export function Title({ title, sub }: { title: string; sub: string }) {
  return (
    <div className="mb-4">
      <h1 className="font-display text-3xl font-semibold">{title}</h1>
      <p className="mt-1 text-sm text-muted">{sub}</p>
    </div>
  );
}

export function Kpis({ items }: { items: [string, ReactNode][] }) {
  return (
    <div className="mb-4 grid gap-3" style={{ gridTemplateColumns: `repeat(${items.length}, minmax(0, 1fr))` }}>
      {items.map(([label, value]) => (
        <div key={label} className="kpi">
          <div className="text-[0.7rem] font-medium tracking-wider text-muted uppercase">{label}</div>
          <div className="mt-1 font-display text-2xl font-semibold">{value}</div>
        </div>
      ))}
    </div>
  );
}

export function Note({ children, tone = "info" }: { children: ReactNode; tone?: "info" | "warn" | "error" | "ok" }) {
  const cls = {
    info: "border-teal/40 bg-teal/10",
    warn: "border-brass/50 bg-brass/10",
    error: "border-danger/50 bg-danger/15",
    ok: "border-ok/50 bg-ok/10",
  }[tone];
  return <div className={`my-3 rounded-md border px-4 py-3 text-sm ${cls}`}>{children}</div>;
}

export function Empty({ children }: { children: ReactNode }) {
  return <Note>{children}</Note>;
}

export function DataTable({ rows, columns, maxHeight = 420 }: { rows: Row[]; columns?: string[]; maxHeight?: number }) {
  const cols = columns ?? (rows.length ? Object.keys(rows[0]) : []);
  if (!rows.length) return <p className="text-sm text-muted">No rows.</p>;
  return (
    <div className="panel overflow-auto" style={{ maxHeight }}>
      <table className="tbl">
        <thead>
          <tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => (
                <td key={c} className="max-w-[28rem] truncate" title={fmt(r[c])}>{fmt(r[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function TablePreview({ layer, tables, initial }: { layer: string; tables: string[]; initial?: string }) {
  const [pick, setPick] = useState(initial ?? tables[0]);
  const q = useQuery({
    queryKey: ["sample", layer, pick],
    queryFn: () => api.get<{ ok: boolean; rows: Row[]; error?: string }>(`/tables/${layer}/${pick}/sample?n=15`),
    enabled: !!pick,
  });
  if (!tables.length) return null;
  return (
    <div className="my-4">
      <div className="mb-2 flex items-center gap-3">
        <span className="text-sm text-muted">Preview</span>
        <select className="input max-w-sm" value={pick} onChange={(e) => setPick(e.target.value)}>
          {tables.map((t) => <option key={t}>{t}</option>)}
        </select>
      </div>
      {q.isLoading && <p className="text-sm text-muted">Loading sample…</p>}
      {q.data?.error && <Note tone="error">{q.data.error}</Note>}
      {q.data?.rows && <DataTable rows={q.data.rows} />}
    </div>
  );
}

export function Tabs({ tabs }: { tabs: [string, ReactNode][] }) {
  const [active, setActive] = useState(0);
  return (
    <div>
      <div className="mb-4 flex gap-1 border-b border-hairline">
        {tabs.map(([label], i) => (
          <button
            key={label}
            onClick={() => setActive(i)}
            className={`cursor-pointer px-4 py-2 text-sm ${i === active ? "border-b-2 border-brass text-foam" : "text-muted hover:text-foam"}`}
          >
            {label}
          </button>
        ))}
      </div>
      {tabs[active][1]}
    </div>
  );
}

export function Loading() {
  return <p className="text-sm text-muted">Loading…</p>;
}
