import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { useMemo, useState } from "react";
import { api, type GapItem } from "../api";
import { Kpis, Loading, Note } from "./ui";

type Action = "approve" | "reject" | "edit";
type Draft = { action: Action; value: string };
type Result = { applied: number; results: { id: string; ok: boolean; error: string | null }[] };

const KIND_LABEL: Record<string, string> = { semantic: "Field meaning", data: "Data value", entity: "Entity link" };
const defaultAction = (g: GapItem): Action => (g.suggestion && g.grade !== "wrong" ? "approve" : "edit");

export function HitlGrid({ kinds }: { kinds: GapItem["kind"][] }) {
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: ["gaps", kinds.join(",")],
    queryFn: () => api.get<GapItem[]>(`/gaps?kinds=${kinds.join(",")}`),
  });
  const [drafts, setDrafts] = useState<Record<string, Draft>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [kind, setKind] = useState("all");
  const [table, setTable] = useState("all");
  const [threshold, setThreshold] = useState(0.85);
  const [sorting, setSorting] = useState<SortingState>([]);
  const [resolved, setResolved] = useState(0);

  const items = q.data ?? [];
  const tableOf = (g: GapItem) => g.table.split(/[.,]/)[0];
  const visible = useMemo(
    () => items.filter((g) => (kind === "all" || g.kind === kind) && (table === "all" || tableOf(g) === table)),
    [items, kind, table],
  );
  const draft = (g: GapItem): Draft => drafts[g.id] ?? { action: defaultAction(g), value: "" };
  const setDraft = (id: string, d: Partial<Draft>, g: GapItem) =>
    setDrafts((p) => ({ ...p, [id]: { ...draft(g), ...d } }));

  const apply = useMutation({
    mutationFn: (decisions: { id: string; action: Action; value: string | null }[]) =>
      api.post<Result>("/gaps/decisions", { decisions }),
    onSuccess: (res) => {
      const errs: Record<string, string> = {};
      res.results.forEach((r) => !r.ok && (errs[r.id] = r.error ?? "failed"));
      setErrors(errs);
      setResolved((n) => n + res.applied);
      setDrafts((p) => Object.fromEntries(Object.entries(p).filter(([id]) => errs[id])));
      qc.invalidateQueries();
    },
  });

  const columns = useMemo<ColumnDef<GapItem>[]>(
    () => [
      { header: "Kind", accessorKey: "kind", cell: (c) => KIND_LABEL[c.getValue<string>()] },
      { header: "Where", accessorKey: "table", cell: (c) => <span className="text-xs text-muted">{c.getValue<string>()}</span> },
      {
        header: "Value / Field",
        accessorKey: "subject",
        cell: ({ row: { original: g } }) => (
          <div>
            <b>{g.subject}</b>
            {g.count ? <div className="text-xs text-muted">{g.count.toLocaleString()} rows</div> : null}
          </div>
        ),
      },
      { header: "Issue", accessorKey: "issue", cell: (c) => <span className="text-xs">{c.getValue<string>() || "—"}</span> },
      {
        header: "Agent Suggestion",
        accessorKey: "suggestion",
        cell: ({ row: { original: g } }) => (
          <div className="text-xs">
            {g.suggestion || <span className="text-muted">— none —</span>}
            {g.rationale && <div className="mt-0.5 text-muted italic">{g.rationale}</div>}
          </div>
        ),
      },
      {
        header: "Score",
        accessorKey: "score",
        cell: ({ row: { original: g } }) => (
          <span className={g.grade === "wrong" ? "text-danger" : g.score >= 0.85 ? "text-ok" : "text-brass"}>
            {g.score.toFixed(2)}
          </span>
        ),
      },
      {
        id: "action",
        header: "Action",
        cell: ({ row: { original: g } }) => {
          const d = draft(g);
          return (
            <div className="flex overflow-hidden rounded border border-hairline text-xs">
              {(["approve", "reject", "edit"] as Action[]).map((a) => (
                <button
                  key={a}
                  disabled={a === "approve" && !g.suggestion}
                  onClick={() => setDraft(g.id, { action: a }, g)}
                  className={`cursor-pointer px-2.5 py-1 capitalize disabled:cursor-not-allowed disabled:opacity-30 ${
                    d.action === a ? (a === "reject" ? "bg-danger/70" : a === "edit" ? "bg-brass/70 text-bg0" : "bg-teal text-bg0") : "hover:bg-white/5"
                  }`}
                >
                  {a}
                </button>
              ))}
            </div>
          );
        },
      },
      {
        id: "value",
        header: "Your Value",
        cell: ({ row: { original: g } }) => {
          const d = draft(g);
          return (
            <div className="min-w-48">
              {d.action === "edit" ? (
                <input
                  className="input py-1"
                  placeholder={g.edit_hint}
                  value={d.value}
                  onChange={(e) => setDraft(g.id, { value: e.target.value }, g)}
                />
              ) : (
                <span className="text-xs text-muted">—</span>
              )}
              {errors[g.id] && <div className="mt-1 text-xs text-danger">{errors[g.id]}</div>}
            </div>
          );
        },
      },
    ],
    [drafts, errors],
  );

  const grid = useReactTable({
    data: visible,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 25 } },
    getRowId: (g) => g.id,
  });

  if (q.isLoading) return <Loading />;
  const counts = kinds.map((k) => [`Open ${KIND_LABEL[k]} Gaps`, items.filter((g) => g.kind === k).length] as [string, number]);
  if (!items.length)
    return (
      <>
        <Kpis items={[...counts, ["Resolved This Session", resolved]]} />
        <Note tone="ok">No open gaps here. Continue to the next step.</Note>
      </>
    );

  const page = grid.getRowModel().rows.map((r) => r.original);
  const submit = (rows: GapItem[]) =>
    apply.mutate(rows.map((g) => ({ id: g.id, action: draft(g).action, value: draft(g).value.trim() || null })));
  const bulk = visible.filter((g) => g.suggestion && g.score >= threshold && g.grade !== "wrong");

  return (
    <>
      <Kpis items={[...counts, ["Resolved This Session", resolved]]} />
      <div className="mb-3 flex flex-wrap items-end gap-3">
        {kinds.length > 1 && (
          <label className="text-xs text-muted">
            Kind
            <select className="input mt-1" value={kind} onChange={(e) => setKind(e.target.value)}>
              <option value="all">All</option>
              {kinds.map((k) => <option key={k} value={k}>{KIND_LABEL[k]}</option>)}
            </select>
          </label>
        )}
        <label className="text-xs text-muted">
          Table
          <select className="input mt-1" value={table} onChange={(e) => setTable(e.target.value)}>
            <option value="all">All</option>
            {[...new Set(items.map(tableOf))].sort().map((t) => <option key={t}>{t}</option>)}
          </select>
        </label>
        <label className="text-xs text-muted">
          Bulk-accept score ≥
          <input
            type="number"
            step={0.05}
            min={0.5}
            max={1}
            className="input mt-1 w-24"
            value={threshold}
            onChange={(e) => setThreshold(Number(e.target.value))}
          />
        </label>
        <button
          className="btn"
          disabled={!bulk.length || apply.isPending}
          onClick={() => apply.mutate(bulk.map((g) => ({ id: g.id, action: "approve", value: null })))}
        >
          Accept {bulk.length} agent suggestion(s)
        </button>
      </div>
      <div className="panel overflow-auto">
        <table className="tbl">
          <thead>
            {grid.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((h) => (
                  <th
                    key={h.id}
                    onClick={h.column.getToggleSortingHandler()}
                    className={h.column.getCanSort() ? "cursor-pointer select-none" : ""}
                  >
                    {flexRender(h.column.columnDef.header, h.getContext())}
                    {{ asc: " ↑", desc: " ↓" }[h.column.getIsSorted() as string] ?? ""}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {grid.getRowModel().rows.map((r) => (
              <tr key={r.id} className={errors[r.id] ? "bg-danger/5" : ""}>
                {r.getVisibleCells().map((c) => (
                  <td key={c.id}>{flexRender(c.column.columnDef.cell, c.getContext())}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="mt-3 flex flex-wrap items-center gap-3">
        <button className="btn-primary" disabled={apply.isPending || !page.length} onClick={() => submit(page)}>
          {apply.isPending ? "Applying…" : `Apply ${page.length} decision(s) on this page`}
        </button>
        <span className="text-xs text-muted">
          Page {grid.getState().pagination.pageIndex + 1} of {grid.getPageCount()} · {visible.length} gap(s)
        </span>
        <button className="btn py-1" disabled={!grid.getCanPreviousPage()} onClick={() => grid.previousPage()}>Prev</button>
        <button className="btn py-1" disabled={!grid.getCanNextPage()} onClick={() => grid.nextPage()}>Next</button>
      </div>
      {apply.error && <Note tone="error">{String(apply.error.message)}</Note>}
      {!!Object.keys(errors).length && (
        <Note tone="warn">{Object.keys(errors).length} decision(s) were not applied — see the highlighted rows.</Note>
      )}
      <p className="mt-2 text-xs text-muted">
        Approve accepts the agent's suggestion, Reject leaves the value unmapped (or keeps the entity separate), Edit
        applies your own value. Decisions are replayed on every later run.
      </p>
    </>
  );
}
