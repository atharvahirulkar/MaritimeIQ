"""Act 2: investigate Silver + semantics → propose a Gold mart + KPI findings → materialize on human approval."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

import yaml
from pyspark.sql import SparkSession

from maritimeiq.agents.judge import JudgeAgent
from maritimeiq.agents.llm import LLM
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.governance.sql_guard import validate_select
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONTEXT, GOLD, SILVER, ensure_lakehouse_dirs

GOLD_PLAN_PATH = CONTEXT / "gold_plan.json"
GOLD_CATALOG_PATH = CONTEXT / "gold_catalog.yaml"

# Multi-terminal port complexes analysts report as one gateway
PORT_COMPLEXES = {"LA/LB": ["USLAX", "USLGB"]}

PORT_CALLS_SQL = """
WITH e AS (
  SELECT coalesce(master_vessel_id, concat('NAME:', vessel_name_norm)) AS vessel_key,
         master_vessel_id, master_port, agency, voyage, event_code, event_ts
  FROM silver_port_agent_events
  WHERE event_ts IS NOT NULL AND master_port IS NOT NULL AND event_code <> 'ETA'
), f AS (
  -- A new call starts after a departure / omission, on a fresh arrival after berthing,
  -- or after a long silence (covers calls whose SLD message was lost).
  SELECT *, CASE
      WHEN lag(event_code) OVER w IS NULL THEN 1
      WHEN lag(event_code) OVER w IN ('SLD', 'OMIT') THEN 1
      WHEN event_code IN ('ANC', 'BRT') AND lag(event_code) OVER w IN ('BRT', 'COM', 'CMP') THEN 1
      WHEN unix_timestamp(event_ts) - unix_timestamp(lag(event_ts) OVER w) > 86400 * 45 THEN 1
      ELSE 0 END AS new_call
  FROM e WINDOW w AS (PARTITION BY vessel_key, master_port ORDER BY event_ts)
), s AS (
  SELECT *, sum(new_call) OVER (PARTITION BY vessel_key, master_port ORDER BY event_ts ROWS UNBOUNDED PRECEDING) AS call_seq
  FROM f
), calls AS (
  SELECT vessel_key, max(master_vessel_id) AS master_vessel_id, master_port, call_seq,
         max(agency) AS agency, max(voyage) AS voyage,
         min(CASE WHEN event_code = 'ANC' THEN event_ts END) AS anc_ts,
         min(CASE WHEN event_code = 'BRT' THEN event_ts END) AS atb_ts,
         max(CASE WHEN event_code = 'SLD' THEN event_ts END) AS atd_ts,
         max(CASE WHEN event_code = 'OMIT' THEN 1 ELSE 0 END) AS omitted
  FROM s GROUP BY vessel_key, master_port, call_seq
)
SELECT concat(vessel_key, '|', master_port, '|', call_seq) AS call_id,
       master_vessel_id, vessel_key, master_port, agency, voyage, anc_ts,
       coalesce(least(anc_ts, atb_ts), atb_ts) AS ata_ts, atb_ts, atd_ts,
       anc_ts IS NOT NULL AS anchored,
       (unix_timestamp(atb_ts) - unix_timestamp(coalesce(least(anc_ts, atb_ts), atb_ts))) / 3600.0 AS wait_h
FROM calls
WHERE omitted = 0 AND atb_ts IS NOT NULL
"""

CONTAINER_EVENTS_SQL = """
SELECT container_id, carrier, event_code, empty_indicator, master_port, region, booking_ref, bl_number,
       event_ts, master_vessel_id, voyage, teu
FROM (
  SELECT *, row_number() OVER (PARTITION BY container_id, event_code, event_ts ORDER BY feed_format) AS rn
  FROM silver_container_events WHERE event_ts IS NOT NULL
) WHERE rn = 1
"""

SHIPMENTS_SQL = """
SELECT bl_number, booking_ref, carrier, container_id, teu, issue_date, pol, pod,
       shipper_master_id, consignee_master_id, master_vessel_id, voyage
FROM (
  SELECT *, row_number() OVER (PARTITION BY bl_number, container_id ORDER BY bl_version) AS rn
  FROM silver_bills_of_lading WHERE bl_version = 1
) WHERE rn = 1
"""

SCHEDULE_ADHERENCE_SQL = """
WITH sch AS (
  SELECT carrier, master_vessel_id, master_port, voyage, min(eta_ts) AS planned_eta_ts
  FROM silver_carrier_schedules
  WHERE status <> 'OMITTED' AND master_vessel_id IS NOT NULL AND eta_ts IS NOT NULL
  GROUP BY carrier, master_vessel_id, master_port, voyage
)
SELECT sch.carrier, c.call_id, c.master_vessel_id, c.master_port, c.voyage, sch.planned_eta_ts, c.ata_ts,
       (unix_timestamp(c.ata_ts) - unix_timestamp(sch.planned_eta_ts)) / 3600.0 AS eta_delta_h,
       abs(unix_timestamp(c.ata_ts) - unix_timestamp(sch.planned_eta_ts)) <= 86400 AS on_time_24h
FROM sch JOIN stage_fact_port_call c
  ON sch.master_vessel_id = c.master_vessel_id AND sch.master_port = c.master_port AND sch.voyage = c.voyage
"""


def _complex_union(select_by_port: str, port_col: str = "master_port") -> str:
    """Repeat an aggregate for each port and for each port complex (scope column)."""
    parts = [select_by_port.replace("{scope}", port_col).replace("{filter}", "TRUE")]
    for name, ports in PORT_COMPLEXES.items():
        in_list = ", ".join(f"'{p}'" for p in ports)
        parts.append(select_by_port.replace("{scope}", f"'{name}'").replace("{filter}", f"{port_col} IN ({in_list})"))
    return "\nUNION ALL\n".join(parts)


def _top(rows: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    rows = [r for r in rows if r.get(key) is not None]
    return max(rows, key=lambda r: r[key]) if rows else None


KPI_LIBRARY: list[dict[str, Any]] = [
    {
        "id": "anchorage_wait_monthly",
        "title": "Anchorage wait by port and month",
        "grain": "scope (port or complex) × month",
        "why": "Anchorage wait is the leading congestion signal: idle vessel days, demurrage, missed slots.",
        "requires": ["port_agent_events"],
        "sql": _complex_union(
            "SELECT {scope} AS scope, date_format(ata_ts, 'yyyy-MM') AS month, "
            "round(avg(wait_h) / 24, 2) AS avg_wait_days, round(max(wait_h) / 24, 2) AS max_wait_days, count(*) AS calls "
            "FROM stage_fact_port_call WHERE {filter} GROUP BY 1, 2"
        ),
        "headline": lambda rows: (
            lambda t: f"Worst congestion: {t['scope']} in {t['month']} — {t['avg_wait_days']} days average anchorage wait "
            f"over {t['calls']} calls." if t else None
        )(_top([r for r in rows if r["calls"] >= 10], "avg_wait_days")),
    },
    {
        "id": "anchorage_wait_yearly",
        "title": "Anchorage wait by port and year",
        "grain": "scope × year",
        "why": "Year-over-year congestion ranking shows where port-time risk is structural vs transient.",
        "requires": ["port_agent_events"],
        "sql": _complex_union(
            "SELECT {scope} AS scope, year(ata_ts) AS year, round(avg(wait_h) / 24, 2) AS avg_wait_days, "
            "count(*) AS calls, round(avg(CASE WHEN anchored THEN 1 ELSE 0 END), 3) AS anchored_share "
            "FROM stage_fact_port_call WHERE {filter} GROUP BY 1, 2"
        ),
        "headline": lambda rows: (
            lambda t: f"Highest yearly wait: {t['scope']} {t['year']} at {t['avg_wait_days']} days." if t else None
        )(_top([r for r in rows if r["calls"] >= 20 and r["scope"] not in PORT_COMPLEXES], "avg_wait_days")),
    },
    {
        "id": "schedule_reliability",
        "title": "Schedule reliability (arrival within 24h of published ETA)",
        "grain": "carrier × year",
        "why": "Unreliable carriers drive buffer inventory and missed connections for shippers.",
        "requires": ["carrier_schedules", "port_agent_events"],
        "sql": "SELECT carrier, year(planned_eta_ts) AS year, count(*) AS calls, "
        "sum(CASE WHEN on_time_24h THEN 1 ELSE 0 END) AS on_time_calls, "
        "round(avg(CASE WHEN on_time_24h THEN 1 ELSE 0 END), 3) AS on_time_share "
        "FROM stage_fact_schedule_adherence GROUP BY 1, 2",
        "headline": lambda rows: (
            lambda lo: f"Least reliable: {lo['carrier']} in {lo['year']} — only {lo['on_time_share']:.0%} of calls "
            f"within 24h of ETA." if lo else None
        )(min((r for r in rows if r["calls"] >= 30), key=lambda r: r["on_time_share"], default=None)),
    },
    {
        "id": "import_dwell_quarterly",
        "title": "Import container dwell (laden discharge → gate-out)",
        "grain": "scope × quarter",
        "why": "Terminal dwell drives yard congestion, storage charges and chassis shortages.",
        "requires": ["container_events"],
        "sql": """
WITH disc AS (
  SELECT container_id, master_port, event_ts AS disc_ts FROM stage_fact_container_event
  WHERE event_code = 'DISC' AND coalesce(empty_indicator, 'LADEN') = 'LADEN'
), gate AS (
  SELECT container_id, master_port, event_ts AS gate_ts FROM stage_fact_container_event WHERE event_code = 'GTOT'
), d AS (
  SELECT disc.container_id, disc.master_port, disc.disc_ts, min(gate.gate_ts) AS gate_ts
  FROM disc JOIN gate ON disc.container_id = gate.container_id AND disc.master_port = gate.master_port
   AND gate.gate_ts > disc.disc_ts AND gate.gate_ts < disc.disc_ts + INTERVAL 60 DAYS
  GROUP BY disc.container_id, disc.master_port, disc.disc_ts
)
"""
        + _complex_union(
            "SELECT {scope} AS scope, concat(year(disc_ts), 'Q', quarter(disc_ts)) AS quarter, "
            "round(avg((unix_timestamp(gate_ts) - unix_timestamp(disc_ts)) / 86400), 2) AS avg_dwell_days, "
            "count(*) AS containers FROM d WHERE {filter} GROUP BY 1, 2"
        ),
        "headline": lambda rows: (
            lambda t: f"Longest import dwell: {t['scope']} {t['quarter']} at {t['avg_dwell_days']} days." if t else None
        )(_top([r for r in rows if r["containers"] >= 50], "avg_dwell_days")),
    },
    {
        "id": "customs_holds",
        "title": "Customs holds by port of discharge",
        "grain": "port",
        "why": "Hold hotspots flag compliance risk and predictable delivery delays.",
        "requires": ["container_events"],
        "sql": "SELECT master_port, count(DISTINCT container_id) AS containers_on_hold, count(*) AS hold_events "
        "FROM stage_fact_container_event WHERE event_code = 'HOLD' AND master_port IS NOT NULL GROUP BY 1",
        "headline": lambda rows: (
            lambda t: f"Customs hold hotspot: {t['master_port']} with {t['containers_on_hold']} containers held." if t else None
        )(_top(rows, "containers_on_hold")),
    },
    {
        "id": "equipment_reassignments",
        "title": "Equipment reassignments by carrier",
        "grain": "carrier",
        "why": "Reassignments signal equipment imbalance and booking churn.",
        "requires": ["container_events"],
        "sql": "SELECT carrier, count(*) AS reassignments FROM stage_fact_container_event "
        "WHERE event_code = 'ASGN' GROUP BY 1",
        "headline": lambda rows: (
            lambda t: f"Most equipment reassignments: {t['carrier']} ({t['reassignments']})." if t else None
        )(_top(rows, "reassignments")),
    },
    {
        "id": "empty_repositioning",
        "title": "Empty container repositioning (empty discharges) by region",
        "grain": "year × region",
        "why": "Empty repositioning is pure cost; regional imbalance drives it.",
        "requires": ["container_events"],
        "sql": "SELECT year(event_ts) AS year, region, count(*) AS empty_discharges FROM stage_fact_container_event "
        "WHERE event_code = 'DISC' AND empty_indicator = 'EMPTY' AND region IS NOT NULL GROUP BY 1, 2",
        "headline": lambda rows: (
            lambda t: f"Empties pile up in {t['region']}: {t['empty_discharges']} empty discharges in {t['year']}." if t else None
        )(_top(rows, "empty_discharges")),
    },
    {
        "id": "booking_rolls",
        "title": "Booking roll share by year of planned sailing",
        "grain": "year",
        "why": "Rolled bookings are service failures customers feel directly.",
        "requires": ["bills_of_lading", "container_events"],
        "sql": """
WITH bk AS (
  SELECT booking_ref, min(issue_date) AS planned_date FROM stage_fact_shipment
  WHERE booking_ref IS NOT NULL AND issue_date IS NOT NULL GROUP BY booking_ref
), r AS (SELECT DISTINCT booking_ref FROM stage_fact_container_event WHERE event_code = 'ROLL')
SELECT year(planned_date) AS year, count(*) AS bookings, count(r.booking_ref) AS rolled_bookings,
       round(count(r.booking_ref) / count(*), 4) AS roll_share
FROM bk LEFT JOIN r ON bk.booking_ref = r.booking_ref GROUP BY 1
""",
        "headline": lambda rows: (
            lambda t: f"Roll rate peaked in {t['year']}: {t['roll_share']:.1%} of bookings rolled." if t else None
        )(_top(rows, "roll_share")),
    },
    {
        "id": "shipper_teu",
        "title": "Top shippers by TEU shipped",
        "grain": "shipper (master customer)",
        "why": "Customer concentration: who the network depends on.",
        "requires": ["bills_of_lading", "master_customer"],
        "sql": "SELECT s.shipper_master_id AS master_customer_id, m.legal_name AS shipper_name, sum(s.teu) AS teu, "
        "count(DISTINCT s.bl_number) AS bills_of_lading FROM stage_fact_shipment s "
        "JOIN silver_master_customer m ON s.shipper_master_id = m.master_customer_id GROUP BY 1, 2",
        "headline": lambda rows: (
            lambda t: f"Largest shipper: {t['shipper_name']} with {t['teu']} TEU." if t else None
        )(_top(rows, "teu")),
    },
    {
        "id": "transit_time_monthly",
        "title": "Transit time by trade lane (load → next discharge)",
        "grain": "origin area × destination region × load month",
        "why": "Transit-time drift is the headline service-quality metric for each trade lane.",
        "requires": ["container_events"],
        "sql": """
WITH legs AS (
  SELECT container_id, event_code, region, event_ts,
         lead(event_code) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_code,
         lead(region) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_region,
         lead(event_ts) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_ts
  FROM stage_fact_container_event
  WHERE event_code IN ('LOAD', 'DISC') AND coalesce(empty_indicator, 'LADEN') = 'LADEN'
)
SELECT split(region, '_')[0] AS origin_area, next_region AS dest_region, date_format(event_ts, 'yyyy-MM') AS load_month,
       round(avg((unix_timestamp(next_ts) - unix_timestamp(event_ts)) / 86400), 1) AS avg_transit_days,
       count(*) AS containers
FROM legs
WHERE event_code = 'LOAD' AND next_code = 'DISC' AND region IS NOT NULL AND next_region IS NOT NULL
  AND split(region, '_')[0] <> split(next_region, '_')[0]
GROUP BY 1, 2, 3
""",
        "headline": lambda rows: (
            lambda t: f"Slowest lane-month: {t['origin_area']} → {t['dest_region']} loaded {t['load_month']} "
            f"took {t['avg_transit_days']} days." if t else None
        )(_top([r for r in rows if r["containers"] >= 30], "avg_transit_days")),
    },
    {
        "id": "vessel_identity_changes",
        "title": "Vessel renames and MMSI changes",
        "grain": "fleet summary",
        "why": "Renamed / re-flagged ships break naive joins and can mask sanctions or ownership changes.",
        "requires": ["master_vessel"],
        "sql": "SELECT count(*) AS vessels, sum(CASE WHEN renamed THEN 1 ELSE 0 END) AS renamed_vessels, "
        "sum(CASE WHEN renamed AND mmsi_count > 1 THEN 1 ELSE 0 END) AS renamed_and_mmsi_changed "
        "FROM silver_master_vessel WHERE imo IS NOT NULL",
        "headline": lambda rows: (
            f"{rows[0]['renamed_vessels']} of {rows[0]['vessels']} vessels were renamed; "
            f"{rows[0]['renamed_and_mmsi_changed']} of those also changed MMSI." if rows else None
        ),
    },
]

FACTS = [
    ("fact_port_call", PORT_CALLS_SQL, ["port_agent_events"], "one row per vessel port call (ATA/ATB/ATD, wait)"),
    ("fact_container_event", CONTAINER_EVENTS_SQL, ["container_events"], "deduplicated container events across 3 feeds"),
    ("fact_shipment", SHIPMENTS_SQL, ["bills_of_lading"], "one row per original B/L × container with TEU"),
    ("fact_schedule_adherence", SCHEDULE_ADHERENCE_SQL, ["carrier_schedules", "port_agent_events"], "published ETA vs actual arrival per call"),
]

DIMS = [
    ("dim_vessel", "SELECT * FROM silver_master_vessel", ["master_vessel"], "one row per master vessel (IMO)"),
    (
        "dim_port",
        "SELECT p.*, t.utc_offset_h, "
        + "CASE "
        + " ".join(f"WHEN p.master_port IN ({', '.join(repr(x) for x in v)}) THEN '{k}'" for k, v in PORT_COMPLEXES.items())
        + " END AS port_complex FROM silver_master_port p LEFT JOIN silver_port_timezones t ON p.master_port = t.master_port",
        ["master_port", "port_timezones"],
        "one row per UN/LOCODE with region, UTC offset and port complex",
    ),
    ("dim_customer", "SELECT * FROM silver_master_customer", ["master_customer"], "one row per resolved customer"),
]


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return json.loads(json.dumps(rows, default=str))


class GoldBuilderAgent:
    """Investigates Silver, proposes Gold facts/KPIs/findings, materializes only with a human approval id."""

    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()
        self.judge = JudgeAgent()

    def _silver_names(self) -> set[str]:
        return {p.name for p in SILVER.iterdir() if p.is_dir()} if SILVER.exists() else set()

    def _register_silver(self) -> set[str]:
        names = self._silver_names()
        for n in names:
            self.spark.read.format("delta").load(str(SILVER / n)).createOrReplaceTempView(f"silver_{n}")
        return names

    def _stage(self, silver: set[str]) -> dict[str, str]:
        """Register facts + dims as stage_* temp views; return {name: error} for failures."""
        errors: dict[str, str] = {}
        for name, sql, req, _ in DIMS + FACTS:
            if not set(req) <= silver:
                errors[name] = f"missing Silver inputs: {sorted(set(req) - silver)}"
                continue
            try:
                self.spark.sql(sql).createOrReplaceTempView(f"stage_{name}")
            except Exception as exc:  # noqa: BLE001
                errors[name] = str(exc)[:300]
        return errors

    # -------------------------------------------------------------- investigate
    def investigate(self) -> dict[str, Any]:
        """Run the KPI library + LLM exploration over Silver; persist a plan awaiting approval."""
        ensure_lakehouse_dirs()
        silver = self._register_silver()
        stage_errors = self._stage(silver)
        kpis: list[dict[str, Any]] = []
        for k in KPI_LIBRARY:
            entry = {key: k[key] for key in ("id", "title", "grain", "why")}
            entry["table_name"] = f"gold.kpi_{k['id']}"
            entry["kind"] = "kpi"
            entry["source"] = "kpi_library"
            if not set(k["requires"]) <= silver:
                entry.update(status="unavailable", error=f"missing {sorted(set(k['requires']) - silver)}")
                kpis.append(entry)
                continue
            try:
                df = self.spark.sql(k["sql"])
                rows = _jsonable([r.asDict() for r in df.collect()])
                entry.update(
                    status="ready",
                    rows=len(rows),
                    columns=df.columns,
                    preview=rows[:8],
                    headline=k["headline"](rows),
                    sql=k["sql"].strip(),
                )
            except Exception as exc:  # noqa: BLE001
                entry.update(status="failed", error=str(exc)[:300])
            kpis.append(entry)

        findings = [
            {
                "id": f"F-{k['id']}",
                "title": k["title"],
                "headline": k.get("headline"),
                "why_it_matters": k["why"],
                "kpi": k["id"],
                "source": "kpi_library",
                "sql": k.get("sql"),
            }
            for k in kpis
            if k.get("status") == "ready" and k.get("headline")
        ]
        findings += self._llm_findings(silver, kpis)
        findings = self.judge.grade_findings(findings)
        findings.sort(key=lambda f: -float(f.get("judge_score") or 0))

        plan = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "awaiting_approval",
            "silver_tables": sorted(silver),
            "semantic_tables": len((self.store.load_semantics() or {}).get("tables") or {}),
            "dims": [{"table_name": f"gold.{n}", "kind": "dimension", "grain": g, "requires": r,
                      "status": "error" if n in stage_errors else "ready", "error": stage_errors.get(n)}
                     for n, _, r, g in DIMS],
            "facts": [{"table_name": f"gold.{n}", "kind": "fact", "grain": g, "requires": r,
                       "status": "error" if n in stage_errors else "ready", "error": stage_errors.get(n)}
                      for n, _, r, g in FACTS],
            "kpis": kpis,
            "findings": findings,
        }
        GOLD_PLAN_PATH.write_text(json.dumps(plan, indent=2, default=str), encoding="utf-8")
        FEED.emit(
            "gold_builder",
            f"Investigation done: {sum(1 for k in kpis if k.get('status') == 'ready')} KPIs, "
            f"{len(findings)} findings — awaiting human approval",
            event_type="gold_plan_ready",
        )
        return plan

    def _llm_findings(self, silver: set[str], kpis: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Let the LLM explore beyond the KPI library; every query is SQL-guarded and executed."""
        if not LLM.available:
            return []
        views = {f"silver_{n}" for n in silver} | {f"stage_{n}" for n, *_ in DIMS + FACTS}
        schemas = {}
        for v in sorted(views):
            try:
                schemas[v] = self.spark.table(v).columns
            except Exception:  # noqa: BLE001
                continue
        payload = {
            "task": "Propose up to 4 additional enterprise-critical findings NOT already covered by the KPIs.",
            "schemas": schemas,
            "existing_kpis": [{"id": k["id"], "headline": k.get("headline")} for k in kpis if k.get("status") == "ready"],
            "rules": [
                "Spark SQL, single SELECT, only the listed views",
                "Aggregate results (<= 50 rows); no PII columns (*_masked)",
                'Return JSON {"findings": [{"title", "why_it_matters", "sql"}]}',
            ],
        }
        raw = LLM.complete_json(
            "You are a senior shipping-operations analyst investigating a governed data lakehouse.",
            json.dumps(payload, default=str),
        )
        out: list[dict[str, Any]] = []
        for i, f in enumerate((raw or {}).get("findings") or []):
            if not isinstance(f, dict) or not f.get("sql"):
                continue
            check = validate_select(f["sql"], {v.lower() for v in schemas})
            if not check.ok:
                FEED.emit("gold_builder", f"LLM finding rejected by SQL guard: {check.reason}", level="warn")
                continue
            try:
                rows = _jsonable([r.asDict() for r in self.spark.sql(f["sql"]).limit(50).collect()])
            except Exception as exc:  # noqa: BLE001
                FEED.emit("gold_builder", f"LLM finding SQL failed: {str(exc)[:120]}", level="warn")
                continue
            if not rows:
                continue
            out.append(
                {
                    "id": f"F-llm-{i + 1}",
                    "title": str(f.get("title") or "LLM finding")[:120],
                    "headline": None,
                    "why_it_matters": str(f.get("why_it_matters") or "")[:400],
                    "source": "llm",
                    "sql": f["sql"],
                    "preview": rows[:8],
                }
            )
        return out

    def load_plan(self) -> dict[str, Any] | None:
        if not GOLD_PLAN_PATH.exists():
            return None
        try:
            return json.loads(GOLD_PLAN_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return None

    # back-compat for UI callers
    def propose_mart(self) -> dict[str, Any]:
        plan = self.load_plan() or self.investigate()
        return {"ok": True, "proposals": plan["dims"] + plan["facts"] + plan["kpis"], "primary_view": "gold.v_ops_overview", **plan}

    # -------------------------------------------------------------- materialize
    def materialize(self, approval_id: str | None, selected_kpis: list[str] | None = None) -> dict[str, Any]:
        """Write Gold. Requires a human approval id registered via review.get_approval."""
        decision = GUARD.check_gold_write(approval_id, agent="gold_builder")
        if not decision.allowed:
            return {"ok": False, "error": decision.reason, "phase": "awaiting_gold_approval"}
        plan = self.load_plan() or self.investigate()
        silver = self._register_silver()
        self._stage(silver)
        written: list[str] = []
        errors: list[str] = []
        catalog_tables: list[dict[str, Any]] = []

        def write(name: str, sql_or_view: str, kind: str, grain: str, description: str) -> None:
            try:
                df = self.spark.sql(sql_or_view) if " " in sql_or_view.strip() else self.spark.table(sql_or_view)
                res = self.gateway.lakehouse.write_delta(df, f"gold.{name}", agent="gold_builder", approval_id=approval_id)
                if res.get("ok"):
                    written.append(f"gold.{name}")
                    catalog_tables.append(
                        {"table_name": f"gold.{name}", "kind": kind, "grain": grain, "description": description,
                         "columns": self.spark.read.format("delta").load(str(GOLD / name)).columns}
                    )
                else:
                    errors.append(f"{name}: {res.get('error')}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{name}: {str(exc)[:300]}")

        for name, _, req, grain in DIMS + FACTS:
            if set(req) <= silver:
                write(name, f"stage_{name}", "dimension" if name.startswith("dim_") else "fact", grain, grain)

        chosen = set(selected_kpis) if selected_kpis else {k["id"] for k in plan["kpis"] if k.get("status") == "ready"}
        for k in KPI_LIBRARY:
            if k["id"] in chosen and set(k["requires"]) <= silver:
                write(f"kpi_{k['id']}", k["sql"], "kpi", k["grain"], f"{k['title']}. {k['why']}")

        # Findings table: the investigation's narrative, queryable from Ask
        findings = [f for f in plan.get("findings") or [] if not f.get("kpi") or f["kpi"] in chosen]
        if findings:
            rows = [
                {
                    "finding_id": f["id"],
                    "title": f["title"],
                    "headline": f.get("headline"),
                    "why_it_matters": f.get("why_it_matters"),
                    "source": f.get("source"),
                    "judge_grade": f.get("judge_grade"),
                    "judge_score": float(f.get("judge_score") or 0),
                    "evidence_sql": f.get("sql"),
                }
                for f in findings
            ]
            self.spark.createDataFrame(
                rows,
                "finding_id string, title string, headline string, why_it_matters string, source string, "
                "judge_grade string, judge_score double, evidence_sql string",
            ).createOrReplaceTempView("stage_insight_findings")
            write("insight_findings", "stage_insight_findings", "findings", "one row per investigation finding",
                  "Judge-graded findings from the Gold investigation")

        # Wide analyst view: port calls with vessel + port attributes (explicit columns, no ambiguity)
        if {"gold.fact_port_call", "gold.dim_vessel", "gold.dim_port"} <= set(written):
            write(
                "v_ops_overview",
                "SELECT c.call_id, c.master_vessel_id, v.imo, v.primary_name AS vessel_name, c.master_port, "
                "p.port_name, p.region, p.port_complex, c.agency, c.voyage, c.ata_ts, c.atb_ts, c.atd_ts, "
                "c.anchored, round(c.wait_h / 24, 3) AS wait_days "
                "FROM stage_fact_port_call c LEFT JOIN stage_dim_vessel v ON c.master_vessel_id = v.master_vessel_id "
                "LEFT JOIN stage_dim_port p ON c.master_port = p.master_port",
                "view",
                "one row per port call with vessel + port attributes",
                "Wide analyst surface for cross-entity questions",
            )

        catalog = {
            "primary_view": "gold.v_ops_overview",
            "approval_id": approval_id,
            "materialized_at": datetime.now(timezone.utc).isoformat(),
            "port_complexes": PORT_COMPLEXES,
            "tables": catalog_tables,
            "materialized": written,
            "errors": errors,
        }
        GOLD_CATALOG_PATH.write_text(yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8")
        plan["status"] = "materialized"
        plan["approval_id"] = approval_id
        GOLD_PLAN_PATH.write_text(json.dumps(plan, indent=2, default=str), encoding="utf-8")
        FEED.emit("gold_builder", f"Gold materialized: {len(written)} tables", event_type="gold_mart_built", tables=written)
        return {"ok": bool(written) and not errors, "phase": "gold_done", "materialized": written, "errors": errors, "catalog": catalog}

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        """Investigate; materialize only when the state carries a human approval id."""
        approval_id = state.get("gold_approval_id")
        plan = self.load_plan()
        if not (approval_id and plan and plan.get("status") == "awaiting_approval"):
            plan = self.investigate()
        if not approval_id:
            state["gold_mart"] = {"ok": False, "phase": "awaiting_gold_approval", "findings": len(plan["findings"])}
            state["phase"] = "awaiting_gold_approval"
            return state
        result = self.materialize(approval_id, state.get("gold_selected_kpis"))
        state["gold_mart"] = result
        state["phase"] = "gold_done" if result.get("ok") else "awaiting_gold_approval"
        return state


HEADLINE_FNS: dict[str, Callable[[list[dict[str, Any]]], str | None]] = {k["id"]: k["headline"] for k in KPI_LIBRARY}
