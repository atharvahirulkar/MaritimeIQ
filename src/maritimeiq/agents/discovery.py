"""Discovery agent: investigate Silver for the patterns and metrics that matter most.

1. Build derived views (port calls, container moves) on top of Silver.
2. Check a library of maritime KPIs against what Silver actually contains; run the ones
   the data supports.
3. With an LLM: propose additional metrics from the schema + semantic layer, validate
   and run them, then rank everything and write the business headline.
Findings are persisted to ``insights.json`` and the selected ones become Gold aggregates.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from maritimeiq.agents.llm import LLM
from maritimeiq.context_store.store import ContextStore
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import SILVER

INSIGHTS = "insights.json"

DERIVED_VIEWS: dict[str, dict[str, Any]] = {
    "derived_port_calls": {
        "requires": {"port_agent_events": ["master_vessel_id", "master_port", "event_code", "event_ts"]},
        "description": "One row per vessel port call: first anchorage (ANC) and first berthing (BRT), wait in days",
        "sql": """
            WITH ev AS (
              SELECT master_vessel_id, master_port, event_code, event_ts,
                     coalesce(sum(CASE WHEN event_code = 'BRT' THEN 1 ELSE 0 END) OVER (
                       PARTITION BY master_vessel_id, master_port ORDER BY event_ts
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS call_seq
              FROM silver_port_agent_events
              WHERE event_code IN ('ANC', 'BRT') AND event_ts IS NOT NULL
                AND master_vessel_id IS NOT NULL AND master_port IS NOT NULL
            ), calls AS (
              SELECT master_vessel_id, master_port, call_seq,
                     min(CASE WHEN event_code = 'ANC' THEN event_ts END) AS anc_ts,
                     min(CASE WHEN event_code = 'BRT' THEN event_ts END) AS brt_ts
              FROM ev GROUP BY master_vessel_id, master_port, call_seq
            )
            SELECT concat(master_vessel_id, '|', master_port, '|', cast(call_seq AS string)) AS port_call_id,
                   master_vessel_id, master_port, anc_ts, brt_ts,
                   coalesce(anc_ts, brt_ts) AS arrival_ts,
                   CASE WHEN anc_ts IS NOT NULL AND brt_ts > anc_ts
                             AND unix_timestamp(brt_ts) - unix_timestamp(anc_ts) < 86400 * 40
                        THEN round((unix_timestamp(brt_ts) - unix_timestamp(anc_ts)) / 86400.0, 3) END AS wait_days
            FROM calls WHERE brt_ts IS NOT NULL
        """,
    },
    "derived_container_moves": {
        "requires": {"container_events": ["container_id", "event_code", "event_ts", "master_port"]},
        "description": "Container legs: load port/time → next discharge port/time (transit days)",
        "sql": """
            WITH ev AS (
              SELECT container_id, booking_ref, carrier, master_port, event_code, event_ts,
                     lead(event_code) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_code,
                     lead(event_ts) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_ts,
                     lead(master_port) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_port
              FROM silver_container_events
              WHERE event_code IN ('LOAD', 'DISC') AND event_ts IS NOT NULL AND container_id <> ''
            )
            SELECT container_id, booking_ref, carrier, master_port AS pol, event_ts AS load_ts,
                   next_port AS pod, next_ts AS disc_ts,
                   round((unix_timestamp(next_ts) - unix_timestamp(event_ts)) / 86400.0, 2) AS transit_days
            FROM ev
            WHERE event_code = 'LOAD' AND next_code = 'DISC' AND next_port <> master_port
              AND next_ts > event_ts AND unix_timestamp(next_ts) - unix_timestamp(event_ts) < 86400 * 90
        """,
    },
}

RECIPES: list[dict[str, Any]] = [
    {
        "id": "anchorage_wait_monthly",
        "title": "Anchorage wait by port and month",
        "why": "Anchorage waiting time is the clearest congestion signal; it drives demurrage, schedule slippage, and fuel burn.",
        "requires": {"derived_port_calls": []},
        "grain": "port × month",
        "sql": """
            SELECT master_port, date_format(anc_ts, 'yyyy-MM') AS month, count(*) AS port_calls,
                   round(avg(wait_days), 2) AS avg_wait_days, round(max(wait_days), 2) AS max_wait_days
            FROM derived_port_calls WHERE wait_days IS NOT NULL
            GROUP BY 1, 2 ORDER BY avg_wait_days DESC
        """,
    },
    {
        "id": "port_congestion_yearly",
        "title": "Most congested ports by year",
        "why": "Ranks ports by average anchorage wait per year so network planners can see where delay concentrates.",
        "requires": {"derived_port_calls": []},
        "grain": "port × year",
        "sql": """
            SELECT master_port, year(anc_ts) AS year, count(*) AS port_calls,
                   round(avg(wait_days), 2) AS avg_wait_days
            FROM derived_port_calls WHERE wait_days IS NOT NULL
            GROUP BY 1, 2 HAVING count(*) >= 5 ORDER BY year, avg_wait_days DESC
        """,
    },
    {
        "id": "schedule_reliability",
        "title": "Schedule reliability by carrier and year",
        "why": "Share of calls arriving within 24h of the published ETA — the headline KPI shippers use to pick carriers.",
        "requires": {"carrier_schedules": ["carrier", "master_vessel_id", "master_port", "eta_ts"], "derived_port_calls": []},
        "grain": "carrier × year",
        "sql": """
            WITH m AS (
              SELECT s.carrier, s.eta_ts, c.arrival_ts,
                     row_number() OVER (PARTITION BY s.carrier, s.master_vessel_id, s.master_port, s.eta_ts
                                        ORDER BY abs(unix_timestamp(c.arrival_ts) - unix_timestamp(s.eta_ts))) AS rn
              FROM silver_carrier_schedules s
              JOIN derived_port_calls c
                ON s.master_vessel_id = c.master_vessel_id AND s.master_port = c.master_port
               AND abs(datediff(c.arrival_ts, s.eta_ts)) <= 7
            )
            SELECT carrier, year(eta_ts) AS year, count(*) AS matched_calls,
                   round(avg(CASE WHEN abs(unix_timestamp(arrival_ts) - unix_timestamp(eta_ts)) <= 86400
                                  THEN 1.0 ELSE 0.0 END), 3) AS on_time_share
            FROM m WHERE rn = 1 GROUP BY 1, 2 ORDER BY year, on_time_share DESC
        """,
    },
    {
        "id": "customs_holds_by_port",
        "title": "Customs holds by port",
        "why": "Holds tie up containers and yard space; concentrated holds point to documentation or compliance issues.",
        "requires": {"container_events": ["event_code", "master_port", "container_id"]},
        "grain": "port",
        "sql": """
            SELECT master_port, count(DISTINCT container_id) AS containers_on_hold, count(*) AS hold_events
            FROM silver_container_events WHERE event_code = 'HOLD'
            GROUP BY 1 ORDER BY containers_on_hold DESC
        """,
    },
    {
        "id": "equipment_events_by_carrier",
        "title": "Equipment exceptions by carrier (reassignments, rolls, holds)",
        "why": "Exception volume per carrier shows operational friction: reassigned equipment, rolled cargo, and holds.",
        "requires": {"container_events": ["carrier", "event_code"]},
        "grain": "carrier × event_code",
        "sql": """
            SELECT carrier, event_code, count(*) AS events
            FROM silver_container_events WHERE event_code IN ('ASGN', 'ROLL', 'HOLD', 'CANC')
            GROUP BY 1, 2 ORDER BY events DESC
        """,
    },
    {
        "id": "rolled_booking_share",
        "title": "Share of bookings rolled to a later sailing by year",
        "why": "Rolled cargo is a service failure customers feel directly; the trend shows whether capacity is tightening.",
        "requires": {"container_events": ["booking_ref", "event_code", "event_ts"]},
        "grain": "year",
        "sql": """
            SELECT year(event_ts) AS year, count(DISTINCT booking_ref) AS bookings,
                   count(DISTINCT CASE WHEN event_code = 'ROLL' THEN booking_ref END) AS rolled_bookings,
                   round(count(DISTINCT CASE WHEN event_code = 'ROLL' THEN booking_ref END)
                         / count(DISTINCT booking_ref), 4) AS rolled_share
            FROM silver_container_events WHERE booking_ref IS NOT NULL AND event_ts IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """,
    },
    {
        "id": "empty_repositioning",
        "title": "Empty containers discharged (repositioned) by country and year",
        "why": "Empty repositioning is pure cost; it reveals trade imbalance by region.",
        "requires": {"container_events": ["event_code", "empty_indicator", "master_port", "event_ts"]},
        "grain": "country × year",
        "sql": """
            SELECT substr(master_port, 1, 2) AS country, year(event_ts) AS year, count(*) AS empty_discharges
            FROM silver_container_events
            WHERE event_code = 'DISC' AND empty_indicator = 'EMPTY' AND master_port IS NOT NULL
            GROUP BY 1, 2 ORDER BY empty_discharges DESC
        """,
    },
    {
        "id": "import_dwell",
        "title": "Import container dwell (discharge → gate-out) by port and quarter",
        "why": "Dwell time drives terminal capacity and detention charges; spikes precede congestion.",
        "requires": {"container_events": ["container_id", "event_code", "event_ts", "master_port", "empty_indicator"]},
        "grain": "port × quarter",
        "sql": """
            WITH ev AS (
              SELECT container_id, master_port, event_code, event_ts, empty_indicator,
                     lead(event_code) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_code,
                     lead(event_ts) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_ts,
                     lead(master_port) OVER (PARTITION BY container_id ORDER BY event_ts) AS next_port
              FROM silver_container_events
              WHERE event_code IN ('DISC', 'GTOT') AND event_ts IS NOT NULL AND container_id <> ''
            )
            SELECT master_port, concat(year(event_ts), '-Q', quarter(event_ts)) AS quarter, count(*) AS containers,
                   round(avg((unix_timestamp(next_ts) - unix_timestamp(event_ts)) / 86400.0), 2) AS avg_dwell_days
            FROM ev
            WHERE event_code = 'DISC' AND next_code = 'GTOT' AND next_port = master_port
              AND coalesce(empty_indicator, 'LADEN') = 'LADEN' AND next_ts > event_ts
              AND unix_timestamp(next_ts) - unix_timestamp(event_ts) < 86400 * 60
            GROUP BY 1, 2 ORDER BY 1, 2
        """,
    },
    {
        "id": "transit_time_lanes",
        "title": "Transit time by lane (load country → discharge country) and month",
        "why": "Lane transit times are what customers plan inventory around; slow lanes are a competitive risk.",
        "requires": {"derived_container_moves": []},
        "grain": "lane × month",
        "sql": """
            SELECT substr(pol, 1, 2) AS load_country, substr(pod, 1, 2) AS discharge_country,
                   date_format(load_ts, 'yyyy-MM') AS load_month, count(*) AS legs,
                   round(avg(transit_days), 2) AS avg_transit_days
            FROM derived_container_moves WHERE pol IS NOT NULL AND pod IS NOT NULL
            GROUP BY 1, 2, 3 HAVING count(*) >= 3 ORDER BY legs DESC
        """,
    },
    {
        "id": "top_shippers_teu",
        "title": "Top shippers by TEU",
        "why": "Customer concentration: the few shippers that drive most volume are the accounts to protect.",
        "requires": {"bills_of_lading": ["ShipperName", "teu"]},
        "grain": "shipper",
        "sql": """
            SELECT coalesce(shipper_customer_id, ShipperName) AS shipper_key,
                   first(ShipperName) AS shipper_name, sum(teu) AS teu, count(*) AS container_lines
            FROM silver_bills_of_lading WHERE teu IS NOT NULL
            GROUP BY 1 ORDER BY teu DESC
        """,
    },
    {
        "id": "vessel_identity_changes",
        "title": "Vessels renamed or re-flagged (multiple names / MMSIs)",
        "why": "Identity changes break naive joins and are a sanctions/compliance signal; ER links them to one master.",
        "requires": {"master_vessel": ["name_count", "mmsi_count"]},
        "grain": "fleet",
        "sql": """
            SELECT count(*) AS vessels,
                   sum(CASE WHEN name_count > 1 THEN 1 ELSE 0 END) AS renamed_vessels,
                   sum(CASE WHEN name_count > 1 AND mmsi_count > 1 THEN 1 ELSE 0 END) AS renamed_and_mmsi_changed
            FROM silver_master_vessel WHERE imo IS NOT NULL
        """,
    },
]


class DiscoveryAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()

    # --- context ------------------------------------------------------------------
    def register_views(self) -> dict[str, list[str]]:
        """Register silver_* temp views and the derived views the data supports."""
        schemas: dict[str, list[str]] = {}
        if SILVER.exists():
            for p in sorted(SILVER.iterdir()):
                if p.is_dir():
                    df = self.spark.read.format("delta").load(str(p))
                    df.createOrReplaceTempView(f"silver_{p.name}")
                    schemas[p.name] = df.columns
        for name, view in DERIVED_VIEWS.items():
            if self._supported(view["requires"], schemas):
                try:
                    df = self.spark.sql(view["sql"])
                    df.createOrReplaceTempView(name)
                    schemas[name] = df.columns
                except Exception as exc:  # noqa: BLE001
                    FEED.emit("discovery", f"derived view {name} failed: {exc}", level="warn")
        return schemas

    @staticmethod
    def _supported(requires: dict[str, list[str]], schemas: dict[str, list[str]]) -> bool:
        return all(t in schemas and all(c in schemas[t] for c in cols) for t, cols in requires.items())

    def _semantic_summary(self) -> dict[str, Any]:
        sem = self.store.load_semantics()
        tables = {
            t: {"description": m.get("description"), "columns": m.get("columns"), "code_map": m.get("code_map")}
            for t, m in (sem.get("silver_tables") or {}).items()
        }
        return {"tables": tables, "relationships": sem.get("relationships") or []}

    # --- run ------------------------------------------------------------------------
    def _execute(self, sql: str) -> dict[str, Any]:
        return self.gateway.lakehouse.run_spark_sql(sql, agent="discovery", limit=25)

    def _headline(self, f: dict[str, Any]) -> str:
        rows = f.get("preview") or []
        if not rows:
            return "No rows returned."
        top = rows[0]
        parts = [f"{k}={v}" for k, v in list(top.items())[:4]]
        return f"Top: {', '.join(parts)} ({f.get('row_count_preview')} rows shown)"

    def _llm_extra(self, schemas: dict[str, list[str]], existing: list[str]) -> list[dict[str, Any]]:
        if not LLM.available:
            return []
        raw = LLM.complete_json(
            "You are a maritime/logistics data analyst investigating clean Silver tables. Propose up to 3 NEW "
            "metrics critical to an ocean carrier or port operator that are not in `existing`. Use Spark SQL over "
            "the listed views only (silver_<table> or derived_*). Return JSON {\"metrics\": [{\"id\", \"title\", "
            "\"why\", \"grain\", \"sql\"}]}.",
            json.dumps({"views": {(k if k.startswith("derived_") else f"silver_{k}"): v for k, v in schemas.items()},
                        "semantics": self._semantic_summary(), "existing": existing}, default=str)[:60000],
        )
        out = []
        for m in (raw or {}).get("metrics") or []:
            if not isinstance(m, dict) or not m.get("sql") or not m.get("title"):
                continue
            refs = set(re.findall(r"\b(?:from|join)\s+([a-zA-Z0-9_]+)", m["sql"], flags=re.I))
            allowed = {f"silver_{k}" for k in schemas} | {k for k in schemas if k.startswith("derived_")}
            if not refs or not refs <= allowed:
                continue
            mid = re.sub(r"[^a-z0-9_]", "_", str(m.get("id") or m["title"]).lower())[:40]
            out.append({**m, "id": f"llm_{mid}", "source": "llm", "requires": {}})
        return out

    def _rank(self, findings: list[dict[str, Any]]) -> str:
        if not LLM.available or not findings:
            for i, f in enumerate(findings):
                f["rank"] = i + 1
                f["headline"] = self._headline(f)
            return "heuristic"
        raw = LLM.complete_json(
            "You are advising an ocean-carrier executive. Rank these findings by business criticality and write a "
            "one-sentence headline per finding that states the key number from its preview rows. Return JSON "
            "{\"ranking\": [{\"id\", \"rank\", \"headline\", \"impact\": \"high|medium|low\"}]}.",
            json.dumps([{"id": f["id"], "title": f["title"], "why": f["why"], "preview": f["preview"][:6]}
                        for f in findings], default=str),
        )
        by_id = {r.get("id"): r for r in (raw or {}).get("ranking") or [] if isinstance(r, dict)}
        for i, f in enumerate(findings):
            r = by_id.get(f["id"]) or {}
            f["rank"] = int(r.get("rank") or (len(findings) + i))
            f["headline"] = r.get("headline") or self._headline(f)
            f["impact"] = r.get("impact")
        findings.sort(key=lambda x: x["rank"])
        for i, f in enumerate(findings):
            f["rank"] = i + 1
        return "llm" if by_id else "heuristic"

    def run(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(state or {})
        FEED.emit("discovery", "Investigating Silver for critical metrics")
        schemas = self.register_views()
        candidates = [{**r, "source": "library"} for r in RECIPES if self._supported(r["requires"], schemas)]
        skipped = [r["id"] for r in RECIPES if not self._supported(r["requires"], schemas)]
        candidates += self._llm_extra(schemas, [c["title"] for c in candidates])
        findings = []
        for c in candidates:
            res = self._execute(c["sql"])
            if not res.get("ok") or not res.get("rows"):
                FEED.emit("discovery", f"{c['id']}: dropped ({res.get('error') or 'no rows'})", level="warn")
                continue
            findings.append({
                "id": c["id"], "title": c["title"], "why": c["why"], "grain": c.get("grain"),
                "sql": " ".join(c["sql"].split()), "source": c["source"], "columns": res["columns"],
                "preview": res["rows"], "row_count_preview": len(res["rows"]),
                "gold_table": f"agg_{c['id']}", "selected": True,
            })
        ranked_by = self._rank(findings)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(), "ranked_by": ranked_by,
            "derived_views": {k: DERIVED_VIEWS[k]["description"] for k in DERIVED_VIEWS if k in schemas},
            "skipped_unsupported": skipped, "findings": findings,
        }
        self.store.save_json(INSIGHTS, payload)
        state["insights"] = {"findings": len(findings), "ranked_by": ranked_by}
        FEED.emit("discovery", f"{len(findings)} findings (ranked by {ranked_by}); skipped={skipped}",
                  event_type="discovery_complete")
        return state

    def set_selection(self, selected_ids: list[str]) -> None:
        payload = self.store.load_json(INSIGHTS, {}) or {}
        for f in payload.get("findings") or []:
            f["selected"] = f["id"] in selected_ids
        self.store.save_json(INSIGHTS, payload)
