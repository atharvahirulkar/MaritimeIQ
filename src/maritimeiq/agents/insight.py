"""Insight agent: guarded Text-to-SQL with a Gold → Silver+semantics → refuse cascade.

Order of defence for every question:
  1. adversarial / PII / injection prompt guard
  2. domain scope check (question must touch the activated data's vocabulary)
  3. verified Gold KPI intents, then LLM SQL over Gold, then LLM SQL over Silver + semantics
  4. structural SQL validation (single SELECT, allowlisted views, no file/JVM access)
  5. LLM-as-judge: a result that does not answer the question is refused, not shown
  6. PII masking on every returned row
"""
from __future__ import annotations

import json
import re
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
from maritimeiq.utils.paths import CONFIGS, CONTEXT, GOLD, SILVER

REFUSE_MSG = "This question cannot be answered from the activated data."

DOMAIN_TERMS = {
    "vessel", "vessels", "ship", "ships", "port", "ports", "anchor", "anchorage", "anchored", "berth", "berthed",
    "container", "containers", "teu", "carrier", "carriers", "booking", "bookings", "shipper", "shippers",
    "consignee", "consignees", "customs", "hold", "holds", "dwell", "transit", "schedule", "schedules", "eta",
    "voyage", "voyages", "crew", "imo", "mmsi", "call", "calls", "empty", "empties", "reposition", "repositioned",
    "reassignment", "reassignments", "reassigned", "rolled", "roll", "wait", "waits", "waiting", "congestion",
    "cargo", "freight", "terminal", "terminals", "lane", "lanes", "region", "regions", "discharge", "discharged",
    "gate", "reliability", "reliable", "on-time", "renamed", "rename", "fleet", "seafarer", "seafarers",
    "bill", "lading", "customer", "customers", "shipment", "shipments", "trade", "sailing", "sailings",
    "insight", "insights", "finding", "findings", "kpi", "kpis", "metric", "metrics", "ais", "position", "positions",
}

# Destination regions named in questions (World Port Index region codes)
DEST_REGIONS = {
    r"north(ern)? europe|n\.? europe": "EUR_N",
    r"(us|north america(n)?) west coast|west coast": "NAM_W",
    r"middle east|gulf": "MIDEAST",
}


def _words(q: str) -> set[str]:
    return set(re.findall(r"[a-z0-9/'-]+", q.lower()))


def _year(q: str) -> int | None:
    m = re.search(r"\b(20\d\d)\b", q)
    return int(m.group(1)) if m else None


def _top_n(q: str, default: int) -> int:
    m = re.search(r"\btop\s+(\d{1,2})\b|\b(\d{1,2})\s+(ports|shippers|carriers|vessels|customers)\b", q.lower())
    if m:
        return max(1, min(50, int(m.group(1) or m.group(2))))
    return default


# Port names known to Gold (lower-case name → LOCODE); refreshed from gold_dim_port before matching
PORT_NAMES: dict[str, str] = {}
_NOT_PLACES = {"january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
               "november", "december", "q1", "q2", "q3", "q4", "total", "each", "the", "which", "what"}
UNKNOWN = "__unknown__"


def _scope(q: str) -> str | None:
    """Port complex / port named in the question; UNKNOWN when a place is named that Gold does not know."""
    ql = q.lower()
    la = re.search(r"\blos angeles\b|\bsan pedro\b", ql)
    lb = re.search(r"\blong beach\b", ql)
    if re.search(r"\bla\s*/\s*lb\b|\bla/long beach\b", ql) or (la and lb):
        return "LA/LB"
    if la:
        return "USLAX"
    if lb:
        return "USLGB"
    for name in sorted(PORT_NAMES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(name)}\b", ql):
            return PORT_NAMES[name]
    m = re.search(r"\b([A-Z]{2}[A-Z0-9]{3})\b", q)
    if m and (not PORT_NAMES or m.group(1) in PORT_NAMES.values()):
        return m.group(1)
    place = re.search(r"\b(?:at|in|off|for|into)\s+(?:the\s+)?(?:port of\s+)?([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)", q)
    if place and place.group(1).lower() not in _NOT_PLACES:
        return UNKNOWN
    return None


def _scope_filter(q: str, col: str = "scope") -> str | None:
    """SQL predicate for the port scope; None means the intent must not answer (unknown place)."""
    sc = _scope(q)
    if sc == UNKNOWN:
        return None
    return f"{col} = '{sc}'" if sc else "TRUE"


def _has(q: str, *patterns: str) -> bool:
    return all(re.search(p, q, flags=re.I) for p in patterns)


def _transit_sql(q: str) -> str:
    conds = []
    m = re.search(r"\b(asia|europe|north america|middle east)\b.*\bto\b", q, flags=re.I)
    if m:
        conds.append(f"origin_area = '{ {'asia': 'ASIA', 'europe': 'EUR', 'north america': 'NAM', 'middle east': 'MIDEAST'}[m.group(1).lower()] }'")
    for pat, region in DEST_REGIONS.items():
        if re.search(r"\bto\b.*(" + pat + ")", q, flags=re.I):
            conds.append(f"dest_region = '{region}'")
            break
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    return (
        "SELECT origin_area, dest_region, load_month, avg_transit_days, containers "
        f"FROM gold_kpi_transit_time_monthly{where} ORDER BY load_month"
    )


# Verified Gold query library. Each intent: (name, gold table it needs, matcher, sql builder, explanation)
Intent = tuple[str, str, Callable[[str], bool], Callable[[str], str], str]

VERIFIED_INTENTS: list[Intent] = [
    (
        "anchorage_monthly",
        "kpi_anchorage_wait_monthly",
        lambda q: _has(q, r"\banchor(age)?\b|\bwaiting at anchor", r"\bwait", r"\bmonths?\b|\bmonthly\b")
        and _scope_filter(q) is not None,
        lambda q: "SELECT scope, month, avg_wait_days, calls FROM gold_kpi_anchorage_wait_monthly "
        f"WHERE {_scope_filter(q)} ORDER BY avg_wait_days DESC LIMIT {_top_n(q, 5)}",
        "Average ATA→ATB anchorage wait per month for the requested port scope (Gold KPI).",
    ),
    (
        "anchorage_ports_ranked",
        "kpi_anchorage_wait_yearly",
        lambda q: _has(q, r"\banchor(age)?\b", r"\bwait", r"\bports\b|\bwhich (\d+ )?port\b")
        and not _has(q, r"\bmonths?\b|\bmonthly\b"),
        lambda q: "SELECT scope AS port, year, avg_wait_days, calls FROM gold_kpi_anchorage_wait_yearly "
        "WHERE scope NOT IN ('LA/LB')"
        + (f" AND year = {_year(q)}" if _year(q) else "")
        + f" ORDER BY {'' if _year(q) else 'year DESC, '}avg_wait_days DESC LIMIT {_top_n(q, 5)}",
        "Ports ranked by average anchorage wait (Gold KPI).",
    ),
    (
        "empty_repositioning",
        "kpi_empty_repositioning",
        lambda q: _has(q, r"\bempt(y|ies)\b", r"\breposition|\bdischarg"),
        lambda q: "SELECT year, region, empty_discharges FROM gold_kpi_empty_repositioning"
        + (f" WHERE year = {_year(q)}" if _year(q) else "")
        + " ORDER BY year, region",
        "Empty container discharges by destination region and year (Gold KPI).",
    ),
    (
        "booking_rolls",
        "kpi_booking_rolls",
        lambda q: _has(q, r"\brolled\b|\brolls?\b|\broll(ed)?[- ]over\b", r"\bbookings?\b|\bsailings?\b"),
        lambda q: "SELECT year, bookings, rolled_bookings, roll_share FROM gold_kpi_booking_rolls "
        "WHERE year IS NOT NULL ORDER BY year",
        "Share of bookings with a ROLL event, by year of the originally planned sailing (Gold KPI).",
    ),
    (
        "vessel_renames",
        "kpi_vessel_identity_changes",
        lambda q: _has(q, r"\brenamed?\b|\brenaming\b|\bname changes?\b", r"\bvessels?\b|\bships?\b"),
        lambda q: "SELECT vessels, renamed_vessels, renamed_and_mmsi_changed FROM gold_kpi_vessel_identity_changes",
        "Vessels whose AIS name changed (typo variants clustered out) and those that also changed MMSI (Gold KPI).",
    ),
    (
        "top_shippers_teu",
        "kpi_shipper_teu",
        lambda q: _has(q, r"\bshippers?\b", r"\bteu\b|\btop\b|\blargest\b|\bbiggest\b"),
        lambda q: f"SELECT shipper_name, teu, bills_of_lading FROM gold_kpi_shipper_teu ORDER BY teu DESC LIMIT {_top_n(q, 5)}",
        "Shippers (resolved across CRM duplicates) ranked by TEU on original B/Ls (Gold KPI).",
    ),
    (
        "equipment_reassignments",
        "kpi_equipment_reassignments",
        lambda q: _has(q, r"\breassign(ed|ment|ments)?\b"),
        lambda q: "SELECT carrier, reassignments FROM gold_kpi_equipment_reassignments ORDER BY carrier",
        "Equipment reassignment events per carrier across all three feed formats (Gold KPI).",
    ),
    (
        "import_dwell",
        "kpi_import_dwell_quarterly",
        lambda q: _has(q, r"\bdwell\b") and _scope_filter(q) is not None,
        lambda q: "SELECT scope, quarter, avg_dwell_days, containers FROM gold_kpi_import_dwell_quarterly "
        f"WHERE {_scope_filter(q)} ORDER BY scope, quarter",
        "Average laden discharge → gate-out dwell by quarter (Gold KPI).",
    ),
    (
        "customs_holds",
        "kpi_customs_holds",
        lambda q: _has(q, r"\bcustoms\b", r"\bholds?\b") or _has(q, r"\bon hold\b", r"\bcontainers?\b"),
        lambda q: "SELECT master_port, containers_on_hold, hold_events FROM gold_kpi_customs_holds "
        f"ORDER BY containers_on_hold DESC LIMIT {_top_n(q, 10)}",
        "Containers placed on customs hold by port of discharge (Gold KPI).",
    ),
    (
        "schedule_reliability",
        "kpi_schedule_reliability",
        lambda q: _has(q, r"\breliab|\bon[- ]time\b|\bwithin 24\s?h", r"\beta\b|\bschedule|\bcarriers?\b|\barriv"),
        lambda q: "SELECT carrier, year, on_time_share, calls FROM gold_kpi_schedule_reliability"
        + (f" WHERE year = {_year(q)}" if _year(q) else "")
        + " ORDER BY carrier, year",
        "Share of calls arriving within 24h of the first published ETA, by carrier and year (Gold KPI).",
    ),
    (
        "transit_time",
        "kpi_transit_time_monthly",
        lambda q: _has(q, r"\btransit\b"),
        _transit_sql,
        "Average load → next-discharge transit days per load month on the requested lane (Gold KPI).",
    ),
    (
        "key_findings",
        "insight_findings",
        lambda q: _has(q, r"\b(key|top|critical|main|most important)\b", r"\b(insights?|findings?|risks?|issues?|patterns?)\b"),
        lambda q: "SELECT title, headline, why_it_matters, judge_grade FROM gold_insight_findings "
        "ORDER BY judge_score DESC LIMIT 10",
        "Judge-graded findings from the Gold investigation agent.",
    ),
]


def match_intent(question: str, available_gold: set[str]) -> tuple[str, str, str] | None:
    """Return (intent_name, sql, explanation) for a verified Gold intent, if any applies."""
    for name, table, matcher, builder, explanation in VERIFIED_INTENTS:
        if table in available_gold and matcher(question):
            return name, builder(question), explanation
    return None


class InsightAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.system_prompt = (CONFIGS / "prompts" / "insight_system.txt").read_text(encoding="utf-8")
        self.store = ContextStore()
        self.judge = JudgeAgent()

    # ------------------------------------------------------------- catalog
    def _layer_root(self, layer: str):
        return GOLD if layer == "gold" else SILVER

    def _register(self, layer: str) -> dict[str, list[str]]:
        """Register <layer>_<table> temp views; return {view: columns}."""
        root = self._layer_root(layer)
        out: dict[str, list[str]] = {}
        if not root.exists():
            return out
        for p in sorted(root.iterdir()):
            if not p.is_dir():
                continue
            try:
                df = self.spark.read.format("delta").load(str(p))
            except Exception:  # noqa: BLE001
                continue
            view = f"{layer}_{p.name}"
            df.createOrReplaceTempView(view)
            out[view] = df.columns
        return out

    def _load_port_names(self) -> None:
        try:
            rows = self.spark.table("gold_dim_port").select("master_port", "port_name").collect()
        except Exception:  # noqa: BLE001
            return
        PORT_NAMES.clear()
        for r in rows:
            if r.port_name:
                PORT_NAMES[str(r.port_name).lower()] = r.master_port

    def _gold_catalog(self) -> dict[str, Any]:
        path = CONTEXT / "gold_catalog.yaml"
        if not path.exists():
            return {"tables": []}
        try:
            return yaml.safe_load(path.read_text(encoding="utf-8")) or {"tables": []}
        except Exception:  # noqa: BLE001
            return {"tables": []}

    def _semantics_payload(self) -> dict[str, Any]:
        sem = self.store.load_semantics()
        glossary = []
        for table, meta in (sem.get("tables") or {}).items():
            for col in meta.get("columns") or []:
                glossary.append(
                    {"table": table, "column": col.get("name"), "meaning": col.get("meaning"), "pii": bool(col.get("pii")),
                     "code_map": col.get("code_map") or None}
                )
        return {"glossary": glossary[:250]}

    def _vocabulary(self, schemas: dict[str, list[str]]) -> set[str]:
        vocab = set(DOMAIN_TERMS)
        for view, cols in schemas.items():
            vocab.update(view.split("_")[1:])
            for c in cols:
                vocab.update(p for p in c.lower().split("_") if len(p) > 3)
        try:
            for r in self.spark.table("gold_dim_port").select("port_name").collect():
                if r.port_name:
                    vocab.update(str(r.port_name).lower().split())
        except Exception:  # noqa: BLE001
            pass
        return vocab

    # ------------------------------------------------------------ planning
    def _llm_plan(self, question: str, layer: str, schemas: dict[str, list[str]], error: str | None = None,
                  previous_sql: str | None = None) -> dict[str, Any] | None:
        if not LLM.available or not schemas:
            return None
        payload: dict[str, Any] = {
            "question": question,
            "layer": layer,
            "views": {v: cols for v, cols in schemas.items()},
            "semantics": self._semantics_payload(),
            "instructions": (
                f"Write ONE Spark SQL SELECT using only the listed {layer}_* views. "
                "If the question cannot be answered from these views, return "
                '{"unsupported": true, "sql": null, "explanation": "..."}.'
            ),
        }
        if layer == "gold":
            catalog = self._gold_catalog()
            payload["gold_catalog"] = [
                {"view": "gold_" + t["table_name"].split(".", 1)[-1], "kind": t.get("kind"), "grain": t.get("grain"),
                 "description": t.get("description")}
                for t in catalog.get("tables") or []
            ]
            payload["port_complexes"] = catalog.get("port_complexes")
        if error:
            payload["previous_sql"] = previous_sql
            payload["error"] = error[:600]
            payload["instructions"] += " The previous SQL failed with the given error — fix it."
        raw = LLM.complete_json(self.system_prompt, json.dumps(payload, default=str))
        if not raw:
            return None
        if raw.get("unsupported") or not raw.get("sql"):
            return {"unsupported": True, "explanation": raw.get("explanation") or REFUSE_MSG}
        return {
            "sql": str(raw["sql"]).strip().rstrip(";"),
            "explanation": raw.get("explanation"),
            "glossary_terms": raw.get("glossary_terms") or [],
            "confidence": float(raw.get("confidence") or 0.6),
            "answer_source": "llm",
        }

    def _try_layer(self, question: str, layer: str) -> dict[str, Any]:
        """Plan, validate, execute and judge on one layer. Returns an answer or {'unsupported': reason}."""
        schemas = self._register(layer)
        if not schemas:
            return {"unsupported": f"{layer} layer is empty"}
        allowed = {v.lower() for v in schemas}
        plan = None
        if layer == "gold":
            self._load_port_names()
            hit = match_intent(question, {v[len("gold_"):] for v in schemas})
            if hit:
                name, sql, explanation = hit
                plan = {"sql": sql, "explanation": explanation, "confidence": 0.95, "answer_source": "verified_query",
                        "intent": name, "glossary_terms": []}
        if plan is None:
            plan = self._llm_plan(question, layer, schemas)
        if plan is None:
            return {"unsupported": "no verified query or LLM plan"}
        if plan.get("unsupported"):
            return {"unsupported": plan.get("explanation") or REFUSE_MSG}

        for attempt in range(2):
            check = validate_select(plan["sql"], allowed)
            if not check.ok:
                FEED.emit("insight", f"SQL rejected: {check.reason}", level="warn", event_type="guard_block")
                return {"unsupported": check.reason, "blocked_sql": plan["sql"]}
            res = self.gateway.lakehouse.run_spark_sql(plan["sql"], agent="insight", limit=200, allowed_tables=allowed)
            if res.get("ok"):
                return {**plan, "rows": res["rows"], "columns": res.get("columns", []), "layer": layer}
            if res.get("blocked") or attempt == 1 or plan.get("answer_source") != "llm":
                return {"unsupported": res.get("error") or "query failed", "failed_sql": plan["sql"]}
            repaired = self._llm_plan(question, layer, schemas, error=res.get("error"), previous_sql=plan["sql"])
            if not repaired or repaired.get("unsupported"):
                return {"unsupported": res.get("error") or "query failed", "failed_sql": plan["sql"]}
            plan = repaired
        return {"unsupported": "query failed"}

    def _summarize(self, question: str, rows: list[dict[str, Any]]) -> str | None:
        if not rows:
            return "No matching records in the activated data."
        if LLM.available:
            raw = LLM.complete_json(
                "Answer the user's question in one or two sentences using ONLY the given rows. "
                'Return JSON {"answer": "..."}. Do not invent numbers.',
                json.dumps({"question": question, "rows": rows[:20]}, default=str),
            )
            if raw and raw.get("answer"):
                return str(raw["answer"])
        first = rows[0]
        return "Top result: " + ", ".join(f"{k} = {v}" for k, v in list(first.items())[:5])

    # ------------------------------------------------------------------ ask
    def _refusal(self, question: str, message: str, reason: str | None = None) -> dict[str, Any]:
        FEED.emit("insight", f"Ask refused — {reason or message}", event_type="ask_refused")
        return {
            "ok": False, "blocked": False, "refused": True, "question": question, "message": message,
            "reason": reason, "error": None, "sql": None, "rows": [], "answer_source": "refused", "answer_layer": None,
        }

    def ask(self, question: str) -> dict[str, Any]:
        decision = GUARD.check_adversarial_prompt(question, agent="insight")
        if not decision.allowed:
            return {
                "ok": False, "blocked": True, "refused": False, "question": question, "error": decision.reason,
                "message": decision.reason, "sql": None, "rows": [], "answer_source": "blocked", "answer_layer": None,
            }

        gold_schemas = self._register("gold")
        silver_schemas = self._register("silver")
        vocab = self._vocabulary({**gold_schemas, **silver_schemas})
        if not (_words(question) & vocab):
            return self._refusal(question, REFUSE_MSG, "question is outside the shipping data domain")

        reasons = []
        for layer in ("gold", "silver"):
            ans = self._try_layer(question, layer)
            if ans.get("unsupported"):
                reasons.append(f"{layer}: {ans['unsupported']}")
                continue
            verdict = self.judge.grade_answer(question, ans["sql"], ans["rows"])
            if verdict and verdict["verdict"] == "does_not_answer":
                reasons.append(f"{layer}: judge — {verdict.get('rationale')}")
                continue
            confidence = ans.get("confidence")
            if verdict:
                confidence = round(min(float(confidence or 1.0), verdict["score"]), 2)
            out = {
                "ok": True,
                "blocked": False,
                "refused": False,
                "question": question,
                "sql": ans["sql"],
                "explanation": ans.get("explanation"),
                "glossary_terms": ans.get("glossary_terms", []),
                "rows": ans["rows"],
                "columns": ans.get("columns", []),
                "summary": self._summarize(question, ans["rows"]),
                "confidence": confidence,
                "judge": verdict,
                "answer_source": ans.get("answer_source"),
                "intent": ans.get("intent"),
                "answer_layer": layer.upper(),
                "message": None,
                "error": None,
            }
            FEED.emit("insight", f"answered via {out['answer_source']} on {layer}", event_type="ask_complete",
                      answer_layer=layer)
            return out
        return self._refusal(question, REFUSE_MSG, "; ".join(reasons))

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        q = state.get("demo_question", "Which 5 ports had the longest average anchorage wait (days) in 2021?")
        state["insight_result"] = self.ask(q)
        state["phase"] = "insight_done"
        return state
