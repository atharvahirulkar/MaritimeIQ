"""Insight agent: guarded Text-to-SQL with Gold → Silver+semantics → refuse cascade."""
from __future__ import annotations

import json
import re
from typing import Any

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maritimeiq.agents.llm import LLM
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONFIGS, CONTEXT, GOLD, SILVER

REFUSE_MSG = "This question cannot be answered from the activated data."


class InsightAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.system_prompt = (CONFIGS / "prompts" / "insight_system.txt").read_text(encoding="utf-8")
        self.store = ContextStore()

    def live_anchor_berth_metrics(self) -> dict[str, Any]:
        path = SILVER / "ais_live"
        if not path.exists():
            return {"ok": False, "error": "silver.ais_live missing"}
        df = self.spark.read.format("delta").load(str(path))
        local = df.where(
            (F.col("lat").between(33.5, 34.0)) & (F.col("lon").between(-119.0, -117.8))
        )
        from pyspark.sql.window import Window

        w = Window.partitionBy("mmsi").orderBy(F.col("event_ts").desc())
        latest = local.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1)
        at_anchor = latest.where(F.col("nav_status") == 1).count()
        at_berth = latest.where(F.col("nav_status") == 5).count()
        return {
            "ok": True,
            "vessels_at_anchor": at_anchor,
            "vessels_at_berth": at_berth,
            "port": "USLAX/USLGB",
        }

    def _layer_schemas(self, layer: str) -> dict[str, list[str]]:
        root = GOLD if layer == "gold" else SILVER
        out: dict[str, list[str]] = {}
        if not root.exists():
            return out
        for p in root.iterdir():
            if p.is_dir():
                try:
                    out[p.name] = self.spark.read.format("delta").load(str(p)).columns
                except Exception:  # noqa: BLE001
                    continue
        return out

    def _schemas(self) -> dict[str, list[str]]:
        return self._layer_schemas("silver")

    def _gold_catalog(self) -> dict[str, Any]:
        path = CONTEXT / "gold_catalog.yaml"
        if not path.exists():
            return {"tables": [], "materialized": []}
        try:
            return yaml.safe_load(path.read_text(encoding="utf-8")) or {"tables": []}
        except Exception:  # noqa: BLE001
            return {"tables": []}

    def _semantics_payload(self) -> dict[str, Any]:
        sem = self.store.load_semantics()
        glossary = []
        joins = []
        for table, meta in (sem.get("tables") or {}).items():
            for col in meta.get("columns") or []:
                glossary.append(
                    {
                        "table": table,
                        "column": col.get("name"),
                        "meaning": col.get("meaning"),
                        "pii": bool(col.get("pii")),
                    }
                )
                if col.get("code_map"):
                    joins.append({"table": table, "column": col.get("name"), "code_map": col.get("code_map")})
        # Joins saved on proposals
        for table, res in (self.store.load_proposals() or {}).items():
            if isinstance(res, dict):
                for j in res.get("joins") or []:
                    joins.append({"table": table, **(j if isinstance(j, dict) else {"raw": j})})
        return {"glossary": glossary[:180], "joins": joins[:80]}

    def _register_temps(self, layer: str) -> list[str]:
        root = GOLD if layer == "gold" else SILVER
        prefix = "gold_" if layer == "gold" else "silver_"
        names: list[str] = []
        if not root.exists():
            return names
        for p in root.iterdir():
            if not p.is_dir():
                continue
            try:
                self.spark.read.format("delta").load(str(p)).createOrReplaceTempView(f"{prefix}{p.name}")
                names.append(p.name)
            except Exception:  # noqa: BLE001
                continue
        return names

    def _sql_tables(self, sql: str) -> set[str]:
        found = set(re.findall(r"\b(?:from|join)\s+([a-zA-Z0-9_]+)", sql, flags=re.I))
        return {t.lower() for t in found}

    def _validate_sql(self, sql: str, layer: str, allowed: set[str]) -> str | None:
        """Return error message if SQL is not allowlisted for layer."""
        low = sql.lower()
        if "bronze" in low or re.search(r"\bbronze_", low):
            return "Bronze tables are not queryable from Ask."
        refs = self._sql_tables(sql)
        prefix = f"{layer}_"
        for t in refs:
            bare = t[len(prefix) :] if t.startswith(prefix) else t
            if t.startswith(prefix) or bare in allowed or t in {f"{layer}_{a}" for a in allowed}:
                continue
            # CTE names etc. — allow if not looking like silver_/gold_
            if not t.startswith("silver_") and not t.startswith("gold_") and t not in allowed:
                continue
            if t.startswith("silver_") or t.startswith("gold_"):
                if bare not in allowed:
                    return f"Table `{t}` is not in the activated {layer} layer."
        return None

    def _template_sql(self, question: str, layer: str) -> dict[str, Any] | None:
        q = question.lower()
        if layer == "gold":
            gold = self._layer_schemas("gold")
            if "longest" in q and "anchor" in q and "fact_anchorage_wait" in gold:
                return {
                    "sql": """
                    SELECT vessel_name, master_vessel_id, round(wait_h, 2) AS wait_hours
                    FROM gold_fact_anchorage_wait
                    WHERE master_port IN ('USLAX','USLGB')
                    ORDER BY wait_h DESC
                    LIMIT 10
                    """.strip(),
                    "explanation": "Longest ANC→BRT waits from Gold fact_anchorage_wait",
                    "glossary_terms": ["ANC", "BRT"],
                    "confidence": 0.92,
                    "unsupported": False,
                }
            if "anchor" in q and ("month" in q or "highest" in q) and "monthly_port_congestion" in gold:
                return {
                    "sql": """
                    SELECT master_port, month, avg_wait_days, pairs
                    FROM gold_monthly_port_congestion
                    WHERE master_port IN ('USLAX','USLGB')
                    ORDER BY avg_wait_days DESC
                    LIMIT 10
                    """.strip(),
                    "explanation": "Monthly anchorage congestion from Gold mart",
                    "glossary_terms": ["ANC", "BRT"],
                    "confidence": 0.9,
                    "unsupported": False,
                }
            if ("hold" in q or "reassign" in q) and "fact_container_holds" in gold:
                code = "HOLD" if "hold" in q else "ASGN"
                return {
                    "sql": f"""
                    SELECT master_port, carrier, event_count
                    FROM gold_fact_container_holds
                    WHERE event_code = '{code}'
                    ORDER BY event_count DESC
                    LIMIT 15
                    """.strip(),
                    "explanation": f"Container {code} aggregates from Gold",
                    "glossary_terms": [code],
                    "confidence": 0.88,
                    "unsupported": False,
                }
            return None

        # Silver templates (existing demo questions)
        wait_cte = """
            WITH ordered AS (
              SELECT master_vessel_id, vessel_name, master_port, event_code, event_ts,
                     lead(event_code) OVER (PARTITION BY coalesce(master_vessel_id, vessel_name), master_port ORDER BY event_ts) AS next_code,
                     lead(event_ts) OVER (PARTITION BY coalesce(master_vessel_id, vessel_name), master_port ORDER BY event_ts) AS next_ts
              FROM silver_port_agent_events
              WHERE master_port IN ('USLAX','USLGB') AND event_code IN ('ANC','BRT') AND event_ts IS NOT NULL
            ),
            pairs AS (
              SELECT master_vessel_id, vessel_name, master_port, event_ts AS anc_ts, next_ts AS brt_ts,
                     (unix_timestamp(next_ts) - unix_timestamp(event_ts)) / 3600.0 AS wait_h
              FROM ordered
              WHERE event_code = 'ANC' AND next_code = 'BRT'
                AND (unix_timestamp(next_ts) - unix_timestamp(event_ts)) BETWEEN 0 AND 60*24*40
            )
        """
        if "longest" in q and "anchor" in q:
            return {
                "sql": wait_cte
                + """
            SELECT vessel_name, master_vessel_id, round(wait_h, 2) AS wait_hours
            FROM pairs ORDER BY wait_h DESC LIMIT 10
            """,
                "explanation": "Longest ANC→BRT waits from Silver port_agent_events",
                "glossary_terms": ["ANC", "BRT"],
                "confidence": 0.9,
                "unsupported": False,
            }
        if "anchor" in q and (
            "long beach" in q or "los angeles" in q or "la/lb" in q or "highest" in q or "month" in q
        ):
            return {
                "sql": wait_cte
                + """
            SELECT date_format(anc_ts, 'yyyy-MM') AS month,
                   round(avg(wait_h)/24.0, 2) AS avg_wait_days
            FROM pairs GROUP BY 1 ORDER BY avg_wait_days DESC LIMIT 5
            """,
                "explanation": "Average ANC→BRT wait at LA/LB by month (Silver)",
                "glossary_terms": ["ANC", "BRT", "USLAX", "USLGB"],
                "confidence": 0.85,
                "unsupported": False,
            }
        if "reassign" in q or "equipment reassignment" in q:
            return {
                "sql": """
            SELECT carrier, count(*) AS reassignments
            FROM silver_container_events WHERE event_code = 'ASGN'
            GROUP BY carrier ORDER BY reassignments DESC
            """,
                "explanation": "Equipment reassignment counts by carrier (Silver)",
                "glossary_terms": ["ASGN"],
                "confidence": 0.88,
                "unsupported": False,
            }
        if "customs hold" in q:
            return {
                "sql": """
            SELECT master_port, count(*) AS holds
            FROM silver_container_events WHERE event_code = 'HOLD'
            GROUP BY master_port ORDER BY holds DESC LIMIT 10
            """,
                "explanation": "Customs holds by port (Silver)",
                "glossary_terms": ["HOLD"],
                "confidence": 0.88,
                "unsupported": False,
            }
        if "top" in q and "shipper" in q:
            return {
                "sql": """
            SELECT ShipperName, count(*) AS container_rows
            FROM silver_bills_of_lading WHERE ShipperName IS NOT NULL
            GROUP BY ShipperName ORDER BY container_rows DESC LIMIT 10
            """,
                "explanation": "Top shippers by B/L line count (Silver)",
                "glossary_terms": ["ShipperName"],
                "confidence": 0.78,
                "unsupported": False,
            }
        if "roll" in q and ("booking" in q or "share" in q or "sailing" in q):
            return {
                "sql": """
            SELECT year(to_timestamp(IssueDate)) AS yr,
                   count(distinct BLNumber) AS bls,
                   sum(case when version = '2' then 1 else 0 end) AS amended_rows
            FROM silver_bills_of_lading GROUP BY 1 ORDER BY 1
            """,
                "explanation": "Amended B/L rows as roll proxy (Silver)",
                "glossary_terms": ["version", "BLNumber"],
                "confidence": 0.72,
                "unsupported": False,
            }
        return None

    def _llm_plan(self, question: str, layer: str) -> dict[str, Any] | None:
        if not LLM.available:
            return None
        schemas = self._layer_schemas(layer)
        if not schemas:
            return None
        semantics = self._semantics_payload()
        catalog = self._gold_catalog() if layer == "gold" else {}
        payload = {
            "question": question,
            "layer": layer,
            "schemas": schemas,
            "temp_view_prefix": f"{layer}_",
            "semantics": semantics,
            "gold_catalog": catalog if layer == "gold" else None,
            "instructions": (
                f"Write Spark SQL using only {layer}_<table> temp views. "
                "If the question cannot be answered from these schemas/semantics, "
                'return JSON {"unsupported": true, "sql": null, "explanation": "..."}.'
            ),
        }
        raw = LLM.complete_json(self.system_prompt, json.dumps(payload))
        if not raw:
            return None
        if raw.get("unsupported") or not raw.get("sql"):
            return {"unsupported": True, "explanation": raw.get("explanation") or REFUSE_MSG}
        return {
            "sql": raw["sql"],
            "explanation": raw.get("explanation"),
            "glossary_terms": raw.get("glossary_terms") or [],
            "confidence": raw.get("confidence", 0.6),
            "unsupported": False,
        }

    def _plan_for_layer(self, question: str, layer: str) -> dict[str, Any] | None:
        plan = self._template_sql(question, layer)
        source = "template" if plan else None
        if plan is None:
            plan = self._llm_plan(question, layer)
            source = "llm" if plan and not plan.get("unsupported") else source
        if plan is None:
            return None
        if plan.get("unsupported"):
            return {**plan, "answer_source": source or "llm", "layer": layer}
        allowed = set(self._layer_schemas(layer))
        err = self._validate_sql(plan["sql"], layer, allowed)
        if err:
            return {"unsupported": True, "explanation": err, "answer_source": source, "layer": layer}
        plan["answer_source"] = source
        plan["layer"] = layer
        return plan

    def ask(self, question: str, approve_gold: bool = False) -> dict[str, Any]:
        decision = GUARD.check_adversarial_prompt(question, agent="insight")
        if not decision.allowed:
            FEED.emit("guard", decision.reason, level="warn", event_type="guard_block")
            return {
                "ok": False,
                "blocked": True,
                "error": decision.reason,
                "answer_source": "blocked",
                "answer_layer": None,
                "message": decision.reason,
            }

        # Cascade: Gold → Silver+semantics → refuse
        plan = None
        gold_schemas = self._layer_schemas("gold")
        if gold_schemas:
            plan = self._plan_for_layer(question, "gold")
            if plan and plan.get("unsupported"):
                plan = None  # try Silver

        answer_layer = "gold" if plan and not plan.get("unsupported") else None
        if plan is None or plan.get("unsupported"):
            plan = self._plan_for_layer(question, "silver")
            if plan and not plan.get("unsupported"):
                answer_layer = "silver"

        if plan is None or plan.get("unsupported"):
            msg = (plan or {}).get("explanation") or REFUSE_MSG
            FEED.emit("insight", "Ask refused — out of activated data", event_type="ask_refused")
            return {
                "ok": False,
                "blocked": False,
                "refused": True,
                "question": question,
                "message": msg if msg else REFUSE_MSG,
                "error": None,
                "sql": None,
                "rows": [],
                "answer_source": "refused",
                "answer_layer": None,
            }

        layer = answer_layer or plan.get("layer") or "silver"
        self._register_temps(layer)
        # Also register silver when gold SQL might reference nothing else
        if layer == "gold":
            self._register_temps("gold")

        sql = plan["sql"]
        result = self.gateway.lakehouse.run_spark_sql(sql, agent="insight", limit=200)
        out = {
            "ok": result.get("ok", False),
            "question": question,
            "sql": sql,
            "explanation": plan.get("explanation"),
            "glossary_terms": plan.get("glossary_terms", []),
            "rows": result.get("rows", []),
            "error": result.get("error"),
            "confidence": plan.get("confidence"),
            "answer_source": plan.get("answer_source"),
            "answer_layer": layer.upper(),
            "refused": False,
            "message": None,
        }
        FEED.emit(
            "insight",
            f"answered via {out['answer_source']} on {layer}",
            event_type="ask_complete",
            ok=out["ok"],
            answer_layer=layer,
        )
        return out

    def write_gold(self, proposal: dict[str, Any], approval_id: str) -> dict[str, Any]:
        decision = GUARD.check_gold_write(approval_id, agent="insight")
        if not decision.allowed:
            return {"ok": False, "error": decision.reason}
        sql = proposal.get("ddl_or_sql") or ""
        select_sql = sql
        m = re.search(r"\bAS\b\s+(SELECT[\s\S]+)$", sql, flags=re.I)
        if m:
            select_sql = m.group(1)
        if not select_sql.strip().lower().startswith("select"):
            return {"ok": False, "error": "Gold proposal must be SELECT-based for MVP"}
        for name in SILVER.iterdir() if SILVER.exists() else []:
            if name.is_dir():
                try:
                    self.spark.read.format("delta").load(str(name)).createOrReplaceTempView(
                        f"silver_{name.name}"
                    )
                except Exception:  # noqa: BLE001
                    pass
        df = self.spark.sql(select_sql)
        table = proposal["table_name"].replace("gold.", "")
        return self.gateway.lakehouse.write_delta(
            df, f"gold.{table}", agent="insight", approval_id=approval_id
        )

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        q = state.get(
            "demo_question",
            "Which vessels waited longest at anchor off Long Beach?",
        )
        state["insight_result"] = self.ask(q, approve_gold=False)
        state["phase"] = "insight_done"
        return state
