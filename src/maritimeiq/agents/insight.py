"""Insight agent: guarded Text-to-SQL — Gold first, then Silver + semantics, otherwise refuse.

Guardrails, in order: adversarial-prompt guard → LLM scope decision (``unsupported``) →
table allowlist → Spark analysis (one repair retry) → SQL guard → LLM-as-Judge check
that the result answers the question. Anything that fails every layer is refused with
``REFUSE_MSG``.
"""
from __future__ import annotations

import json
import re
from typing import Any

from pyspark.sql import SparkSession

from maritimeiq.agents.discovery import INSIGHTS, DiscoveryAgent
from maritimeiq.agents.gold_builder import load_gold_catalog
from maritimeiq.agents.judge import JudgeAgent
from maritimeiq.agents.llm import LLM
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONFIGS, GOLD

REFUSE_MSG = "This question cannot be answered from the activated data."
_WORD = re.compile(r"[a-z]{3,}")
_STOP = {"the", "and", "for", "which", "what", "how", "many", "each", "per", "was", "were", "did", "with", "from", "by"}


class InsightAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()
        self.judge = JudgeAgent()
        self.discovery = DiscoveryAgent(spark, gateway)
        self.system_prompt = (CONFIGS / "prompts" / "insight_system.txt").read_text(encoding="utf-8")

    # --- layer context ------------------------------------------------------------
    def _context(self, layer: str) -> tuple[dict[str, list[str]], dict[str, Any]]:
        if layer == "gold":
            views: dict[str, list[str]] = {}
            if GOLD.exists():
                for p in sorted(GOLD.iterdir()):
                    if p.is_dir():
                        df = self.spark.read.format("delta").load(str(p))
                        df.createOrReplaceTempView(f"gold_{p.name}")
                        views[f"gold_{p.name}"] = df.columns
            cat = load_gold_catalog()
            info = {
                "tables": [
                    {"view": t["table_name"].replace("gold.", "gold_"), "kind": t.get("kind"), "grain": t.get("grain"),
                     "description": t.get("description"), "columns": t.get("columns"), "join_keys": t.get("join_keys")}
                    for t in cat.get("tables") or []
                ],
                "relationships": cat.get("relationships") or [],
            }
            return views, info
        schemas = self.discovery.register_views()
        views = {(k if k.startswith("derived_") else f"silver_{k}"): v for k, v in schemas.items()}
        sem = self.store.load_semantics()
        info = {
            "tables": [
                {"view": f"silver_{t}", "description": m.get("description"), "columns": m.get("columns"),
                 "code_map": m.get("code_map")}
                for t, m in (sem.get("silver_tables") or {}).items()
            ],
            "derived_views": (self.store.load_json(INSIGHTS, {}) or {}).get("derived_views") or {},
            "relationships": sem.get("relationships") or [],
        }
        return views, info

    # --- planning / validation ----------------------------------------------------
    def _plan(self, question: str, layer: str, views: dict[str, list[str]], info: dict[str, Any],
              feedback: str | None = None) -> dict[str, Any] | None:
        payload = {
            "question": question, "layer": layer, "views": views, "catalog": info,
            "previous_error": feedback,
        }
        return LLM.complete_json(self.system_prompt, json.dumps(payload, default=str)[:90000])

    def _validate(self, sql: str, allowed: set[str]) -> str | None:
        s = sql.strip().rstrip(";")
        if not re.match(r"^(select|with)\b", s, flags=re.I):
            return "Only SELECT queries are allowed."
        if re.search(r"\bbronze", s, flags=re.I):
            return "Bronze is not queryable from Ask."
        ctes = {c.lower() for c in re.findall(r"(?:\bwith|,)\s*([a-zA-Z_]\w*)\s+as\s*\(", s, flags=re.I)}
        refs = {r.lower() for r in re.findall(r"\b(?:from|join)\s+([a-zA-Z_]\w*)", s, flags=re.I)}
        bad = sorted(refs - ctes - {a.lower() for a in allowed})
        if bad:
            return f"Tables not in this layer: {', '.join(bad)}"
        try:
            self.spark.sql(s).schema
        except Exception as exc:  # noqa: BLE001
            return str(exc).split("\n")[0][:300]
        return None

    def _try_layer(self, question: str, layer: str) -> dict[str, Any]:
        views, info = self._context(layer)
        if not views:
            return {"ok": False, "reason": f"{layer} layer is empty"}
        plan = self._plan(question, layer, views, info)
        if not plan or plan.get("unsupported") or not plan.get("sql"):
            return {"ok": False, "reason": (plan or {}).get("explanation") or "out of scope for this layer"}
        err = self._validate(plan["sql"], set(views))
        if err:
            plan = self._plan(question, layer, views, info, feedback=f"SQL failed validation: {err}") or {}
            if plan.get("unsupported") or not plan.get("sql"):
                return {"ok": False, "reason": plan.get("explanation") or err}
            err = self._validate(plan["sql"], set(views))
            if err:
                return {"ok": False, "reason": err, "sql": plan["sql"]}
        res = self.gateway.lakehouse.run_spark_sql(plan["sql"], agent="insight", limit=200)
        if not res.get("ok"):
            return {"ok": False, "reason": res.get("error"), "sql": plan["sql"]}
        verdict = self.judge.check_answer(question, plan["sql"], res["columns"], res["rows"])
        if not verdict["answers_question"]:
            return {"ok": False, "reason": f"Judge rejected: {verdict['rationale']}", "sql": plan["sql"], "judge": verdict}
        return {"ok": True, "plan": plan, "result": res, "judge": verdict}

    # --- public -------------------------------------------------------------------
    def ask(self, question: str) -> dict[str, Any]:
        decision = GUARD.check_adversarial_prompt(question, agent="insight")
        if not decision.allowed:
            FEED.emit("guard", decision.reason, level="warn", event_type="guard_block")
            return {"ok": False, "blocked": True, "refused": False, "question": question,
                    "message": decision.reason, "answer_layer": None, "rows": []}
        if not LLM.available:
            return self._ask_offline(question)

        trace = []
        for layer in ("gold", "silver"):
            out = self._try_layer(question, layer)
            trace.append({"layer": layer.upper(), "answered": out["ok"], "reason": out.get("reason"), "sql": out.get("sql")})
            if out["ok"]:
                plan, res = out["plan"], out["result"]
                FEED.emit("insight", f"answered on {layer}", event_type="ask_complete", answer_layer=layer)
                return {
                    "ok": True, "blocked": False, "refused": False, "question": question,
                    "answer_layer": layer.upper(), "answer_source": "llm", "sql": plan["sql"],
                    "explanation": plan.get("explanation"), "glossary_terms": plan.get("glossary_terms") or [],
                    "confidence": plan.get("confidence"), "columns": res["columns"], "rows": res["rows"],
                    "judge": out["judge"], "trace": trace,
                }
        FEED.emit("insight", "Ask refused — not answerable from activated data", event_type="ask_refused")
        return {"ok": False, "blocked": False, "refused": True, "question": question, "message": REFUSE_MSG,
                "answer_layer": None, "rows": [], "trace": trace}

    def _ask_offline(self, question: str) -> dict[str, Any]:
        """No LLM: only questions that match a discovered finding can be answered."""
        q = set(_WORD.findall(question.lower())) - _STOP
        best, best_score = None, 0.0
        for f in (self.store.load_json(INSIGHTS, {}) or {}).get("findings") or []:
            words = set(_WORD.findall(f"{f['title']} {f['id']}".lower().replace("_", " "))) - _STOP
            score = len(q & words) / max(1, len(words))
            if score > best_score:
                best, best_score = f, score
        if best is None or best_score < 0.5:
            return {"ok": False, "blocked": False, "refused": True, "question": question, "message": REFUSE_MSG,
                    "answer_layer": None, "rows": [],
                    "trace": [{"layer": "—", "answered": False, "reason": "No LLM configured; question matches no finding"}]}
        layer = "GOLD" if (GOLD / best["gold_table"]).exists() else "SILVER"
        views, _ = self._context(layer.lower())
        sql = f"SELECT * FROM gold_{best['gold_table']}" if layer == "GOLD" else best["sql"]
        res = self.gateway.lakehouse.run_spark_sql(sql, agent="insight", limit=200)
        return {"ok": res.get("ok", False), "blocked": False, "refused": False, "question": question,
                "answer_layer": layer, "answer_source": "finding_match", "sql": sql,
                "explanation": f"Matched discovered finding: {best['title']}", "columns": res.get("columns"),
                "rows": res.get("rows", []), "confidence": round(best_score, 2), "trace": []}
