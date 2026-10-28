"""LangGraph-style state machine orchestrating the medallion walkthrough."""
from __future__ import annotations

from typing import Any, TypedDict

from maritimeiq.agents.context import ContextAgent
from maritimeiq.agents.entity_resolution import EntityResolutionAgent
from maritimeiq.agents.gold_builder import GoldBuilderAgent
from maritimeiq.agents.ingestion import IngestionAgent
from maritimeiq.agents.insight import InsightAgent
from maritimeiq.agents.transform import TransformationAgent
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.spark.session import get_spark
from maritimeiq.context_store.store import ContextStore
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import ensure_lakehouse_dirs


def _open_hitl_count() -> int:
    proposals = ContextStore().load_proposals()
    n = 0
    for res in proposals.values():
        if not isinstance(res, dict):
            continue
        for col in res.get("columns", []) or []:
            if col.get("status") == "needs_review":
                n += 1
    return n



class PipelineState(TypedDict, total=False):
    phase: str
    bronze_tables: dict
    semantic_results: dict
    masters: dict
    silver_tables: dict
    insight_result: dict
    gold_mart: dict
    live_metrics: dict
    context_learning: dict
    demo_question: str
    auto_approve_gold: bool
    walkthrough: bool
    hitl_checkpoints: list


def build_gateway() -> MCPGateway:
    spark = get_spark()
    return MCPGateway(spark)


class Orchestrator:
    def __init__(self) -> None:
        ensure_lakehouse_dirs()
        self.spark = get_spark()
        self.gateway = MCPGateway(self.spark)
        self.ingestion = IngestionAgent(self.spark, self.gateway)
        self.context = ContextAgent(self.gateway)
        self.er = EntityResolutionAgent(self.spark, self.gateway)
        self.transform = TransformationAgent(self.spark, self.gateway)
        self.gold_builder = GoldBuilderAgent(self.spark, self.gateway)
        self.insight = InsightAgent(self.spark, self.gateway)

    def _register_bronze_pii_hints(self) -> None:
        from maritimeiq.governance.guard import GUARD
        from maritimeiq.utils.paths import BRONZE

        if not BRONZE.exists():
            return
        for p in BRONZE.iterdir():
            if not p.is_dir():
                continue
            try:
                cols = self.spark.read.format("delta").load(str(p)).columns
            except Exception:  # noqa: BLE001
                continue
            pii = [
                c
                for c in cols
                if any(
                    x in c.lower()
                    for x in [
                        "passport",
                        "email",
                        "phone",
                        "tax",
                        "dob",
                        "birth",
                        "contact",
                        "surname",
                        "family",
                        "given",
                        "seaman",
                        "cdc",
                        "street",
                        "owner",
                    ]
                )
            ]
            if pii:
                GUARD.register_pii(f"bronze.{p.name}", pii)

    def run(self, state: dict[str, Any] | None = None, through: str = "silver") -> dict[str, Any]:
        state = dict(state or {})
        state.setdefault("demo_question", "Which vessels waited longest at anchor off Long Beach?")
        state.setdefault("walkthrough", True)
        state.setdefault("auto_approve_gold", False)
        state["hitl_checkpoints"] = []
        FEED.emit("orchestrator", f"Pipeline start through={through} walkthrough={state['walkthrough']}")

        order = ["bronze", "context", "er", "silver", "gold", "insight"]
        stop_at = through if through in order else "silver"

        state = self.ingestion.run(state)
        self._register_bronze_pii_hints()
        if stop_at == "bronze":
            return state

        state = self.context.run(state)
        open_n = _open_hitl_count()
        if state.get("walkthrough") and (state.get("phase") == "awaiting_hitl" or open_n):
            state["phase"] = "awaiting_hitl"
            state["hitl_checkpoints"].append(
                {"id": 1, "name": "semantic_review", "status": "awaiting_hitl", "open": open_n}
            )
            FEED.emit("orchestrator", f"Paused for HITL — {open_n} open gaps", event_type="hitl_pause")
            # Do not advance past HITL while gaps remain.
            return state
        state["hitl_checkpoints"].append({"id": 1, "name": "semantic_review", "status": "cleared"})
        if stop_at == "context":
            return state

        if state.get("walkthrough") and _open_hitl_count():
            state["phase"] = "awaiting_hitl"
            return state

        state = self.er.run(state)
        state["hitl_checkpoints"].append({"id": 2, "name": "entity_match_review", "status": "tasks_created"})
        if stop_at == "er":
            return state

        state = self.transform.run(state)
        if stop_at == "silver":
            return state

        state = self.gold_builder.run(state)
        state["hitl_checkpoints"].append(
            {
                "id": 3,
                "name": "gold_mart",
                "status": "built" if state.get("gold_mart", {}).get("ok") else "failed",
            }
        )
        if stop_at == "gold":
            return state

        state = self.insight.run(state)
        FEED.emit("orchestrator", "Pipeline complete", phase=state.get("phase"))
        return state


def run_pipeline(through: str = "silver") -> dict[str, Any]:
    return Orchestrator().run(through=through)
