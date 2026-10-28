"""Orchestrator: runs one activation stage at a time; HITL gates sit between stages.

Stages read their inputs from the lakehouse/context store, so re-running one stage never
replays earlier ones (and never discards human decisions).

    ingest → semantics → quality → [HITL: semantic + data gaps] → connect →
    [HITL: entity gaps] → silver → discover → gold → ask
"""
from __future__ import annotations

import time
from typing import Any, Callable

from maritimeiq.agents.context import ContextAgent
from maritimeiq.agents.discovery import DiscoveryAgent
from maritimeiq.agents.entity_resolution import ENTITY_GAPS, EntityResolutionAgent, apply_entity_decision
from maritimeiq.agents.gold_builder import GoldBuilderAgent
from maritimeiq.agents.ingestion import IngestionAgent
from maritimeiq.agents.insight import InsightAgent
from maritimeiq.agents.quality import DATA_GAPS, QualityAgent
from maritimeiq.agents.transform import TransformationAgent
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.spark.session import get_spark
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, ensure_lakehouse_dirs

STAGES = ["ingest", "semantics", "quality", "connect", "silver", "discover", "gold"]
STAGE_RUNS = "stage_runs.json"


def open_gap_counts() -> dict[str, int]:
    store = ContextStore()
    semantic = sum(
        1
        for res in store.load_proposals().values()
        if isinstance(res, dict)
        for c in res.get("columns") or []
        if c.get("status") == "needs_review"
    )
    data = sum(1 for g in store.load_gaps(DATA_GAPS) if g.get("status") == "open")
    entity = sum(1 for g in store.load_gaps(ENTITY_GAPS) if g.get("status") == "open")
    return {"semantic": semantic, "data": data, "entity": entity}


class Orchestrator:
    def __init__(self) -> None:
        ensure_lakehouse_dirs()
        self.spark = get_spark()
        self.gateway = MCPGateway(self.spark)
        self.ingestion = IngestionAgent(self.spark, self.gateway)
        self.context = ContextAgent(self.gateway)
        self.quality = QualityAgent(self.gateway)
        self.er = EntityResolutionAgent(self.spark, self.gateway)
        self.transform = TransformationAgent(self.spark, self.gateway)
        self.discovery = DiscoveryAgent(self.spark, self.gateway)
        self.gold_builder = GoldBuilderAgent(self.spark, self.gateway)
        self.insight = InsightAgent(self.spark, self.gateway)
        self.register_pii_tags()

    def register_pii_tags(self) -> None:
        """Re-hydrate guard PII tags (in-memory) from Bronze column names + the semantic layer."""
        store = ContextStore()
        hints = ("passport", "email", "phone", "tax", "dob", "birth", "contact", "surname",
                 "family", "given", "first_name", "seaman", "cdc", "street", "owner")
        if not BRONZE.exists():
            return
        for p in BRONZE.iterdir():
            if not p.is_dir() or p.name.startswith("ref_"):
                continue
            try:
                cols = self.spark.read.format("delta").load(str(p)).columns
            except Exception:  # noqa: BLE001
                continue
            pii = [c for c in cols if any(h in c.lower() for h in hints)] + store.pii_columns(p.name)
            if pii:
                GUARD.register_pii(f"bronze.{p.name}", pii)

    def _stage_fn(self, stage: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
        return {
            "ingest": self.ingestion.run,
            "semantics": self.context.run,
            "quality": self.quality.run,
            "connect": self.er.run,
            "silver": self._silver,
            "discover": self.discovery.run,
            "gold": self.gold_builder.run,
        }[stage]

    def _silver(self, state: dict[str, Any]) -> dict[str, Any]:
        # Replay ER so entity decisions made in HITL are reflected in the masters.
        state = self.er.run(state)
        return self.transform.run(state)

    def run_stage(self, stage: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage}")
        FEED.emit("orchestrator", f"Stage start: {stage}", event_type="stage_start", stage=stage)
        state = self._stage_fn(stage)(dict(state or {}))
        if stage == "ingest":
            self.register_pii_tags()
        store = ContextStore()
        runs = store.load_json(STAGE_RUNS, {}) or {}
        runs[stage] = time.time()
        if stage == "silver":
            runs["connect"] = runs[stage]
        store.save_json(STAGE_RUNS, runs)
        state["last_stage"] = stage
        state["open_gaps"] = open_gap_counts()
        FEED.emit("orchestrator", f"Stage done: {stage}", event_type="stage_done", stage=stage,
                  open_gaps=state["open_gaps"])
        return state

    def accept_agent_suggestions(self) -> int:
        """Batch mode only: accept every agent suggestion in place of a human reviewer."""
        store = ContextStore()
        n = 0
        for table, res in store.load_proposals().items():
            for c in (res or {}).get("columns") or []:
                if c.get("status") == "needs_review":
                    self.context.apply_human_decision(table, c["name"], "approve")
                    n += 1
        for g in store.load_gaps(DATA_GAPS):
            if g.get("status") == "open":
                self.quality.apply_gap_decision(g["id"], "approve" if g.get("suggestion") else "reject")
                n += 1
        for g in store.load_gaps(ENTITY_GAPS):
            if g.get("status") == "open":
                apply_entity_decision(g["id"], "approve")
                n += 1
        return n

    def run_all(self, skip_ingest: bool = False) -> dict[str, Any]:
        """Unattended run for CLI/eval: agent suggestions stand in for HITL decisions."""
        state: dict[str, Any] = {}
        for stage in STAGES:
            if stage == "ingest" and skip_ingest:
                continue
            state = self.run_stage(stage, state)
            if stage in {"quality", "connect"}:
                state["auto_accepted"] = state.get("auto_accepted", 0) + self.accept_agent_suggestions()
        return state
