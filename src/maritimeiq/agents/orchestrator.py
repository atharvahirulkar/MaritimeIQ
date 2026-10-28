"""LangGraph state machine orchestrating the medallion walkthrough with HITL gates.

Stages run once and are checkpointed in ``lakehouse/context/pipeline_progress.json``;
``run(through=X)`` resumes from the first incomplete stage instead of starting over.
Human decisions invalidate downstream stages (e.g. a semantic edit → Silver/Gold rebuild).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, TypedDict

from maritimeiq.agents.context import ContextAgent
from maritimeiq.agents.entity_resolution import EntityResolutionAgent, open_er_items
from maritimeiq.agents.gold_builder import GoldBuilderAgent
from maritimeiq.agents.ingestion import IngestionAgent
from maritimeiq.agents.insight import InsightAgent
from maritimeiq.agents.transform import TransformationAgent
from maritimeiq.context_store.store import ContextStore
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.spark.session import get_spark
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONTEXT, ensure_lakehouse_dirs

STAGES = ["bronze", "context", "er", "silver", "gold", "insight"]
PROGRESS_PATH = CONTEXT / "pipeline_progress.json"
STAGE_PHASE = {
    "bronze": "bronze_done",
    "context": "context_done",
    "er": "er_done",
    "silver": "silver_done",
    "gold": "gold_done",
    "insight": "insight_done",
}


def load_progress() -> dict[str, str]:
    if not PROGRESS_PATH.exists():
        return {}
    try:
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8")).get("completed", {})
    except Exception:  # noqa: BLE001
        return {}


def _save_progress(done: dict[str, str]) -> None:
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps({"completed": done}, indent=2), encoding="utf-8")


def mark_done(stage: str) -> None:
    done = load_progress()
    done[stage] = datetime.now(timezone.utc).isoformat()
    _save_progress(done)


def invalidate_from(stage: str) -> None:
    """Forget completion of ``stage`` and every later stage."""
    if stage not in STAGES:
        return
    keep = STAGES[: STAGES.index(stage)]
    done = {k: v for k, v in load_progress().items() if k in keep}
    _save_progress(done)


def reset_progress() -> None:
    _save_progress({})


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
    through: str
    walkthrough: bool
    auto_approve_gold: bool
    gold_approval_id: str
    gold_selected_kpis: list
    bronze_tables: dict
    semantic_results: dict
    masters: dict
    silver_tables: dict
    silver_qc: dict
    gold_mart: dict
    insight_result: dict
    live_metrics: dict
    context_learning: dict
    demo_question: str
    hitl_checkpoints: list
    stopped_reason: str
    completed_stages: list


STATE_KEYS = set(PipelineState.__annotations__)


def build_gateway() -> MCPGateway:
    return MCPGateway(get_spark())


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
        self._graph = None

    # ---------------------------------------------------------------- stages
    def _register_bronze_pii_hints(self) -> None:
        from maritimeiq.governance.guard import GUARD
        from maritimeiq.utils.paths import BRONZE

        if not BRONZE.exists():
            return
        hints = ["passport", "email", "phone", "tax", "dob", "birth", "contact", "surname", "family",
                 "given", "seaman", "cdc", "street", "owner"]
        for p in BRONZE.iterdir():
            if not p.is_dir():
                continue
            try:
                cols = self.spark.read.format("delta").load(str(p)).columns
            except Exception:  # noqa: BLE001
                continue
            pii = [c for c in cols if any(x in c.lower() for x in hints)]
            if pii:
                GUARD.register_pii(f"bronze.{p.name}", pii)

    def _run_bronze(self, state: dict[str, Any]) -> dict[str, Any]:
        state = self.ingestion.run(state)
        self._register_bronze_pii_hints()
        return state

    def _run_gold(self, state: dict[str, Any]) -> dict[str, Any]:
        if state.get("auto_approve_gold") and not state.get("gold_approval_id"):
            # Explicit operator opt-in (CLI --auto-approve-gold), recorded as such in approvals.jsonl
            appr = self.gateway.review.get_approval(
                "gold.relational_mart", agent="human", approver="batch-cli --auto-approve-gold"
            )
            state["gold_approval_id"] = appr["approval_id"]
        return self.gold_builder.run(state)

    def _stage_fns(self) -> dict[str, Callable[[dict[str, Any]], dict[str, Any]]]:
        return {
            "bronze": self._run_bronze,
            "context": self.context.run,
            "er": self.er.run,
            "silver": self.transform.run,
            "gold": self._run_gold,
            "insight": self.insight.run,
        }

    # ----------------------------------------------------------------- gates
    def _gate(self, stage: str, state: dict[str, Any]) -> str | None:
        """Return a pause phase when a human must act before the next stage."""
        walk = bool(state.get("walkthrough"))
        if stage == "context" and walk and _open_hitl_count():
            return "awaiting_hitl"
        if stage == "er" and walk and open_er_items():
            return "awaiting_er_review"
        if stage == "gold" and state.get("phase") == "awaiting_gold_approval":
            return "awaiting_gold_approval"
        return None

    def _next_stage(self, state: dict[str, Any]) -> str | None:
        through = state.get("through", "silver")
        last = STAGES.index(through) if through in STAGES else STAGES.index("silver")
        done = load_progress()
        for i, s in enumerate(STAGES[: last + 1]):
            if s not in done:
                # A gate left open on a completed predecessor still blocks
                if i > 0:
                    pause = self._gate(STAGES[i - 1], {**state, "phase": state.get("phase")})
                    if pause and pause != "awaiting_gold_approval":
                        state["phase"] = pause
                        state["stopped_reason"] = f"{pause} before {s}"
                        return None
                return s
        return None

    def _execute(self, stage: str, state: dict[str, Any]) -> dict[str, Any]:
        FEED.emit("orchestrator", f"stage {stage} start", event_type="stage_start", stage=stage)
        state = self._stage_fns()[stage](dict(state))
        pause = self._gate(stage, state)
        if pause == "awaiting_gold_approval":
            state["phase"] = pause
            state["stopped_reason"] = "Gold plan awaiting human approval"
            FEED.emit("orchestrator", "Paused — Gold plan awaiting approval", event_type="hitl_pause")
            return state
        mark_done(stage)
        if pause:
            state["phase"] = pause
            state["stopped_reason"] = pause
            FEED.emit("orchestrator", f"Paused after {stage}: {pause}", event_type="hitl_pause")
        else:
            state["phase"] = STAGE_PHASE[stage]
        return state

    # ------------------------------------------------------------- LangGraph
    def _graph_app(self):
        if self._graph is not None:
            return self._graph
        try:
            from langgraph.graph import END, START, StateGraph
        except Exception:  # noqa: BLE001
            self._graph = False
            return self._graph

        def node(stage: str):
            def _fn(state: PipelineState) -> dict[str, Any]:
                out = self._execute(stage, dict(state))
                return {k: v for k, v in out.items() if k in STATE_KEYS}

            return _fn

        def route(state: PipelineState):
            if str(state.get("phase", "")).startswith("awaiting_"):
                return END
            nxt = self._next_stage(dict(state))
            return nxt or END

        g = StateGraph(PipelineState)
        for s in STAGES:
            g.add_node(s, node(s))
            g.add_conditional_edges(s, route)
        g.add_conditional_edges(START, route)
        self._graph = g.compile()
        return self._graph

    def run(self, state: dict[str, Any] | None = None, through: str = "silver", fresh: bool = False) -> dict[str, Any]:
        state = dict(state or {})
        state.setdefault("demo_question", "Which 5 ports had the longest average anchorage wait (days) in 2021?")
        state.setdefault("walkthrough", True)
        state.setdefault("auto_approve_gold", False)
        state["through"] = through if through in STAGES else "silver"
        state.pop("stopped_reason", None)
        if str(state.get("phase", "")).startswith("awaiting_"):
            state["phase"] = "resuming"
        if fresh:
            reset_progress()
        FEED.emit("orchestrator", f"Pipeline run through={through} fresh={fresh} walkthrough={state['walkthrough']}")

        app = self._graph_app()
        if app:
            out = app.invoke({k: v for k, v in state.items() if k in STATE_KEYS}, {"recursion_limit": 50})
            state.update(out)
        else:  # pragma: no cover - langgraph missing
            while not str(state.get("phase", "")).startswith("awaiting_"):
                nxt = self._next_stage(state)
                if not nxt:
                    break
                state = self._execute(nxt, state)

        state["completed_stages"] = [s for s in STAGES if s in load_progress()]
        if not str(state.get("phase", "")).startswith("awaiting_"):
            if state["completed_stages"]:
                state["phase"] = STAGE_PHASE[state["completed_stages"][-1]]
            # Routing functions cannot write state, so re-evaluate the gate that stopped the graph
            self._next_stage(state)
        state["hitl_checkpoints"] = [
            {"id": 1, "name": "semantic_review", "open": _open_hitl_count()},
            {"id": 2, "name": "entity_match_review", "open": len(open_er_items())},
            {"id": 3, "name": "gold_approval", "status": "approved" if "gold" in load_progress() else "pending"},
        ]
        FEED.emit("orchestrator", f"Pipeline at phase={state.get('phase')}", phase=state.get("phase"))
        return state


def run_pipeline(through: str = "silver") -> dict[str, Any]:
    return Orchestrator().run(through=through)
