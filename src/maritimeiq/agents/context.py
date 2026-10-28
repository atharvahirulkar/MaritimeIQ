"""Context agent: LLM proposes the semantic layer, Judge grades it, gaps go to HITL."""
from __future__ import annotations

from typing import Any

from maritimeiq.agents.judge import JudgeAgent
from maritimeiq.agents.llm import LLM
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONFIGS

# Process these first so corrections on source 1 can teach source 2.
LEARNING_ORDER = ["port_agent_calls", "ais_positions"]
RESOLVED = {"human_approved", "rejected"}


class ContextAgent:
    def __init__(self, gateway: MCPGateway) -> None:
        self.gateway = gateway
        self.store = ContextStore()
        self.judge = JudgeAgent()
        self.system_prompt = (CONFIGS / "prompts" / "context_system.txt").read_text(encoding="utf-8")
        self.auto_threshold = 0.85

    def tables(self) -> list[str]:
        bronze = [t for t in self.gateway.lakehouse.list_tables("bronze")["tables"] if not t.startswith("ref_")]
        return [t for t in LEARNING_ORDER if t in bronze] + sorted(t for t in bronze if t not in LEARNING_ORDER)

    def propose_for_table(self, table: str, prior: dict[str, Any]) -> dict[str, Any]:
        prof = self.gateway.lakehouse.profile_table(f"bronze.{table}", agent="context")
        columns, profiles = prof["columns"], prof["profiles"]
        kept = {c["name"]: c for c in (prior.get("columns") or []) if c.get("status") in RESOLVED}
        todo = [c for c in columns if c not in kept]
        corrections = self.store.read_corrections()
        proposal = LLM.propose_semantics(table, todo, profiles, corrections, self.system_prompt) if todo else None
        source = LLM.last_source if todo else "kept"
        graded = self.judge.grade_semantics(table, [c.model_dump() for c in proposal.columns], profiles) if proposal else []
        graded = [g for g in graded if g.get("name") in profiles]

        for g in graded:
            g["profile"] = profiles.get(g["name"], {})
            ok = g.get("score", 0) >= self.auto_threshold and g.get("grade") in {"correct", "plausible"}
            g["status"] = "auto_approved" if ok else "needs_review"
        all_cols = list(kept.values()) + graded

        approved = [
            {k: v for k, v in c.items() if k != "profile"}
            for c in all_cols
            if c["status"] in {"auto_approved", "human_approved"}
        ]
        self.store.approve_table(table, approved, auto=True)
        pii = [c["name"] for c in approved if c.get("pii")]
        if pii:
            GUARD.register_pii(f"bronze.{table}", pii)
        review_n = sum(1 for c in all_cols if c["status"] == "needs_review")
        FEED.emit(
            "context",
            f"{table}: auto={sum(1 for c in graded if c['status'] == 'auto_approved')} "
            f"review={review_n} kept={len(kept)} (source={source}, corrections_used={len(corrections)})",
            event_type="context_table_done",
            table=table,
        )
        return {
            "table": table,
            "ok": True,
            "columns": all_cols,
            "joins": proposal.joins if proposal else prior.get("joins") or [],
            "needs_review": review_n,
            "corrections_used": len(corrections),
            "proposal_source": source,
        }

    def apply_human_decision(
        self, table: str, column_name: str, decision: str, edited_meaning: str | None = None
    ) -> dict[str, Any]:
        proposals = self.store.load_proposals()
        entry = proposals.get(table) or {"columns": []}
        col = next((c for c in entry["columns"] if c["name"] == column_name), None)
        if col is None:
            return {"ok": False, "error": f"{table}.{column_name} not found"}
        if decision == "edit" and edited_meaning:
            col["meaning"] = edited_meaning
            col["confidence"] = 1.0
        col["status"] = "rejected" if decision == "reject" else "human_approved"
        col["resolved"] = True
        self.store.append_correction(
            {"table": table, "column": column_name, "action": decision, "meaning": col.get("meaning")}
        )
        self.store.save_proposals(proposals)

        sem = self.store.load_semantics()
        tentry = sem.setdefault("tables", {}).setdefault(table, {"columns": [], "approved": True})
        cols = {c["name"]: c for c in tentry.get("columns") or []}
        if decision == "reject":
            cols.pop(column_name, None)
        else:
            cols[column_name] = {k: v for k, v in col.items() if k != "profile"}
        tentry["columns"] = list(cols.values())
        self.store.save_semantics(sem)
        if col.get("pii") and decision != "reject":
            GUARD.register_pii(f"bronze.{table}", [column_name])
        return {"ok": True}

    def run(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(state or {})
        FEED.emit("context", "Building semantic layer from Bronze profiles")
        prior = self.store.load_proposals()
        results = {t: self.propose_for_table(t, prior.get(t) or {}) for t in self.tables()}
        self.store.save_proposals(results)
        order = [t for t in LEARNING_ORDER if t in results]
        state["context_learning"] = {
            "source1": order[0] if order else None,
            "source1_needs_review": results[order[0]]["needs_review"] if order else None,
            "source2": order[1] if len(order) > 1 else None,
            "source2_needs_review": results[order[1]]["needs_review"] if len(order) > 1 else None,
            "open_gaps": sum(r["needs_review"] for r in results.values()),
        }
        self.store.save_json("context_learning.json", state["context_learning"])
        return state
