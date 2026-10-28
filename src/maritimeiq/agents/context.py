"""Context agent: propose semantic layer with confidence + HITL queue."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from maritimeiq.agents.judge import JudgeAgent
from maritimeiq.agents.llm import LLM
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONFIGS


class ContextAgent:
    # Process tables in an order that demonstrates learning across agencies/feeds
    TABLE_ORDER = [
        "port_agent_calls",
        "ais_positions",
        "ais_live",
        "carrier_schedules",
        "bills_of_lading",
        "container_events",
        "customers",
        "crew_manifests",
    ]

    def __init__(self, gateway: MCPGateway) -> None:
        self.gateway = gateway
        self.store = ContextStore()
        self.judge = JudgeAgent()
        self.system_prompt = (CONFIGS / "prompts" / "context_system.txt").read_text(encoding="utf-8")
        self.auto_threshold = 0.85

    def propose_for_table(self, table: str, prior_columns: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        sample = self.gateway.lakehouse.read_sample(f"bronze.{table}", n=5, agent="context")
        if not sample.get("ok"):
            return {"table": table, "ok": False, "error": sample.get("error")}
        columns = [c for c in sample.get("columns", []) if not c.startswith("_")]
        # Bound cost: large tables get fewer profiles
        limit = 12 if table in {"container_events", "port_agent_calls", "ais_positions"} else 25
        profiles = {}
        for c in columns[:limit]:
            profiles[c] = self.gateway.lakehouse.profile_column(f"bronze.{table}", c, agent="context")
        # Still propose heuristics for remaining columns without full profile
        for c in columns[limit:]:
            profiles[c] = {"ok": True, "samples": [], "stats": {}}
        corrections = self.store.read_corrections()
        proposal = LLM.propose_semantics(table, columns, profiles, corrections, self.system_prompt)
        source = LLM.last_source
        FEED.emit(
            "context",
            f"proposals ready for {table} via {source}",
            event_type="proposal_ready",
            table=table,
            source=source,
            n_columns=len(proposal.columns),
        )
        # Judge sees only the proposal + profile evidence (never ground truth)
        graded = self.judge.grade_semantics([c.model_dump() for c in proposal.columns], profiles)
        prior = {c.get("name"): c for c in (prior_columns or [])}
        approved_cols = []
        needs_review = []
        for g in graded:
            human = prior.get(g.get("name"))
            if human and human.get("status") in {"human_approved", "rejected"}:
                # Re-runs never undo a human decision
                g.update({k: human[k] for k in ("meaning", "pii", "code_map", "status", "resolved") if k in human})
                if g["status"] == "human_approved":
                    approved_cols.append(g)
                continue
            if g.get("score", 0) >= self.auto_threshold and g.get("grade") in {"correct", "plausible"}:
                g["status"] = "auto_approved"
                approved_cols.append(g)
            else:
                g["status"] = "needs_review"
                needs_review.append(g)
                self.gateway.review.create_review_task(
                    "semantic_column",
                    {"table": table, "column": g.get("name"), "grade": g.get("grade"), "score": g.get("score")},
                    agent="context",
                )
        if needs_review:
            FEED.emit(
                "context",
                f"HITL waiting on {table}: {len(needs_review)} columns",
                event_type="hitl_waiting",
                table=table,
                count=len(needs_review),
            )
        if approved_cols:
            existing = self.store.load_semantics().get("tables", {}).get(table, {}).get("columns", [])
            by_name = {c["name"]: c for c in existing}
            for c in approved_cols:
                by_name[c["name"]] = c
            self.store.approve_table(table, list(by_name.values()), auto=True)
            pii_cols = [c["name"] for c in by_name.values() if c.get("pii")]
            if pii_cols:
                GUARD.register_pii(f"bronze.{table}", pii_cols)
                GUARD.register_pii(f"silver.{table}", pii_cols)

        FEED.emit(
            "context",
            f"{table}: auto={len(approved_cols)} review={len(needs_review)} "
            f"(source={source}, corrections_used={len(corrections)})",
            event_type="context_table_done",
            table=table,
            source=source,
        )
        return {
            "table": table,
            "ok": True,
            "columns": graded,
            "joins": proposal.joins,
            "auto_approved": len(approved_cols),
            "needs_review": len(needs_review),
            "corrections_used": len(corrections),
            "proposal_source": source,
        }

    def apply_human_decision(
        self,
        table: str,
        column_name: str,
        decision: str,
        edited_meaning: str | None = None,
        approver: str = "human",
        pii: bool | None = None,
    ) -> dict[str, Any]:
        proposals = self.store.load_proposals()
        table_props = proposals.get(table, {}).get("columns", [])
        col = next((c for c in table_props if c["name"] == column_name), {"name": column_name})
        if pii is not None:
            col["pii"] = bool(pii)
        col["approver"] = approver
        if decision == "reject":
            self.store.append_correction(
                {"table": table, "column": column_name, "action": "reject", "prior": col}
            )
            for c in table_props:
                if c.get("name") == column_name:
                    c["status"] = "rejected"
                    c["resolved"] = True
            proposals[table] = {**(proposals.get(table) or {}), "columns": table_props}
            self.store.save_proposals(proposals)
            self._invalidate_silver()
            return {"ok": True}
        if decision == "edit" and edited_meaning:
            col["meaning"] = edited_meaning
            col["confidence"] = 1.0
            self.store.append_correction(
                {
                    "table": table,
                    "column": column_name,
                    "action": "edit",
                    "meaning": edited_meaning,
                }
            )
        else:
            self.store.append_correction(
                {"table": table, "column": column_name, "action": "approve", "meaning": col.get("meaning"),
                 "pii": col.get("pii"), "approver": approver}
            )
        sem = self.store.load_semantics()
        tables = sem.setdefault("tables", {})
        entry = tables.setdefault(table, {"columns": [], "approved": True})
        cols = {c["name"]: c for c in entry.get("columns", [])}
        cols[column_name] = {**col, "status": "human_approved"}
        entry["columns"] = list(cols.values())
        self.store.save_semantics(sem)
        # Clear gap from proposals queue (status alone gates open_review_items)
        for c in table_props:
            if c.get("name") == column_name:
                c["status"] = "human_approved" if decision != "reject" else "rejected"
                c["resolved"] = True
                if decision == "edit" and edited_meaning:
                    c["meaning"] = edited_meaning
        proposals[table] = {**(proposals.get(table) or {}), "columns": table_props}
        self.store.save_proposals(proposals)
        for c in table_props:
            if c.get("name") == column_name and pii is not None:
                c["pii"] = bool(pii)
        self.store.save_proposals(proposals)
        if col.get("pii"):
            GUARD.register_pii(f"bronze.{table}", [column_name])
            GUARD.register_pii(f"silver.{table}", [column_name])
        self._invalidate_silver()
        return {"ok": True}

    @staticmethod
    def _invalidate_silver() -> None:
        """Semantics changed → Silver (and everything after it) must be rebuilt."""
        from maritimeiq.agents.orchestrator import invalidate_from

        invalidate_from("silver")

    def run(self, state: dict[str, Any], tables: list[str] | None = None) -> dict[str, Any]:
        FEED.emit("context", "Building semantic layer")
        listed = self.gateway.lakehouse.list_tables("bronze", agent="context").get("tables", [])
        # Known tables first (demonstrates correction learning), then any new source
        ordered = [t for t in self.TABLE_ORDER if t in listed] + sorted(
            t for t in listed if t not in self.TABLE_ORDER and not t.startswith("ref_")
        )
        tables = tables or ordered
        prior = self.store.load_proposals() or {}
        results = {}
        walkthrough = bool(state.get("walkthrough", False))
        for t in tables:
            if t not in listed:
                continue
            results[t] = self.propose_for_table(t, (prior.get(t) or {}).get("columns"))
        self.store.save_proposals(results)

        open_gaps = 0
        for t, res in results.items():
            for col in res.get("columns", []):
                if col.get("status") != "needs_review":
                    continue
                if walkthrough:
                    # Leave gaps for HITL step — do not silent-approve.
                    open_gaps += 1
                    if col.get("grade") == "wrong":
                        FEED.emit("context", f"HITL required for {t}.{col['name']}", level="warn")
                elif col.get("grade") != "wrong":
                    # Unattended batch run: policy approves non-wrong gaps; recorded as such, not as a human.
                    self.apply_human_decision(t, col["name"], "approve", approver="batch-policy")
                else:
                    FEED.emit("context", f"HITL required for {t}.{col['name']}", level="warn")
                    open_gaps += 1

        state["semantic_results"] = results
        state["phase"] = "awaiting_hitl" if (walkthrough and open_gaps) else "context_done"
        # Learning metric: corrections needed proxy
        first = results.get("port_agent_calls", {})
        second = results.get("ais_positions", {})
        state["context_learning"] = {
            "source1_needs_review": first.get("needs_review"),
            "source2_needs_review": second.get("needs_review"),
            "open_gaps": open_gaps,
        }
        return state
