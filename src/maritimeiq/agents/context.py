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
from maritimeiq.utils.paths import CONFIGS, GROUND_TRUTH


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

    def _gold_meanings_for_table(self, table: str) -> dict[str, str]:
        path = GROUND_TRUTH / "data_dictionary.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        # fuzzy match dictionary keys
        out: dict[str, str] = {}
        for key, cols in data.items():
            if not isinstance(cols, dict):
                continue
            key_l = key.lower()
            if table.replace("_", " ") in key_l or table.split("_")[0] in key_l or (
                table == "port_agent_calls" and "port_agent" in key_l
            ) or (table == "ais_positions" and "ais_positions" in key_l) or (
                table == "ais_live" and "ais_live" in key_l
            ) or (table == "bills_of_lading" and "bills_of_lading" in key_l) or (
                table == "container_events" and "container_events" in key_l
            ) or (table == "customers" and "customers" in key_l) or (
                table == "crew_manifests" and "crew_manifests" in key_l
            ) or (table == "carrier_schedules" and "carrier_schedules" in key_l):
                for col, meaning in cols.items():
                    if col.startswith("_"):
                        continue
                    if isinstance(meaning, str):
                        out[col] = meaning
        return out

    def propose_for_table(self, table: str) -> dict[str, Any]:
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
        gold = self._gold_meanings_for_table(table)
        graded = self.judge.grade_semantics([c.model_dump() for c in proposal.columns], gold)
        approved_cols = []
        needs_review = []
        for g in graded:
            if g.get("score", 0) >= self.auto_threshold and g.get("grade") in {"correct", "plausible"}:
                g["status"] = "auto_approved"
                approved_cols.append(g)
            else:
                g["status"] = "needs_review"
                needs_review.append(g)
                self.gateway.review.create_review_task(
                    "semantic_column",
                    {"table": table, "column": g},
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
        self, table: str, column_name: str, decision: str, edited_meaning: str | None = None
    ) -> dict[str, Any]:
        proposals = self.store.load_proposals()
        table_props = proposals.get(table, {}).get("columns", [])
        col = next((c for c in table_props if c["name"] == column_name), {"name": column_name})
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
                {"table": table, "column": column_name, "action": "approve", "meaning": col.get("meaning")}
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
        if col.get("pii"):
            GUARD.register_pii(f"bronze.{table}", [column_name])
        return {"ok": True}

    def run(self, state: dict[str, Any], tables: list[str] | None = None) -> dict[str, Any]:
        FEED.emit("context", "Building semantic layer")
        tables = tables or self.TABLE_ORDER
        results = {}
        walkthrough = bool(state.get("walkthrough", False))
        # Demonstrate learning: process first port-agent-like then continue
        for t in tables:
            # skip missing
            listed = self.gateway.lakehouse.list_tables("bronze")
            if t not in listed.get("tables", []):
                continue
            results[t] = self.propose_for_table(t)
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
                    # Legacy batch/demo path: soft-approve non-wrong reviews.
                    self.apply_human_decision(t, col["name"], "approve")
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
