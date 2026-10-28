"""Unified HITL queue: semantic gaps (Judge), data gaps (Quality), entity gaps (ER)."""
from __future__ import annotations

from typing import Any

from maritimeiq.agents.entity_resolution import ENTITY_GAPS, apply_entity_decision
from maritimeiq.agents.quality import DATA_GAPS
from maritimeiq.context_store.store import ContextStore

KINDS = ("semantic", "data", "entity")


def open_items(kinds: tuple[str, ...] = KINDS) -> list[dict[str, Any]]:
    """Every open gap as a uniform row, lowest-confidence first."""
    store = ContextStore()
    items: list[dict[str, Any]] = []
    if "semantic" in kinds:
        for table, res in store.load_proposals().items():
            for c in (res or {}).get("columns") or []:
                if c.get("status") != "needs_review":
                    continue
                items.append({
                    "id": f"semantic:{table}:{c['name']}", "kind": "semantic", "table": table,
                    "subject": c["name"], "issue": c.get("judge_rationale") or c.get("rationale") or "",
                    "suggestion": c.get("meaning") or "", "score": float(c.get("score") or 0),
                    "grade": c.get("grade"), "pii": bool(c.get("pii")), "rationale": "",
                    "edit_hint": "Corrected field meaning", "column": c["name"], "count": None,
                })
    for name, kind in ((DATA_GAPS, "data"), (ENTITY_GAPS, "entity")):
        if kind not in kinds:
            continue
        for g in store.load_gaps(name):
            if g.get("status") != "open":
                continue
            hint = ("UN/LOCODE, e.g. SGSIN" if g.get("domain") == "port" else
                    "Canonical code" if kind == "data" else "Master id, e.g. VES-9113721")
            items.append({
                "id": g["id"], "kind": kind, "table": g.get("table") or "",
                "subject": g.get("raw") or g.get("field"), "issue": g.get("detail") or "",
                "suggestion": g.get("suggestion_label") or g.get("suggestion") or "",
                "score": float(g.get("score") or 0), "grade": None, "pii": False, "edit_hint": hint,
                "rationale": g.get("rationale") or "", "count": g.get("count"), "domain": g.get("domain"),
            })
    items.sort(key=lambda x: (x["grade"] != "wrong", x["score"]))
    return items


def apply_decision(orch, item: dict[str, Any], action: str, value: str | None = None) -> dict[str, Any]:
    """Route an approve/reject/edit to the agent that owns the gap."""
    if action not in {"approve", "reject", "edit"}:
        return {"ok": False, "error": f"unknown action {action}"}
    if action == "approve" and not item.get("suggestion"):
        return {"ok": False, "error": "no agent suggestion — use Edit or Reject"}
    if action == "edit" and not (value or "").strip():
        return {"ok": False, "error": "Edit needs a value"}
    if item["kind"] == "semantic":
        return orch.context.apply_human_decision(item["table"], item["column"], action, value)
    if item["kind"] == "data":
        return orch.quality.apply_gap_decision(item["id"], action, value)
    return apply_entity_decision(item["id"], action, value)
