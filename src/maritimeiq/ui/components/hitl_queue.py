"""HITL review queue helpers."""
from __future__ import annotations

from typing import Any

from maritimeiq.context_store.store import ContextStore


def open_review_items() -> list[dict[str, Any]]:
    """Flatten proposals that still need review, sorted by judge score ascending."""
    store = ContextStore()
    proposals = store.load_proposals()
    items: list[dict[str, Any]] = []
    for table, res in proposals.items():
        if not isinstance(res, dict):
            continue
        for col in res.get("columns", []):
            status = col.get("status", "")
            grade = col.get("grade", "plausible")
            score = float(col.get("score") or col.get("confidence") or 0.5)
            # Only unresolved queue items block Continue. Approved/rejected rows
            # (including former grade==wrong) must leave the queue.
            if status != "needs_review":
                continue
            items.append(
                {
                    "table": table,
                    "column": col.get("name"),
                    "meaning": col.get("meaning"),
                    "grade": grade,
                    "score": score,
                    "pii": col.get("pii", False),
                    "rationale": col.get("rationale", ""),
                    "raw": col,
                }
            )
    items.sort(key=lambda x: (0 if x["grade"] == "wrong" else 1, x["score"]))
    return items


def correction_learning_summary(state: dict[str, Any] | None) -> str:
    cl = (state or {}).get("context_learning") or {}
    s1 = cl.get("source1_needs_review")
    s2 = cl.get("source2_needs_review")
    if s1 is None and s2 is None:
        return "Run Context to measure learning (fewer reviews on Source 2 indicates improvement)."
    return (
        f"HITL reviews required — Source 1: {s1}, Source 2: {s2} "
        f"(lower on Source 2 indicates context learning)."
    )
