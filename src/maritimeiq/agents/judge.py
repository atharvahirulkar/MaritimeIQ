"""LLM-as-judge with heuristic rubric fallback."""
from __future__ import annotations

import json
from typing import Any

from maritimeiq.agents.llm import LLM
from maritimeiq.utils.logging_feed import FEED


def grade_column_proposal(proposal: dict[str, Any], gold_meaning: str | None) -> dict[str, Any]:
    """Return grade: correct | plausible | wrong."""
    if gold_meaning:
        prop = (proposal.get("meaning") or "").lower()
        gold = gold_meaning.lower()
        prop_toks = set(prop.replace("/", " ").split())
        gold_toks = set(gold.replace("/", " ").split())
        overlap = len(prop_toks & gold_toks) / max(1, len(gold_toks))
        if overlap >= 0.4 or any(t in prop for t in list(gold_toks)[:3]):
            grade = "correct"
            score = 0.9
        elif overlap >= 0.15:
            grade = "plausible"
            score = 0.6
        else:
            grade = "wrong"
            score = 0.2
    else:
        conf = float(proposal.get("confidence") or 0.5)
        grade = "plausible"
        score = conf
    name = (proposal.get("name") or "").lower()
    if any(x in name for x in ["passport", "email", "phone", "tax", "dob"]) and not proposal.get("pii"):
        grade = "wrong"
        score = 0.1
    return {"grade": grade, "score": score, "judge_source": "heuristic"}


class JudgeAgent:
    def grade_semantics(
        self, proposals: list[dict[str, Any]], gold_dict: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        gold_dict = gold_dict or {}
        out: list[dict[str, Any]] = []
        # When LLM available, batch-grade a sample for richer HITL ordering
        llm_grades: dict[str, dict[str, Any]] = {}
        if LLM.available and proposals:
            sample = proposals[:12]
            payload = {
                "task": "grade each column proposal",
                "items": [
                    {
                        "name": p.get("name"),
                        "meaning": p.get("meaning"),
                        "pii": p.get("pii"),
                        "gold_hint": gold_dict.get(p.get("name", "")),
                    }
                    for p in sample
                ],
            }
            raw = LLM.complete_json(
                "You are an independent semantic judge for shipping data. "
                "Return JSON {grades: [{name, grade: correct|plausible|wrong, score: 0-1, rationale}]}.",
                json.dumps(payload),
            )
            if raw and isinstance(raw.get("grades"), list):
                for g in raw["grades"]:
                    if isinstance(g, dict) and g.get("name"):
                        llm_grades[str(g["name"])] = g

        for p in proposals:
            base = grade_column_proposal(p, gold_dict.get(p.get("name", "")))
            lg = llm_grades.get(str(p.get("name")))
            if lg and lg.get("grade") in {"correct", "plausible", "wrong"}:
                base = {
                    "grade": lg["grade"],
                    "score": float(lg.get("score") or base["score"]),
                    "judge_source": "llm",
                    "judge_rationale": lg.get("rationale"),
                }
            out.append({**p, **base})
        FEED.emit(
            "judge",
            f"graded {len(out)} proposals (llm_overlay={len(llm_grades)})",
            event_type="judge_complete",
            llm_graded=len(llm_grades),
        )
        return out

    def grade_insight(self, question: str, sql: str, answer: Any, expected: Any | None = None) -> dict[str, Any]:
        if expected is None:
            return {"grade": "plausible", "score": 0.5, "agree_with_human": None}
        ok = str(answer) == str(expected)
        return {"grade": "correct" if ok else "wrong", "score": 1.0 if ok else 0.0, "agree_with_human": ok}

    def llm_grade(self, context: str) -> dict[str, Any] | None:
        if not LLM.available:
            return None
        return LLM.complete_json(
            "You are an independent judge. Return JSON {grade, score, rationale}.",
            context,
        )
