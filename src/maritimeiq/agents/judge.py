"""LLM-as-Judge: independently checks agent outputs (semantics, Text-to-SQL answers).

The judge only sees what the pipeline sees (masked profiles, schemas, results) — never
ground truth. Ground truth is used later by the eval harness to measure judge accuracy.
"""
from __future__ import annotations

import json
import re
from typing import Any

from maritimeiq.agents.llm import LLM
from maritimeiq.utils.logging_feed import FEED

PII_NAME = re.compile(r"passport|email|phone|tax|dob|birth|contact|seaman|cdc|surname|family|given|first_?name|street", re.I)
GENERIC_MEANING = re.compile(r"^field `.+` from source profile$", re.I)


def _numeric(samples: list[str]) -> list[float]:
    out = []
    for s in samples:
        try:
            out.append(float(s))
        except (TypeError, ValueError):
            continue
    return out


def heuristic_grade(p: dict[str, Any], profile: dict[str, Any] | None) -> dict[str, Any]:
    """Consistency checks between the proposal, the column name, and its samples."""
    name = str(p.get("name") or "")
    meaning = str(p.get("meaning") or "").strip()
    samples = (profile or {}).get("samples") or []
    conf = float(p.get("confidence") or 0.5)
    issues: list[str] = []
    if not meaning or GENERIC_MEANING.match(meaning):
        issues.append("agent could not infer a meaning")
    if PII_NAME.search(name) and not p.get("pii"):
        issues.append("name suggests PII but not tagged")
    low = meaning.lower()
    nums = _numeric(samples)
    if "latitude" in low and nums and any(abs(v) > 90 for v in nums):
        issues.append("latitude samples out of range")
    if "longitude" in low and nums and any(abs(v) > 180 for v in nums):
        issues.append("longitude samples out of range")
    if "imo" in low and samples and not any(re.search(r"\d{7}", s) for s in samples):
        issues.append("no 7-digit IMO in samples")
    if "status" in low or "code" in low:
        if samples and not p.get("code_map"):
            issues.append("coded field without a code dictionary")
    if issues:
        hard = any(k in i for i in issues for k in ("could not infer", "PII", "out of range"))
        return {"grade": "wrong" if hard else "plausible", "score": 0.2 if hard else min(conf, 0.6),
                "judge_source": "heuristic", "judge_rationale": "; ".join(issues)}
    grade = "correct" if conf >= 0.85 else "plausible"
    return {"grade": grade, "score": conf, "judge_source": "heuristic",
            "judge_rationale": "consistent with name and samples"}


class JudgeAgent:
    BATCH = 30

    def grade_semantics(
        self, table: str, proposals: list[dict[str, Any]], profiles: dict[str, Any]
    ) -> list[dict[str, Any]]:
        llm_grades: dict[str, dict[str, Any]] = {}
        if LLM.available:
            for i in range(0, len(proposals), self.BATCH):
                batch = proposals[i : i + self.BATCH]
                payload = {
                    "table": table,
                    "items": [
                        {
                            "name": p.get("name"),
                            "proposed_meaning": p.get("meaning"),
                            "proposed_pii": p.get("pii"),
                            "proposed_code_map": p.get("code_map") or {},
                            "samples": (profiles.get(p.get("name")) or {}).get("samples", [])[:6],
                            "stats": (profiles.get(p.get("name")) or {}).get("stats", {}),
                        }
                        for p in batch
                    ],
                }
                raw = LLM.complete_json(
                    "You are an independent data-quality judge for shipping/logistics data. For each "
                    "column, decide whether the proposed meaning, PII flag, and code map are supported "
                    "by the column name and samples (samples may be masked tokens). Be strict: generic or "
                    "unsupported meanings are 'wrong'. Return JSON {\"grades\": [{\"name\", \"grade\": "
                    "\"correct|plausible|wrong\", \"score\": 0-1, \"rationale\"}]}.",
                    json.dumps(payload, default=str),
                )
                for g in (raw or {}).get("grades") or []:
                    if isinstance(g, dict) and g.get("name") and g.get("grade") in {"correct", "plausible", "wrong"}:
                        llm_grades[str(g["name"])] = g
        out = []
        for p in proposals:
            base = heuristic_grade(p, profiles.get(p.get("name")))
            lg = llm_grades.get(str(p.get("name")))
            if lg:
                score = float(lg.get("score") or base["score"])
                # PII safety net: the judge may not clear an untagged PII-looking column.
                if base["grade"] == "wrong" and "PII" in base["judge_rationale"]:
                    lg = {**lg, "grade": "wrong"}
                    score = min(score, 0.2)
                base = {"grade": lg["grade"], "score": score, "judge_source": "llm",
                        "judge_rationale": lg.get("rationale") or ""}
            out.append({**p, **base})
        FEED.emit("judge", f"{table}: graded {len(out)} proposals (llm={len(llm_grades)})",
                  event_type="judge_complete", table=table)
        return out

    def check_answer(
        self, question: str, sql: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Does the SQL result actually answer the question? Used as an Ask guardrail."""
        if not LLM.available:
            ok = bool(rows)
            return {"answers_question": ok, "score": 0.5 if ok else 0.0, "judge_source": "heuristic",
                    "rationale": "result returned" if ok else "empty result"}
        raw = LLM.complete_json(
            "You are a strict judge of Text-to-SQL answers. Given a question, the SQL that was run, and "
            "the first result rows, decide if the result genuinely answers the question (right entities, "
            "metric, filters, grain). Return JSON {\"answers_question\": bool, \"score\": 0-1, \"rationale\"}.",
            json.dumps({"question": question, "sql": sql, "columns": columns, "rows": rows[:15]}, default=str),
        )
        if not raw:
            return {"answers_question": bool(rows), "score": 0.5, "judge_source": "heuristic", "rationale": "judge unavailable"}
        return {"answers_question": bool(raw.get("answers_question")), "score": float(raw.get("score") or 0),
                "judge_source": "llm", "rationale": raw.get("rationale") or ""}
