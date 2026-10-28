"""LLM-as-Judge: grades semantic proposals, Gold findings and Ask answers.

Never sees ground truth. The heuristic rubric checks proposals against profile evidence
(sample values); the LLM judge runs on a separately configurable model
(MARITIMEIQ_JUDGE_MODEL) so it is not simply the generator grading itself.
"""
from __future__ import annotations

import json
import re
from typing import Any

from maritimeiq.agents.llm import LLM
from maritimeiq.utils.logging_feed import FEED

GRADES = {"correct", "plausible", "wrong"}
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PII_NAME_HINTS = ("passport", "email", "phone", "tax", "dob", "birth", "contact", "seaman", "cdc", "surname", "family_name", "given")
GENERIC_MEANING = re.compile(r"^field `.*` from source profile$", re.I)


def _is_number(v: Any) -> bool:
    try:
        float(str(v))
        return True
    except ValueError:
        return False


def evidence_checks(proposal: dict[str, Any], profile: dict[str, Any] | None) -> list[tuple[str, bool]]:
    """(check, passed) pairs comparing the proposed meaning with sampled values."""
    name = (proposal.get("name") or "").lower()
    meaning = (proposal.get("meaning") or "").lower()
    samples = [s for s in ((profile or {}).get("samples") or []) if s not in (None, "")]
    sample_str = [str(s) for s in samples]
    checks: list[tuple[str, bool]] = []
    looks_pii = any(h in name for h in PII_NAME_HINTS) or any(
        EMAIL.search(s) or s.startswith("tok_") for s in sample_str
    )
    if looks_pii:
        checks.append(("PII evidence → must be tagged PII", bool(proposal.get("pii"))))
    if not sample_str:
        return checks
    if "latitude" in meaning or name in {"lat", "latitude"}:
        checks.append(("latitude samples numeric in [-90, 90]", all(_is_number(s) and -90 <= float(s) <= 90 for s in sample_str)))
    if "longitude" in meaning or name in {"lon", "lng", "longitude"}:
        checks.append(("longitude samples numeric in [-180, 180]", all(_is_number(s) and -180 <= float(s) <= 180 for s in sample_str)))
    if "imo" in meaning.split() or "imo ship" in meaning:
        ok = sum(1 for s in sample_str if re.search(r"\d{7}", s)) / len(sample_str) >= 0.5
        checks.append(("IMO samples contain 7-digit numbers", ok))
    if "mmsi" in meaning:
        checks.append(("MMSI samples are 9-digit", sum(1 for s in sample_str if re.fullmatch(r"\d{9}", s.strip())) / len(sample_str) >= 0.5))
    if any(k in meaning for k in ("timestamp", "date", "time")) and "zone" not in meaning:
        ok = sum(1 for s in sample_str if re.search(r"\d", s)) / len(sample_str) >= 0.8
        checks.append(("time samples contain digits", ok))
    if proposal.get("code_map"):
        known = {str(k).upper() for k in proposal["code_map"]}
        hit = sum(1 for s in sample_str if s.strip().upper() in known) / len(sample_str)
        checks.append(("samples covered by proposed code map", hit >= 0.5))
    return checks


def grade_column_proposal(proposal: dict[str, Any], profile: dict[str, Any] | None = None) -> dict[str, Any]:
    """Heuristic rubric: confidence adjusted by evidence. Returns grade correct | plausible | wrong."""
    score = float(proposal.get("confidence") or 0.5)
    checks = evidence_checks(proposal, profile)
    failed = [c for c, ok in checks if not ok]
    passed = [c for c, ok in checks if ok]
    if GENERIC_MEANING.match(proposal.get("meaning") or ""):
        score = min(score, 0.4)
    score = min(1.0, score + 0.05 * len(passed))
    if failed:
        score = min(score, 0.2)
    grade = "wrong" if failed else ("correct" if score >= 0.85 else "plausible" if score >= 0.5 else "wrong")
    return {
        "grade": grade,
        "score": round(score, 3),
        "judge_source": "heuristic",
        "evidence_passed": passed,
        "evidence_failed": failed,
    }


class JudgeAgent:
    system = (
        "You are an independent data-semantics judge for shipping data. You did not write the proposals. "
        "Grade each proposed column meaning against its sample values. Be strict: 'correct' only when the "
        "samples clearly support the meaning and PII tagging; 'wrong' when contradicted or PII is missed."
    )

    def grade_semantics(
        self, proposals: list[dict[str, Any]], profiles: dict[str, dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        profiles = profiles or {}
        llm_grades: dict[str, dict[str, Any]] = {}
        if LLM.available and proposals:
            for start in range(0, len(proposals), 25):
                chunk = proposals[start : start + 25]
                payload = {
                    "items": [
                        {
                            "name": p.get("name"),
                            "meaning": p.get("meaning"),
                            "pii": p.get("pii"),
                            "code_map": p.get("code_map") or {},
                            "samples": [str(s)[:60] for s in (profiles.get(p.get("name"), {}).get("samples") or [])[:8]],
                        }
                        for p in chunk
                    ],
                    "output": '{"grades": [{"name", "grade": "correct|plausible|wrong", "score": 0-1, "rationale"}]}',
                }
                raw = LLM.complete_json(self.system, json.dumps(payload, default=str), judge=True)
                for g in (raw or {}).get("grades") or []:
                    if isinstance(g, dict) and g.get("name") and g.get("grade") in GRADES:
                        llm_grades[str(g["name"])] = g

        out: list[dict[str, Any]] = []
        for p in proposals:
            base = grade_column_proposal(p, profiles.get(p.get("name", "")))
            lg = llm_grades.get(str(p.get("name")))
            if lg:
                score = float(lg.get("score") or base["score"])
                grade = lg["grade"]
                if base["evidence_failed"]:  # deterministic contradictions always win
                    grade, score = "wrong", min(score, base["score"])
                base = {**base, "grade": grade, "score": round(score, 3), "judge_source": "llm",
                        "judge_rationale": lg.get("rationale")}
            out.append({**p, **base})
        FEED.emit(
            "judge",
            f"graded {len(out)} proposals (llm={len(llm_grades)}, heuristic={len(out) - len(llm_grades)})",
            event_type="judge_complete",
            llm_graded=len(llm_grades),
        )
        return out

    def grade_findings(self, findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Judge whether each Gold finding is supported by its evidence and decision-relevant."""
        llm: dict[str, dict[str, Any]] = {}
        if LLM.available and findings:
            payload = {
                "items": [
                    {"id": f["id"], "title": f["title"], "headline": f.get("headline"),
                     "why_it_matters": f.get("why_it_matters"), "sql": (f.get("sql") or "")[:1500],
                     "preview": (f.get("preview") or [])[:5]}
                    for f in findings
                ],
                "output": '{"grades": [{"id", "grade": "correct|plausible|wrong", "score": 0-1, "rationale"}]}',
                "criteria": "correct = SQL computes what the headline claims and it matters to a carrier/port operator",
            }
            raw = LLM.complete_json(
                "You are an independent reviewer of analytics findings for a shipping enterprise.",
                json.dumps(payload, default=str),
                judge=True,
            )
            for g in (raw or {}).get("grades") or []:
                if isinstance(g, dict) and g.get("id") and g.get("grade") in GRADES:
                    llm[str(g["id"])] = g
        out = []
        for f in findings:
            g = llm.get(f["id"])
            if g:
                f = {**f, "judge_grade": g["grade"], "judge_score": float(g.get("score") or 0.5),
                     "judge_rationale": g.get("rationale"), "judge_source": "llm"}
            else:
                has_evidence = bool(f.get("headline") or f.get("preview"))
                base = 0.8 if f.get("source") == "kpi_library" else 0.6
                f = {**f, "judge_grade": "plausible" if has_evidence else "wrong",
                     "judge_score": base if has_evidence else 0.1, "judge_source": "heuristic",
                     "judge_rationale": "Evidence query executed and returned rows" if has_evidence else "No evidence rows"}
            out.append(f)
        return out

    def grade_answer(self, question: str, sql: str, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Does this SQL result actually answer the question? None when no LLM is configured."""
        if not LLM.available:
            return None
        raw = LLM.complete_json(
            "You are an independent judge of Text-to-SQL answers. Decide whether the SQL and its result "
            "answer the user's question as asked (right metric, filters, grain). "
            'Return JSON {"verdict": "answers|partial|does_not_answer", "score": 0-1, "rationale": "..."}',
            json.dumps({"question": question, "sql": sql, "result_preview": rows[:10]}, default=str),
            judge=True,
        )
        if not raw or raw.get("verdict") not in {"answers", "partial", "does_not_answer"}:
            return None
        return {"verdict": raw["verdict"], "score": float(raw.get("score") or 0.5), "rationale": raw.get("rationale")}
