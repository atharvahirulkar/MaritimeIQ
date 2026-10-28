"""Judge evidence rubric and honest eval scoring."""
from __future__ import annotations

from maritimeiq.agents.judge import grade_column_proposal
from maritimeiq.eval.harness import leaf_matched, meaning_matches, score_answer


def test_judge_flags_missed_pii() -> None:
    g = grade_column_proposal(
        {"name": "Contact_Email", "meaning": "Customer segment", "pii": False, "confidence": 0.9},
        {"samples": ["tok_ab12", "tok_cd34"]},
    )
    assert g["grade"] == "wrong"


def test_judge_flags_contradicted_meaning() -> None:
    g = grade_column_proposal({"name": "LAT", "meaning": "Latitude decimal degrees", "confidence": 0.9},
                              {"samples": ["abc", "def"]})
    assert g["grade"] == "wrong"


def test_judge_generic_meaning_needs_review() -> None:
    g = grade_column_proposal({"name": "x", "meaning": "Field `x` from source profile", "confidence": 0.55}, {"samples": ["1"]})
    assert g["score"] < 0.85


def test_answer_scoring_requires_keys_and_values() -> None:
    gold = {"USLAX": 7.93, "USLGB": 7.7}
    good = [{"port": "USLAX", "avg_wait_days": 7.9}, {"port": "USLGB", "avg_wait_days": 7.7}]
    swapped = [{"port": "USLAX", "avg_wait_days": 5.0}, {"port": "USLGB", "avg_wait_days": 9.1}, {"vessel": "X", "w": 7.93}]
    assert score_answer(gold, good) == 1.0
    assert score_answer(gold, swapped) == 0.0
    assert not leaf_matched(("USLAX",), 7.93, [{"vessel": "X", "wait": 7.93}])


def test_meaning_match_ignores_stopwords() -> None:
    assert not meaning_matches("a field of the source", "Latitude of the position")
    assert meaning_matches("UTC timestamp of the AIS position", "UTC timestamp of the position (no zone marker)")
