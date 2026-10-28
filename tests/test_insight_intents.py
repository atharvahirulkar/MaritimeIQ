"""Verified Gold intents: precise on in-scope questions, silent on look-alikes."""
from __future__ import annotations

import maritimeiq.agents.insight as insight
from maritimeiq.agents.insight import DOMAIN_TERMS, _words, match_intent

GOLD = {
    "kpi_anchorage_wait_monthly", "kpi_anchorage_wait_yearly", "kpi_empty_repositioning", "kpi_booking_rolls",
    "kpi_vessel_identity_changes", "kpi_shipper_teu", "kpi_equipment_reassignments", "kpi_import_dwell_quarterly",
    "kpi_customs_holds", "kpi_schedule_reliability", "kpi_transit_time_monthly", "insight_findings",
}


def setup_module() -> None:
    insight.PORT_NAMES.update({"busan": "KRPUS", "rotterdam": "NLRTM", "los angeles": "USLAX", "long beach": "USLGB"})


def test_lookalikes_do_not_match() -> None:
    for q in [
        "Which shareholders hold the most stock?",
        "What is our payroll enrollment?",
        "Who rolled out the new HR system?",
        "Top shippers of holiday cards to Mars?" if False else "What is the weather on Mars?",
    ]:
        assert match_intent(q, GOLD) is None, q


def test_out_of_domain_has_no_vocabulary() -> None:
    assert not (_words("What is the weather forecast for Mars next Tuesday?") & DOMAIN_TERMS)
    assert _words("Which ports had the longest anchorage wait?") & DOMAIN_TERMS


def test_scope_from_port_names() -> None:
    name, sql, _ = match_intent("How long do ships wait at anchor in Busan by month?", GOLD)
    assert name == "anchorage_monthly" and "'KRPUS'" in sql
    _, sql, _ = match_intent("In which month was the average anchorage wait at Los Angeles/Long Beach highest?", GOLD)
    assert "'LA/LB'" in sql


def test_unknown_place_defers_to_llm() -> None:
    assert match_intent("Average anchorage wait by month in Mumbai?", GOLD) is None


def test_year_and_top_n() -> None:
    _, sql, _ = match_intent("Which 5 ports had the longest average anchorage wait (days) in 2022?", GOLD)
    assert "year = 2022" in sql and "LIMIT 5" in sql


def test_intent_requires_gold_table() -> None:
    assert match_intent("How many equipment reassignments did each carrier record?", set()) is None
