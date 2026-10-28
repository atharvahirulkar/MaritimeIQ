"""Center-pane content for each walkthrough step."""
from __future__ import annotations

from typing import Any

import streamlit as st

from maritimeiq.agents.gold_builder import GoldBuilderAgent
from maritimeiq.context_store.store import ContextStore
from maritimeiq.ui.atmosphere import render_hero_lanes
from maritimeiq.ui.components.hitl_queue import correction_learning_summary, open_review_items
from maritimeiq.ui.views import ask as ask_view
from maritimeiq.ui.views import review as review_view
from maritimeiq.ui.views import trust as trust_view
from maritimeiq.ui.walkthrough import (
    cleanup_summary,
    gold_dim_model_mermaid,
    load_gold_catalog,
    silver_ready_checklist,
    source_inventory,
)
from maritimeiq.utils.paths import BRONZE, GOLD, ROOT, SILVER, SOURCES


def _title(title: str, sub: str) -> None:
    st.markdown(f'<p class="mq-title">{title}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="mq-sub">{sub}</p>', unsafe_allow_html=True)


def _kpi(label: str, value: Any) -> None:
    st.markdown(
        f'<div class="mq-kpi mq-rise"><div class="label">{label}</div>'
        f'<div class="value">{value}</div></div>',
        unsafe_allow_html=True,
    )


def render_sources(orch) -> None:
    _title("Sources", "Pre-Bronze source pack — original files before activation")
    render_hero_lanes()
    rows = source_inventory()
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.warning(f"No sources under `{SOURCES}`.")
    st.info(
        "These feeds are messy and not AI-usable yet. Continue runs Bronze ingest "
        "to land them in the lakehouse."
    )
    if SOURCES.exists():
        st.caption(f"Source root: `{SOURCES.relative_to(ROOT)}`")


def render_bronze(orch) -> None:
    _title("Bronze", "Raw ingested tables — still messy, not AI-ready")
    tables = sorted(p.name for p in BRONZE.iterdir() if p.is_dir()) if BRONZE.exists() else []
    c1, c2 = st.columns(2)
    with c1:
        _kpi("Bronze Tables", len(tables) or "—")
    with c2:
        _kpi("Status", "Messy / Not Usable" if tables else "Empty")
    if not tables:
        st.warning("No Bronze tables yet. Use Continue to ingest from Sources.")
        return
    pick = st.selectbox("Inspect Bronze Table", tables)
    try:
        sample = orch.gateway.lakehouse.read_sample(f"bronze.{pick}", n=15, agent="ui")
        if sample.get("rows"):
            st.dataframe(sample["rows"], use_container_width=True)
            st.caption("Raw Bronze sample — column meanings and PII not governed yet.")
        elif sample.get("error"):
            st.error(sample["error"])
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))


def render_semantics(orch) -> None:
    _title("Semantics", "LLM proposes field meanings, types, PII, and join hints — Judge scores them")
    store = ContextStore()
    proposals = store.load_proposals()
    sem = store.load_semantics().get("tables", {})
    c1, c2, c3 = st.columns(3)
    with c1:
        _kpi("Proposed Tables", len(proposals) or "—")
    with c2:
        _kpi("Approved Tables", len(sem) or "—")
    with c3:
        _kpi("Open HITL Gaps", len(open_review_items()))
    if not proposals and not sem:
        st.info("Continue runs Context + LLM-as-Judge on Bronze tables.")
        return
    keys = sorted(set(list(proposals.keys()) + list(sem.keys())))
    t = st.selectbox("Table", keys)
    cols = (proposals.get(t) or {}).get("columns") or (sem.get(t) or {}).get("columns") or []
    view = [
        {
            "Field": c.get("name"),
            "Meaning": c.get("meaning"),
            "PII": "Yes" if c.get("pii") else "No",
            "Grade": str(c.get("grade") or "—").title(),
            "Score": c.get("score"),
            "Status": str(c.get("status") or "—").replace("_", " ").title(),
        }
        for c in cols
    ]
    st.dataframe(view, use_container_width=True, hide_index=True)
    joins = (proposals.get(t) or {}).get("joins") or []
    if joins:
        st.markdown("#### Proposed Joins")
        st.json(joins)
    st.info(correction_learning_summary(st.session_state.get("pipeline_state")))


def render_hitl(orch) -> None:
    _title(
        "HITL Gaps",
        "Where Judge/confidence left gaps — guide the agent so activation can continue",
    )
    items = open_review_items()
    if items:
        st.warning(f"{len(items)} gaps need your guidance before Silver activation continues.")
        if st.button(
            "Accept Remaining High-Confidence Gaps",
            help="Approve needs_review items with score ≥ 0.7",
        ):
            from maritimeiq.agents.context import ContextAgent

            ctx = ContextAgent(orch.gateway)
            approved = 0
            for it in list(open_review_items()):
                if float(it.get("score") or 0) >= 0.7 and it.get("grade") != "wrong":
                    ctx.apply_human_decision(it["table"], it["column"], "approve")
                    approved += 1
            st.session_state["hitl_resolved_count"] = int(
                st.session_state.get("hitl_resolved_count") or 0
            ) + approved
            st.success(f"Approved {approved} gaps.")
            st.rerun()
    review_view.render_review(orch, embedded=True)


def render_connect(orch) -> None:
    _title("Connect", "Entity resolution — masters that link disparate sources")
    silver = sorted(p.name for p in SILVER.iterdir() if p.is_dir()) if SILVER.exists() else []
    masters = [t for t in silver if t.startswith("master_") or "alias" in t or "record_map" in t]
    c1, c2 = st.columns(2)
    with c1:
        _kpi("Master / Link Tables", len(masters) or "—")
    with c2:
        _kpi("Phase", st.session_state.get("pipeline_state", {}).get("phase", "—"))
    if not masters:
        st.info("Continue runs Entity Resolution to build vessel / port / customer masters.")
        return
    st.write(", ".join(masters))
    pick = st.selectbox("Preview Master", masters)
    sample = orch.gateway.lakehouse.read_sample(f"silver.{pick}", n=20, agent="ui")
    if sample.get("rows"):
        st.dataframe(sample["rows"], use_container_width=True)


def render_silver(orch) -> None:
    _title("Silver", "Clean, typed, PII-masked tables — Act 1 complete when checklist passes")
    ready = silver_ready_checklist()
    tab_preview, tab_cleanup = st.tabs(["Silver Preview", "Cleanup Summary"])
    with tab_preview:
        for c in ready["checks"]:
            st.markdown(("✅ " if c["ok"] else "⬜ ") + c["label"])
        c1, c2, c3 = st.columns(3)
        with c1:
            _kpi("Silver Tables", ready["silver_n"] or "—")
        with c2:
            _kpi("Semantics Tables", ready["sem_tables"] or "—")
        with c3:
            _kpi("Silver Ready", "Yes" if ready["ok"] else "Not Yet")
        tables = sorted(p.name for p in SILVER.iterdir() if p.is_dir()) if SILVER.exists() else []
        if not tables:
            st.info("Continue runs Silver transforms from Bronze + ER masters.")
        else:
            pick = st.selectbox("Preview Silver Table", tables)
            n = st.slider("Rows", 5, 40, 15, key="silver_walk_rows")
            sample = orch.gateway.lakehouse.read_sample(f"silver.{pick}", n=n, agent="ui")
            if sample.get("rows"):
                st.dataframe(sample["rows"], use_container_width=True)
                st.caption("Samples pass the governance guard (PII masked when tagged).")
        if ready["ok"]:
            st.success("Silver is ready. Continue to Act 2 — Gold relational mart.")
        else:
            st.warning("Resolve checklist items before Gold Build / Ask unlock fully.")
    with tab_cleanup:
        summary = cleanup_summary()
        st.write(summary["narrative"])
        st.dataframe(summary["rows"], use_container_width=True, hide_index=True)
        st.caption("Messy pre-Bronze / Bronze vs what activation cleaned up for AI use.")


def render_gold(orch) -> None:
    _title(
        "Gold Build",
        "AI investigates Silver + semantics and materializes a relational Gold mart for LLM Q&A",
    )
    ready = silver_ready_checklist()
    if not ready["ok"]:
        st.error("Silver is not ready yet. Go back and complete Act 1.")
        return
    catalog = load_gold_catalog()
    builder = GoldBuilderAgent(orch.spark, orch.gateway)
    proposal = builder.propose_mart()
    tab_mart, tab_cleanup, tab_dim = st.tabs(["Mart Build", "Cleanup Summary", "Dimensional Model"])
    with tab_mart:
        st.markdown("#### Proposed Relational Mart")
        st.dataframe(
            [
                {
                    "Table": p["table_name"],
                    "Kind": p["kind"],
                    "Grain": p["grain"],
                    "Join Keys": ", ".join(p.get("join_keys") or []),
                    "Answers": ", ".join(p.get("answers") or []),
                }
                for p in proposal.get("proposals") or []
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Primary analyst surface: `{proposal.get('primary_view')}`")
        if catalog.get("materialized"):
            st.success("Materialized: " + ", ".join(catalog["materialized"]))
        elif st.session_state.get("pipeline_state", {}).get("gold_mart", {}).get("materialized"):
            st.success(
                "Materialized: "
                + ", ".join(st.session_state["pipeline_state"]["gold_mart"]["materialized"])
            )
        gold_tables = sorted(p.name for p in GOLD.iterdir() if p.is_dir()) if GOLD.exists() else []
        if gold_tables:
            st.markdown("#### Gold Tables On Disk")
            st.write(", ".join(gold_tables))
        st.info("Continue materializes the Gold mart (dims, facts, v_ops_overview).")
    with tab_cleanup:
        summary = cleanup_summary()
        st.write(summary["narrative"])
        st.dataframe(summary["rows"], use_container_width=True, hide_index=True)
    with tab_dim:
        st.markdown("#### Star / Snowflake Sketch")
        st.markdown(gold_dim_model_mermaid(proposal.get("proposals")))
        st.markdown("#### Join Keys")
        st.dataframe(
            [
                {
                    "Table": p["table_name"],
                    "Kind": p["kind"],
                    "Grain": p["grain"],
                    "Join Keys": ", ".join(p.get("join_keys") or []),
                    "Source": p.get("source"),
                }
                for p in proposal.get("proposals") or []
            ],
            use_container_width=True,
            hide_index=True,
        )


def render_ask(orch) -> None:
    _title(
        "Ask",
        "Text-to-SQL cascade: Gold mart first → Silver + semantics → refuse if out of scope",
    )
    ready = silver_ready_checklist()
    if not ready["ok"]:
        st.error("Complete Act 1 (Silver ready) before Ask.")
        return
    ask_view.render_ask(orch)


def render_trust(orch) -> None:
    trust_view.render_trust()


RENDERERS = {
    "sources": render_sources,
    "bronze": render_bronze,
    "semantics": render_semantics,
    "hitl": render_hitl,
    "connect": render_connect,
    "silver": render_silver,
    "gold": render_gold,
    "ask": render_ask,
    "trust": render_trust,
}
