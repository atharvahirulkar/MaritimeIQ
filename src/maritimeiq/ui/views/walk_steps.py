"""Center-pane content for each walkthrough step."""
from __future__ import annotations

import json
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
            "Accept All Judge-Plausible Gaps (score ≥ 0.7)",
            help="Your bulk decision: approves every open gap the Judge did not grade 'wrong' with score ≥ 0.7. "
            "Wrong-graded gaps still need individual review.",
        ):
            from maritimeiq.agents.context import ContextAgent

            ctx = ContextAgent(orch.gateway)
            approved = 0
            for it in list(open_review_items()):
                if float(it.get("score") or 0) >= 0.7 and it.get("grade") != "wrong":
                    ctx.apply_human_decision(it["table"], it["column"], "approve", approver="human-bulk")
                    approved += 1
            st.session_state["hitl_resolved_count"] = int(
                st.session_state.get("hitl_resolved_count") or 0
            ) + approved
            st.success(f"Approved {approved} gaps.")
            st.rerun()
    review_view.render_review(orch, embedded=True)


def _er_decide(item: dict[str, Any], action: str, target: str | None) -> None:
    from maritimeiq.agents.entity_resolution import record_er_decision
    from maritimeiq.agents.orchestrator import invalidate_from

    record_er_decision(item["id"], action, target, approver="ui-user")
    invalidate_from("er")  # ER re-runs with the decision before Silver
    st.session_state["hitl_resolved_count"] = int(st.session_state.get("hitl_resolved_count") or 0) + 1


def render_connect(orch) -> None:
    from maritimeiq.agents.entity_resolution import load_er_review, open_er_items

    _title("Connect", "Entity resolution links disparate sources — you confirm what the agent is unsure about")
    silver = sorted(p.name for p in SILVER.iterdir() if p.is_dir()) if SILVER.exists() else []
    masters = [t for t in silver if t.startswith("master_") or "alias" in t or "record_map" in t]
    items = open_er_items()
    reviewed = [i for i in load_er_review() if i.get("status") == "resolved"]
    c1, c2, c3 = st.columns(3)
    with c1:
        _kpi("Master / Link Tables", len(masters) or "—")
    with c2:
        _kpi("Open Match Reviews", len(items))
    with c3:
        _kpi("Resolved Reviews", len(reviewed))
    if not masters:
        st.info("Continue from HITL runs Entity Resolution to build vessel / port / customer masters.")
        return

    if items:
        st.warning(
            f"{len(items)} uncertain matches need your guidance. Unconfirmed vessels/customers stay separate and "
            "unresolved ports stay unmapped until you decide — decisions re-run entity resolution."
        )
        bulk = [i for i in items if i.get("candidates") and float(i.get("score") or 0) >= 85]
        if bulk and st.button(f"Accept Top Suggestion for {len(bulk)} High-Score Matches (≥ 85)"):
            for it in bulk:
                top = it["candidates"][0]
                if it["kind"] == "port":
                    _er_decide(it, "map", top["locode"])
                else:
                    _er_decide(it, "link", top.get("master_vessel_id") or top.get("group"))
            st.rerun()
        kinds = sorted({i["kind"] for i in items})
        kind = st.radio("Review type", kinds, horizontal=True, key="er_kind",
                        format_func=lambda k: {"port": "Ports", "vessel": "AIS vessels", "vessel_name": "Vessel names",
                                               "customer": "Customers"}.get(k, k))
        for it in [i for i in items if i["kind"] == kind][:25]:
            with st.container(border=True):
                st.markdown(f"**{it['question']}**  ·  match score {it.get('score')}")
                cands = it.get("candidates") or []
                key = f"er_{it['id']}"
                if it["kind"] == "port":
                    opts = [f"{c['locode']} — {c['name']} ({c['score']})" for c in cands]
                    cols = st.columns([3, 1, 1])
                    choice = cols[0].selectbox("Map to", opts + ["Other LOCODE…"], key=f"{key}_sel", label_visibility="collapsed")
                    other = cols[0].text_input("LOCODE", key=f"{key}_other", placeholder="e.g. CNSZX") if choice == "Other LOCODE…" else ""
                    if cols[1].button("Map", key=f"{key}_map", type="primary"):
                        code = other.strip().upper() if choice == "Other LOCODE…" else choice.split(" — ")[0]
                        if code:
                            _er_decide(it, "map", code)
                            st.rerun()
                    if cols[2].button("Not a port", key=f"{key}_np"):
                        _er_decide(it, "not_port", None)
                        st.rerun()
                else:
                    cand = cands[0] if cands else {}
                    st.caption(f"Left: `{json.dumps(it.get('left'), default=str)}`  →  Candidate: `{json.dumps(cand, default=str)[:220]}`")
                    cols = st.columns([1, 1, 3])
                    if cols[0].button("Same entity — link", key=f"{key}_link", type="primary"):
                        _er_decide(it, "link", cand.get("master_vessel_id") or cand.get("group"))
                        st.rerun()
                    if cols[1].button("Different — keep separate", key=f"{key}_sep"):
                        _er_decide(it, "separate", None)
                        st.rerun()
    else:
        st.success("No open match reviews. Continue builds Silver (re-running entity resolution if you changed anything).")

    st.markdown("#### Masters")
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
        qc_path = ROOT / "lakehouse" / "context" / "silver_qc.json"
        if qc_path.exists():
            qc = json.loads(qc_path.read_text(encoding="utf-8"))
            with st.expander("Silver quality checks", expanded=not ready["ok"]):
                st.dataframe(
                    [
                        {"Table": t, "OK": "✅" if v.get("ok") else "❌", "Rows": v.get("rows"),
                         "Masked PII columns": ", ".join(v.get("masked_columns") or []) or "—",
                         "Checks": "; ".join(f"{c['check']}={c['detail']}" for c in v.get("checks") or [])}
                        for t, v in qc.items()
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
        if ready["ok"]:
            st.success("Silver is ready. Continue runs the Gold investigation agent.")
        else:
            st.warning("Resolve checklist items before Gold Build / Ask unlock fully.")
    with tab_cleanup:
        summary = cleanup_summary()
        st.write(summary["narrative"])
        st.dataframe(summary["rows"], use_container_width=True, hide_index=True)
        st.caption("Messy pre-Bronze / Bronze vs what activation cleaned up for AI use.")


def render_gold(orch) -> None:
    from maritimeiq.agents.orchestrator import load_progress

    _title(
        "Gold Build",
        "The investigation agent mined Silver for the metrics that matter — review, then approve to build Gold",
    )
    ready = silver_ready_checklist()
    if not ready["ok"]:
        st.error("Silver is not ready yet. Go back and complete Act 1.")
        return
    builder = GoldBuilderAgent(orch.spark, orch.gateway)
    plan = builder.load_plan()
    if not plan:
        st.info("Continue from Silver runs the investigation. You can also start it here.")
        if st.button("Run Investigation", type="primary"):
            with st.spinner("Investigating Silver…"):
                builder.investigate()
            st.rerun()
        return
    built = "gold" in load_progress()
    tab_find, tab_kpi, tab_model, tab_cleanup = st.tabs(["Findings", "KPI Tables", "Dimensional Model", "Cleanup Summary"])
    with tab_find:
        st.caption("Each finding is backed by an executed query and graded by the LLM-as-Judge.")
        for f in plan.get("findings") or []:
            with st.container(border=True):
                st.markdown(f"**{f['title']}** · Judge: `{f.get('judge_grade')}` ({float(f.get('judge_score') or 0):.2f}) · {f.get('source')}")
                if f.get("headline"):
                    st.markdown(f.get("headline"))
                st.caption(f.get("why_it_matters") or "")
                if f.get("judge_rationale"):
                    st.caption(f"Judge: {f['judge_rationale']}")
    with tab_kpi:
        kpis = plan.get("kpis") or []
        ready_ids = [k["id"] for k in kpis if k.get("status") == "ready"]
        selected = st.multiselect(
            "KPI tables to materialize", ready_ids, default=ready_ids, key="gold_kpi_select", disabled=built
        )
        for k in kpis:
            with st.expander(f"{k['title']} — {k.get('status')}", expanded=False):
                st.caption(f"`{k['table_name']}` · grain: {k['grain']} · {k['why']}")
                if k.get("error"):
                    st.error(k["error"])
                if k.get("preview"):
                    st.dataframe(k["preview"], use_container_width=True)
                if k.get("sql"):
                    st.code(k["sql"], language="sql")
    with tab_model:
        st.markdown(gold_dim_model_mermaid(plan["dims"] + plan["facts"] + plan["kpis"]))
        st.dataframe(
            [{"Table": t["table_name"], "Kind": t.get("kind", "kpi"), "Grain": t["grain"], "Status": t.get("status")}
             for t in plan["dims"] + plan["facts"] + plan["kpis"]],
            use_container_width=True, hide_index=True,
        )
    with tab_cleanup:
        summary = cleanup_summary()
        st.write(summary["narrative"])
        st.dataframe(summary["rows"], use_container_width=True, hide_index=True)

    st.divider()
    if built:
        catalog = load_gold_catalog()
        st.success(f"Gold built under approval `{catalog.get('approval_id')}`: " + ", ".join(catalog.get("materialized") or []))
        if catalog.get("errors"):
            st.error("Errors: " + "; ".join(catalog["errors"]))
        return
    st.warning("Gold writes need your approval. Nothing is written until you approve this plan.")
    approver = st.text_input("Approver name", value=st.session_state.get("gold_approver", ""), key="gold_approver")
    if st.button("Approve & Build Gold", type="primary", disabled=not approver.strip() or not st.session_state.get("gold_kpi_select")):
        appr = orch.gateway.review.get_approval("gold.relational_mart", agent="human", approver=approver.strip())
        with st.spinner("Materializing Gold…"):
            state = orch.run(
                through="gold",
                state={"walkthrough": True, "gold_approval_id": appr["approval_id"],
                       "gold_selected_kpis": st.session_state.get("gold_kpi_select")},
            )
        st.session_state.setdefault("pipeline_state", {})["phase"] = state.get("phase")
        if state.get("phase") == "gold_done":
            st.session_state["walk_step"] = "ask"
        st.rerun()


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
