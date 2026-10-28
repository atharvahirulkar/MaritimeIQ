"""Walkthrough step rail — gated pre-Bronze → Gold."""
from __future__ import annotations

import streamlit as st

from maritimeiq.ui.walkthrough import STEPS, silver_ready_checklist, step_unlocked
from maritimeiq.utils.paths import BRONZE, GOLD, SILVER


def render_pipeline_rail() -> None:
    st.markdown("##### Walkthrough")
    phase = st.session_state.get("pipeline_state", {}).get("phase")
    current = st.session_state.get("walk_step", "sources")
    ready = silver_ready_checklist()

    bronze_n = len([p for p in BRONZE.iterdir() if p.is_dir()]) if BRONZE.exists() else 0
    silver_n = len([p for p in SILVER.iterdir() if p.is_dir()]) if SILVER.exists() else 0
    gold_n = len([p for p in GOLD.iterdir() if p.is_dir()]) if GOLD.exists() else 0

    steps_html = ['<div class="mq-rail-wrap"><div class="mq-track" aria-hidden="true"></div>']
    for step in STEPS:
        sid = step["id"]
        unlocked = step_unlocked(sid, phase)
        if sid == current:
            cls = "active"
            mark = "Current"
        elif unlocked:
            cls = "done"
            mark = "Open"
        else:
            cls = "pending"
            mark = "Locked"
        extra = ""
        if sid == "bronze":
            extra = f" · {bronze_n} Tables"
        elif sid == "silver":
            extra = f" · {silver_n} Tables"
        elif sid == "gold":
            extra = f" · {gold_n} Gold"
        elif sid == "hitl":
            extra = f" · {ready['hitl_open']} Gaps"
        act = step.get("act", "")
        act_lbl = f"Act {act} · " if act in {"0", "1", "2"} else ""
        steps_html.append(
            f'<div class="mq-rail-step {cls}"><b>{step["label"]}</b><br/>'
            f'<span style="color:#8aa0b2">{act_lbl}{mark}{extra}</span></div>'
        )
    steps_html.append("</div>")
    st.markdown("".join(steps_html), unsafe_allow_html=True)

    st.caption("Continue advances one stage. Trust is always available.")

    with st.expander("Advanced: Batch Run", expanded=False):
        st.caption("Walkthrough mode stays on (HITL pauses). Prefer Continue on each step.")
        options = ["bronze", "context", "er", "silver", "gold"]
        labels = ["Bronze", "Context", "Entity Resolution", "Silver", "Gold Mart"]
        pick = st.selectbox("Run Through", labels, index=3, key="rail_through_label")
        through = options[labels.index(pick)]
        if st.button("Run Pipeline Batch", use_container_width=True):
            st.session_state["run_pipeline_request"] = through
            st.session_state["pipeline_walkthrough"] = True
