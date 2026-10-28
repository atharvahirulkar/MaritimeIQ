"""MaritimeIQ walkthrough — gated pre-Bronze → Gold activation console."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from maritimeiq.utils.env import load_env

load_env()

from maritimeiq.agents.entity_resolution import open_er_items
from maritimeiq.agents.orchestrator import Orchestrator, load_progress
from maritimeiq.ui.components.agent_feed import render_agent_feed
from maritimeiq.ui.components.hitl_queue import open_review_items
from maritimeiq.ui.components.pipeline_rail import render_pipeline_rail
from maritimeiq.ui.components.status_bar import render_status_bar
from maritimeiq.ui.theme import apply_theme
from maritimeiq.ui.views import walk_steps
from maritimeiq.ui.walkthrough import (
    STEP_IDS,
    STEPS,
    activation_progress,
    continue_action,
    hard_reset_walkthrough,
    infer_resume_step,
    load_walk_session,
    save_walk_session,
    silver_ready_checklist,
    step_unlocked,
)
from maritimeiq.utils.logging_feed import FEED

st.set_page_config(
    page_title="MaritimeIQ Walkthrough",
    layout="wide",
    page_icon="⚓",
    initial_sidebar_state="collapsed",
)
apply_theme()


@st.cache_resource
def get_orch() -> Orchestrator:
    return Orchestrator()


def _persist() -> None:
    save_walk_session(
        st.session_state.get("walk_step", "sources"),
        st.session_state.get("pipeline_state"),
        hitl_resolved_count=int(st.session_state.get("hitl_resolved_count") or 0),
    )


def _store_pipeline_state(state: dict) -> None:
    slim = {
        k: state.get(k)
        for k in (
            "phase",
            "bronze_tables",
            "silver_tables",
            "context_learning",
            "live_metrics",
            "hitl_checkpoints",
            "gold_mart",
        )
    }
    if state.get("insight_result"):
        ir = dict(state["insight_result"])
        ir["rows"] = (ir.get("rows") or [])[:20]
        slim["insight_result"] = ir
    for key in ("bronze_tables", "silver_tables"):
        val = slim.get(key)
        if isinstance(val, dict):
            slim[key] = {
                k: (v.get("rows") if isinstance(v, dict) else v) for k, v in val.items()
            }
    st.session_state["pipeline_state"] = slim
    _persist()


def _apply_run_result(state: dict, label: str) -> None:
    _store_pipeline_state(state)
    phase = st.session_state["pipeline_state"].get("phase")
    FEED.emit("orchestrator", f"UI ran {label} → phase={phase}")
    if phase == "awaiting_hitl":
        st.session_state["walk_step"] = "hitl"
        st.toast("Paused for HITL — resolve semantic gaps, then Continue.")
    elif phase == "awaiting_er_review":
        st.session_state["walk_step"] = "connect"
        st.toast("Paused — review uncertain entity matches, then Continue.")
    elif phase == "awaiting_gold_approval":
        st.session_state["walk_step"] = "gold"
        st.toast("Gold plan ready — review findings and approve to build Gold.")
    else:
        st.toast(f"Phase: {str(phase).replace('_', ' ').title()}")
    _persist()


def _handle_pipeline_run() -> None:
    through = st.session_state.pop("run_pipeline_request", None)
    if not through:
        return
    with st.spinner(f"Running through {through} (HITL gates on)…"):
        state = get_orch().run(through=through, state={"walkthrough": True})
    _apply_run_result(state, f"batch through {through}")


def _run_stage(through: str, extra: dict | None = None) -> dict:
    """Resume the pipeline up to ``through``; completed stages are not re-run."""
    with st.spinner(f"Running {through}…"):
        state = get_orch().run(through=through, state={"walkthrough": True, **(extra or {})})
    _apply_run_result(state, through)
    return state


def _bootstrap_session() -> None:
    """Restore walk position from disk so refresh does not jump to step 0."""
    if st.session_state.get("_walk_bootstrapped"):
        return
    st.session_state["_walk_bootstrapped"] = True

    saved = load_walk_session()
    if saved and saved.get("walk_step") in STEP_IDS:
        st.session_state["walk_step"] = saved["walk_step"]
        st.session_state["pipeline_state"] = saved.get("pipeline_state") or {"phase": "idle"}
        st.session_state["hitl_resolved_count"] = int(saved.get("hitl_resolved_count") or 0)
        return

    # No session file — infer furthest completed stage from lakehouse
    phase = (st.session_state.get("pipeline_state") or {}).get("phase")
    step, inferred_phase = infer_resume_step(phase)
    st.session_state["walk_step"] = step
    st.session_state.setdefault("pipeline_state", {})["phase"] = inferred_phase
    st.session_state.setdefault("hitl_resolved_count", 0)
    _persist()


render_status_bar()
_handle_pipeline_run()

if "show_agent_feed" not in st.session_state:
    st.session_state["show_agent_feed"] = False
if "pipeline_state" not in st.session_state:
    st.session_state["pipeline_state"] = {"phase": "idle"}
if "hitl_resolved_count" not in st.session_state:
    st.session_state["hitl_resolved_count"] = 0

_bootstrap_session()

phase = st.session_state.get("pipeline_state", {}).get("phase")
# If a HITL gate was cleared after a refresh, drop the stale pause
if (phase == "awaiting_hitl" and not open_review_items()) or (phase == "awaiting_er_review" and not open_er_items()):
    st.session_state["pipeline_state"]["phase"] = "resuming"
    phase = "resuming"
    _persist()

prog = activation_progress(
    st.session_state.get("walk_step", "sources"),
    int(st.session_state.get("hitl_resolved_count") or 0),
)
st.progress(prog["pct"] / 100.0)
st.markdown(f'<p class="mq-progress-caption">{prog["caption"]}</p>', unsafe_allow_html=True)

show_feed = st.session_state["show_agent_feed"]
if show_feed:
    left, center, right = st.columns([1.1, 3.2, 1.3], gap="medium")
else:
    left, center = st.columns([1.1, 4.2], gap="medium")
    right = None

with left:
    render_pipeline_rail()
    labels = []
    ids = []
    label_by_id = {s["id"]: s["label"] for s in STEPS}
    for sid in STEP_IDS:
        if step_unlocked(sid, phase) or sid == st.session_state["walk_step"]:
            labels.append(label_by_id[sid])
            ids.append(sid)
    cur = st.session_state["walk_step"]
    if cur not in ids:
        # Keep current step even if briefly locked; don't snap to sources
        ids = [cur] + [i for i in ids if i != cur]
        labels = [label_by_id.get(cur, cur.title())] + labels
    idx = ids.index(cur) if cur in ids else 0
    pick = st.radio("Step", labels, index=idx, label_visibility="collapsed", key="walk_radio")
    st.session_state["walk_step"] = ids[labels.index(pick)]
    st.checkbox("Show Agent Feed", key="show_agent_feed")

    st.divider()
    if st.button(
        "Hard Reset",
        use_container_width=True,
        help="Keep data/ + Bronze; clear Silver/Gold/context and restart after Bronze",
    ):
        st.session_state["confirm_hard_reset"] = True
    if st.session_state.get("confirm_hard_reset"):
        st.warning(
            "Clears Silver, Gold, and context (semantics / HITL / walk session). "
            "**Keeps** `data/` and Bronze. Never deletes the source pack."
        )
        c_yes, c_no = st.columns(2)
        with c_yes:
            if st.button("Confirm reset", type="primary", use_container_width=True):
                get_orch.clear()  # drop cached Spark orchestrator bound to old paths
                result = hard_reset_walkthrough()
                for k in list(st.session_state.keys()):
                    if k.startswith("hitl_") or k in {
                        "walk_step",
                        "pipeline_state",
                        "ask_result",
                        "ask_question",
                        "confirm_hard_reset",
                        "_walk_bootstrapped",
                        "walk_radio",
                    }:
                        del st.session_state[k]
                st.session_state["walk_step"] = result["walk_step"]
                st.session_state["pipeline_state"] = {"phase": result["phase"]}
                st.session_state["hitl_resolved_count"] = 0
                st.session_state["_walk_bootstrapped"] = True
                _persist()
                st.toast(
                    f"Hard reset complete — Bronze kept ({result['bronze_tables']} tables); "
                    f"resuming at {result['walk_step'].title()}."
                )
                st.rerun()
        with c_no:
            if st.button("Cancel", use_container_width=True):
                st.session_state["confirm_hard_reset"] = False
                st.rerun()

with center:
    step = st.session_state["walk_step"]
    orch = get_orch()
    renderer = walk_steps.RENDERERS.get(step, walk_steps.render_sources)
    renderer(orch)

    st.divider()
    b1, b2, b3 = st.columns([1, 1, 2])
    with b1:
        if st.button("Back", use_container_width=True) and step in STEP_IDS:
            i = STEP_IDS.index(step)
            if i > 0:
                st.session_state["walk_step"] = STEP_IDS[i - 1]
                _persist()
                st.rerun()
    with b2:
        cont = st.button("Continue", type="primary", use_container_width=True)
    with b3:
        action = continue_action(step)
        st.caption(f"Next: **{action.get('next', '—').title()}**")

    if cont:
        action = continue_action(step)
        error = None
        if action.get("require_hitl_clear") and open_review_items():
            error = f"Resolve open HITL gaps before continuing ({len(open_review_items())} still need review)."
        elif action.get("require_er_clear") and open_er_items():
            error = f"Review uncertain entity matches before Silver ({len(open_er_items())} open)."
        elif action.get("require_gold") and "gold" not in load_progress():
            error = "Approve and build the Gold mart before Ask."
        if error:
            st.error(error)
        elif action.get("run"):
            run_to = action["run"]
            _run_stage(run_to)
            phase_now = st.session_state.get("pipeline_state", {}).get("phase", "")
            if not str(phase_now).startswith("awaiting_"):
                if run_to == "silver" and not silver_ready_checklist()["ok"]:
                    st.session_state["walk_step"] = "silver"
                    st.warning("Silver ran but the readiness checklist is incomplete.")
                else:
                    st.session_state["walk_step"] = action["next"]
            _persist()
            st.rerun()
        else:
            st.session_state["walk_step"] = action.get("next", step)
            _persist()
            st.rerun()

# Persist whenever the radio changes step
_persist()

if right is not None:
    with right:
        render_agent_feed(50)

if show_feed != st.session_state["show_agent_feed"]:
    st.rerun()
