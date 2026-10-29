"""HITL semantic review — row table with Approve / Reject / Edit actions."""
from __future__ import annotations

import streamlit as st

from maritimeiq.agents.context import ContextAgent
from maritimeiq.context_store.store import ContextStore
from maritimeiq.ui.components.hitl_queue import correction_learning_summary, open_review_items
from maritimeiq.utils.logging_feed import FEED

GRADE_LABELS = {
    "correct": "Correct",
    "plausible": "Plausible",
    "wrong": "Wrong",
}

ACTION_APPROVE = "Approve"
ACTION_REJECT = "Reject"
ACTION_EDIT = "Edit"


def _bump_resolved(n: int = 1) -> None:
    st.session_state["hitl_resolved_count"] = int(st.session_state.get("hitl_resolved_count") or 0) + n


def _row_key(table: str, column: str) -> str:
    safe_col = column.replace(" ", "_")
    return f"{table}::{safe_col}"


def render_review(orch, *, embedded: bool = False) -> None:
    if not embedded:
        st.markdown('<p class="mq-title">HITL Review Queue</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="mq-sub">Human-in-the-loop on field meanings and PII tags — '
            "corrections teach the next source in the activation loop</p>",
            unsafe_allow_html=True,
        )

    items = open_review_items()
    resolved = int(st.session_state.get("hitl_resolved_count") or 0)
    open_n = len(items)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="mq-kpi mq-rise"><div class="label">Open Gaps</div>'
            f'<div class="value">{open_n}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="mq-kpi mq-rise"><div class="label">Resolved This Session</div>'
            f'<div class="value">{resolved}</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        total = open_n + resolved
        pct = int(round(100 * resolved / total)) if total else 0
        st.markdown(
            f'<div class="mq-kpi mq-rise"><div class="label">HITL Progress</div>'
            f'<div class="value">{pct}%</div></div>',
            unsafe_allow_html=True,
        )

    st.info(correction_learning_summary(st.session_state.get("pipeline_state")))

    store = ContextStore()
    if not items:
        st.success("No open review items. Continue to the next walkthrough step.")
        sem = store.load_semantics()
        tables = sem.get("tables", {})
        st.caption(f"Approved Tables: {len(tables)}")
        if tables:
            t = st.selectbox("Inspect Approved Table", sorted(tables.keys()), key="hitl_approved_table")
            st.dataframe(tables[t].get("columns", []), use_container_width=True)
        return

    # Optional filter to keep long queues manageable
    table_opts = ["All tables"] + sorted({it["table"] for it in items})
    filter_table = st.selectbox("Filter by table", table_opts, key="hitl_table_filter")
    visible = items if filter_table == "All tables" else [it for it in items if it["table"] == filter_table]

    st.caption(
        "Pick Approve / Reject / Edit on each row. Choosing Edit unlocks the New meaning column. "
        "Then click Apply decisions."
    )

    # Header
    h = st.columns([1.3, 1.2, 2.4, 0.7, 0.55, 0.45, 1.6, 2.0])
    headers = ["Table", "Field", "Proposed meaning", "Grade", "Score", "PII", "Action", "New meaning (if Edit)"]
    for col, label in zip(h, headers):
        col.markdown(f"**{label}**")

    st.divider()

    # Limit render for Streamlit performance
    max_rows = 40
    page_items = visible[:max_rows]
    if len(visible) > max_rows:
        st.warning(f"Showing first {max_rows} of {len(visible)} gaps — filter by table to narrow.")

    for it in page_items:
        rk = _row_key(it["table"], it["column"])
        action_key = f"hitl_act_{rk}"
        edit_key = f"hitl_new_{rk}"
        grade = GRADE_LABELS.get(it["grade"], str(it.get("grade") or "—").title())
        meaning = (it.get("meaning") or "")[:180]
        if len(it.get("meaning") or "") > 180:
            meaning += "…"

        row = st.columns([1.3, 1.2, 2.4, 0.7, 0.55, 0.45, 1.6, 2.0])
        row[0].markdown(f"`{it['table']}`")
        row[1].markdown(f"**{it['column']}**")
        row[2].caption(meaning or "—")
        row[3].markdown(grade)
        row[4].markdown(f"{float(it.get('score') or 0):.2f}")
        row[5].markdown("Yes" if it.get("pii") else "—")

        # Default Approve for faster checks (wrong grades default to Edit)
        default_idx = 2 if it.get("grade") == "wrong" else 0
        if action_key not in st.session_state:
            st.session_state[action_key] = [ACTION_APPROVE, ACTION_REJECT, ACTION_EDIT][default_idx]

        with row[6]:
            action = st.radio(
                "action",
                [ACTION_APPROVE, ACTION_REJECT, ACTION_EDIT],
                horizontal=True,
                key=action_key,
                label_visibility="collapsed",
            )

        with row[7]:
            if action == ACTION_EDIT:
                st.text_input(
                    "new meaning",
                    value=it.get("meaning") or "",
                    key=edit_key,
                    label_visibility="collapsed",
                    placeholder="Enter corrected meaning…",
                )
            else:
                st.caption("—")

        if it.get("rationale"):
            st.caption(f"_{it['table']}.{it['column']}: {it['rationale'][:160]}")

    st.divider()
    apply = st.button("Apply Decisions", type="primary", use_container_width=False)
    if apply:
        ctx = ContextAgent(orch.gateway)
        applied = 0
        skipped_edit = 0
        for it in page_items:
            rk = _row_key(it["table"], it["column"])
            action = st.session_state.get(f"hitl_act_{rk}", ACTION_APPROVE)
            if action == ACTION_APPROVE:
                decision = "approve"
                edited = None
            elif action == ACTION_REJECT:
                decision = "reject"
                edited = None
            else:
                decision = "edit"
                edited = (st.session_state.get(f"hitl_new_{rk}") or "").strip()
                if not edited:
                    skipped_edit += 1
                    continue
            res = ctx.apply_human_decision(it["table"], it["column"], decision, edited)
            if res.get("ok"):
                applied += 1
                FEED.emit(
                    "review",
                    f"HITL {action} {it['table']}.{it['column']}",
                    event_type="hitl_decision",
                    table=it["table"],
                    column=it["column"],
                    decision=decision,
                )
            # Clear row widget state so removed gaps don't linger
            for k in (f"hitl_act_{rk}", f"hitl_new_{rk}"):
                if k in st.session_state:
                    del st.session_state[k]

        _bump_resolved(applied)
        if applied:
            st.success(f"Applied {applied} decision(s). Gaps removed from the queue.")
            # If queue empty, allow Continue past HITL
            if not open_review_items():
                st.session_state.setdefault("pipeline_state", {})["phase"] = "context_done"
                try:
                    from maritimeiq.ui.walkthrough import save_walk_session

                    save_walk_session(
                        st.session_state.get("walk_step", "hitl"),
                        st.session_state.get("pipeline_state"),
                        hitl_resolved_count=int(st.session_state.get("hitl_resolved_count") or 0),
                    )
                except Exception:  # noqa: BLE001
                    pass
        if skipped_edit:
            st.warning(f"Skipped {skipped_edit} Edit row(s) with empty new meaning.")
        if applied:
            st.rerun()
