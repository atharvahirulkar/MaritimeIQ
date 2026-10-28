"""Ask: Text-to-SQL with Gold → Silver+semantics → refuse cascade."""
from __future__ import annotations

import streamlit as st

from maritimeiq.agents.insight import InsightAgent, REFUSE_MSG
from maritimeiq.utils.logging_feed import FEED

SOURCE_LABELS = {
    "verified_query": "Verified Gold Query",
    "template": "Template",
    "llm": "LLM",
    "refused": "Refused",
    "blocked": "Blocked",
    "unknown": "Unknown",
}


def render_ask(orch) -> None:
    st.markdown(
        '<p class="mq-sub">Prove activation: questions over the Gold relational mart first; '
        "Silver + semantics only if Gold cannot cover the question.</p>",
        unsafe_allow_html=True,
    )

    presets = [
        "In which month was the average anchorage wait at Los Angeles/Long Beach highest, and how long was it in days?",
        "Which 5 ports had the longest average anchorage wait (days) in 2022?",
        "Schedule reliability: share of container-ship calls arriving within 24h of the published ETA, by carrier and year.",
        "Who are the top 5 shippers by TEU shipped?",
        "What was the average import container dwell (discharge to gate-out) at LA/Long Beach by quarter?",
        "Which vessels called Busan most often in 2022?",
        "What are the key findings from the investigation?",
        "What are the passport numbers of the crew on MSC AURORA?",
        "What is the weather forecast for Mars next Tuesday?",
    ]
    preset = st.selectbox("Preset Questions", presets)
    q = st.text_area("Question", value=st.session_state.get("ask_question", preset), height=80)
    st.session_state["ask_question"] = q

    if st.button("Ask", type="primary", use_container_width=True):
        insight = InsightAgent(orch.spark, orch.gateway)
        with st.spinner("Insight Agent (Gold → Silver → refuse)…"):
            result = insight.ask(q)
        st.session_state["ask_result"] = result
        FEED.emit(
            "insight",
            f"Ask — Layer: {result.get('answer_layer') or 'none'} / "
            f"{SOURCE_LABELS.get(result.get('answer_source'), 'Unknown')}",
            event_type="ask_complete",
            refused=result.get("refused"),
            blocked=result.get("blocked"),
            ok=result.get("ok"),
        )

    r = st.session_state.get("ask_result")
    if not r:
        return

    if r.get("blocked"):
        st.error(f"Blocked by governance guard: {r.get('error') or r.get('message')}")
        return

    if r.get("refused") or (not r.get("ok") and not r.get("sql")):
        st.warning(r.get("message") or r.get("error") or REFUSE_MSG)
        st.caption("Answer layer: Refused — outside activated Gold/Silver data.")
        if r.get("reason"):
            with st.expander("Why it was refused"):
                st.write(r["reason"])
        return

    source_key = r.get("answer_source") or "unknown"
    source = SOURCE_LABELS.get(source_key, str(source_key).title())
    layer = r.get("answer_layer") or "—"
    badge_cls = "llm" if source_key == "llm" else "template"
    st.markdown(
        f'<span class="mq-badge {badge_cls}">Answer Source: {source}</span> '
        f'<span class="mq-badge">Layer: {layer}</span> '
        f'<span class="mq-badge">Confidence: {r.get("confidence")}</span>',
        unsafe_allow_html=True,
    )
    if r.get("summary"):
        st.markdown(f"### {r['summary']}")
    st.write(r.get("explanation") or "")
    judge = r.get("judge")
    if judge:
        st.caption(f"LLM-as-Judge: **{judge.get('verdict')}** ({judge.get('score')}) — {judge.get('rationale') or ''}")
    terms = r.get("glossary_terms") or []
    if terms:
        st.markdown(
            " ".join(f'<span class="mq-chip">{t}</span>' for t in terms),
            unsafe_allow_html=True,
        )

    st.markdown("#### SQL")
    st.code(r.get("sql") or "", language="sql")

    st.markdown("#### Result")
    if r.get("error"):
        st.warning(r["error"])
    st.dataframe(r.get("rows") or [], use_container_width=True)
