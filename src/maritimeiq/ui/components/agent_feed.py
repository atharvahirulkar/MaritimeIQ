"""Right-rail agent event feed."""
from __future__ import annotations

import json
import re

import streamlit as st

from maritimeiq.utils.logging_feed import FEED

AGENT_COLORS = {
    "orchestrator": "#c9a227",
    "ingestion": "#2a8f8a",
    "context": "#3d9a6a",
    "entity_resolution": "#c9a227",
    "transform": "#2a8f8a",
    "insight": "#c9a227",
    "judge": "#8aa0b2",
    "guard": "#c45c5c",
    "eval": "#3d9a6a",
    "lakehouse": "#8aa0b2",
    "review": "#3d9a6a",
}

AGENT_LABELS = {
    "orchestrator": "Orchestrator",
    "ingestion": "Ingestion",
    "context": "Context",
    "entity_resolution": "Entity Resolution",
    "transform": "Transform",
    "insight": "Insight",
    "judge": "Judge",
    "guard": "Guard",
    "eval": "Eval",
    "lakehouse": "Lakehouse",
    "review": "Review",
    "reference": "Reference",
}


def _titleize_event(event_type: str) -> str:
    if not event_type:
        return ""
    return re.sub(r"[_\-]+", " ", event_type).title()


def render_agent_feed(n: int = 40) -> None:
    st.markdown("##### Agent Feed")
    events = list(reversed(FEED.recent(n)))
    if not events:
        st.caption("No agent events yet. Run the pipeline or demo script.")
        return
    for ev in events:
        agent = ev.get("agent", "?")
        color = AGENT_COLORS.get(agent, "#8aa0b2")
        level = ev.get("level", "info")
        ts = (ev.get("ts") or "")[11:19]
        msg = ev.get("message", "")
        event_type = (ev.get("data") or {}).get("event_type", "")
        agent_label = AGENT_LABELS.get(agent, str(agent).replace("_", " ").title())
        badge = (
            f' <span class="mq-badge">{_titleize_event(str(event_type))}</span>'
            if event_type
            else ""
        )
        st.markdown(
            f'<div class="mq-feed-item">'
            f'<span class="ts">{ts}</span> '
            f'<span class="agent" style="color:{color}">{agent_label}</span>'
            f"{badge}<br/>{msg}"
            f"</div>",
            unsafe_allow_html=True,
        )
        data = ev.get("data") or {}
        payload = {k: v for k, v in data.items() if k != "event_type"}
        if payload and level in {"warn", "error"} or (
            payload and len(json.dumps(payload, default=str)) < 400
        ):
            with st.expander("Details", expanded=False):
                st.json(payload)
