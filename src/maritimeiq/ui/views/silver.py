"""Transforms view: Bronze → Silver activation with masked samples."""
from __future__ import annotations

import streamlit as st

from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.paths import BRONZE, GOLD, SILVER


def render_silver(orch) -> None:
    st.markdown('<p class="mq-title">Transforms</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="mq-sub">Medallion activation — typed, resolved, PII-masked Silver that AI agents may query. '
        "Insight never reads raw Bronze PII.</p>",
        unsafe_allow_html=True,
    )

    bronze = sorted(p.name for p in BRONZE.iterdir() if p.is_dir()) if BRONZE.exists() else []
    tables = sorted(p.name for p in SILVER.iterdir() if p.is_dir()) if SILVER.exists() else []
    gold = sorted(p.name for p in GOLD.iterdir() if p.is_dir()) if GOLD.exists() else []

    c1, c2, c3 = st.columns(3)
    c1.metric("Bronze Tables", len(bronze))
    c2.metric("Silver Tables", len(tables))
    c3.metric("Gold Tables", len(gold))

    if not tables:
        st.warning("No Silver tables yet. Run pipeline through Silver or Insight.")
        if bronze:
            st.caption("Bronze ready: " + ", ".join(bronze))
        return

    masters = [
        t
        for t in tables
        if t.startswith("master_") or t.endswith("_aliases") or "record_map" in t
    ]
    facts = [t for t in tables if t not in masters]
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Entity Masters (ER Output)**")
        st.write(", ".join(masters) or "—")
    with c2:
        st.markdown("**Activated Fact Tables**")
        st.write(", ".join(facts) or "—")

    if gold:
        st.markdown("**Gold (Approved AI Products)**")
        st.write(", ".join(gold))

    st.markdown("#### Silver Sample")
    st.caption("Samples go through the governance guard — PII columns masked when tagged in the semantic layer.")
    default_idx = tables.index("port_agent_events") if "port_agent_events" in tables else 0
    table = st.selectbox("Preview Table", tables, index=default_idx)
    n = st.slider("Rows", 5, 50, 20)
    if st.button("Load Sample", type="primary") or st.session_state.get("silver_auto"):
        gw = MCPGateway(orch.spark)
        sample = gw.lakehouse.read_sample(f"silver.{table}", n=n, agent="ui")
        if sample.get("error"):
            st.error(sample["error"])
        elif sample.get("rows"):
            st.dataframe(sample["rows"], use_container_width=True)
        else:
            st.info("Empty sample.")
