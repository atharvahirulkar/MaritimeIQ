"""Data activation hub: sources, field semantics, transforms, HITL — not fleet ops."""
from __future__ import annotations

import json
from typing import Any

import streamlit as st

from maritimeiq.context_store.store import ContextStore
from maritimeiq.ui.atmosphere import render_hero_lanes
from maritimeiq.ui.components.hitl_queue import open_review_items
from maritimeiq.utils.paths import BRONZE, GOLD, ROOT, SILVER, SOURCES


def _kpi(label: str, value: Any) -> None:
    st.markdown(
        f'<div class="mq-kpi mq-rise"><div class="label">{label}</div><div class="value">{value}</div></div>',
        unsafe_allow_html=True,
    )


def _load_manifest() -> dict[str, Any]:
    path = ROOT / "data" / "hackathon" / "manifest.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _bronze_tables() -> list[str]:
    if not BRONZE.exists():
        return []
    return sorted(p.name for p in BRONZE.iterdir() if p.is_dir())


def _silver_tables() -> list[str]:
    if not SILVER.exists():
        return []
    return sorted(p.name for p in SILVER.iterdir() if p.is_dir())


def _field_stats(store: ContextStore) -> dict[str, Any]:
    sem = store.load_semantics().get("tables", {})
    proposals = store.load_proposals()
    approved_cols = 0
    pii_cols = 0
    for table in sem.values():
        for col in table.get("columns") or []:
            approved_cols += 1
            if col.get("pii"):
                pii_cols += 1
    proposed_tables = len(proposals)
    proposed_cols = sum(len((v or {}).get("columns") or []) for v in proposals.values())
    return {
        "approved_tables": len(sem),
        "approved_fields": approved_cols,
        "pii_fields": pii_cols,
        "proposed_tables": proposed_tables,
        "proposed_fields": proposed_cols,
    }


def _transform_map() -> list[dict[str, str]]:
    """Known Bronze → Silver activation edges for the demo lakehouse."""
    edges = [
        ("ais_positions", "ais_positions", "Typed AIS facts + vessel keys"),
        ("ais_live", "ais_live", "Live AIS slice for NL questions"),
        ("port_agent_calls", "port_agent_events", "ANC→BRT wait events"),
        ("bills_of_lading", "bills_of_lading", "Shipper / consignee resolved"),
        ("container_events", "container_events", "Hold / reassignment facts"),
        ("carrier_schedules", "carrier_schedules", "Schedule facts"),
        ("customers", "customers / master_customer", "CRM + customer master"),
        ("crew_manifests", "crew_manifests", "PII-masked crew sample"),
        ("(ER)", "master_vessel / vessel_aliases", "Vessel entity resolution"),
        ("(ER)", "master_port", "Port UN/LOCODE master"),
        ("(ER)", "customer_record_map", "Customer record linkage"),
    ]
    bronze = set(_bronze_tables())
    silver = set(_silver_tables())
    rows = []
    for src, dst, note in edges:
        dst_key = dst.split(" / ")[0].strip()
        if src.startswith("("):
            status = "Active" if dst_key in silver or any(d in silver for d in dst.split(" / ")) else "Pending"
        else:
            b_ok = src in bronze
            s_ok = any(part.strip() in silver for part in dst.split("/"))
            if b_ok and s_ok:
                status = "Active"
            elif b_ok:
                status = "Bronze Only"
            else:
                status = "Pending"
        rows.append({"Source / Stage": src, "Activated As": dst, "Purpose": note, "Status": status})
    return rows


def render_overview(orch, get_orch) -> None:
    st.markdown('<p class="mq-title">Data Activation</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="mq-sub">Ingest sources → propose field semantics → HITL approve → '
        "transform to governed Silver → activate for AI questions</p>",
        unsafe_allow_html=True,
    )
    render_hero_lanes()

    store = ContextStore()
    fields = _field_stats(store)
    hitl_n = len(open_review_items())
    bronze = _bronze_tables()
    silver = _silver_tables()
    gold_n = len([p for p in GOLD.iterdir() if p.is_dir()]) if GOLD.exists() else 0
    manifest = _load_manifest()
    source_meta = (manifest.get("sources") or {}) if manifest else {}

    nl = "—"
    eval_path = ROOT / ".eval_results" / "latest.json"
    if eval_path.exists():
        try:
            nl = f"{json.loads(eval_path.read_text())['nl_questions']['silver_accuracy']:.0%}"
        except Exception:  # noqa: BLE001
            pass

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi("Bronze Sources", len(bronze) or len(source_meta) or "—")
    with c2:
        _kpi("Fields With Semantics", fields["approved_fields"] or "—")
    with c3:
        _kpi("Open HITL Tasks", hitl_n)
    with c4:
        _kpi("Silver Tables Activated", len(silver) or "—")

    c5, c6, c7 = st.columns(3)
    with c5:
        _kpi("PII Fields Tagged", fields["pii_fields"] or "—")
    with c6:
        _kpi("Gold Tables", gold_n or "—")
    with c7:
        _kpi("NL On Activated Silver", nl)

    st.markdown("#### Sources")
    st.caption("Raw feeds registered for the pipeline. Activation means Bronze ingest + semantic coverage.")
    sem_tables = store.load_semantics().get("tables", {})
    src_rows = []
    for name, meta in sorted(source_meta.items()):
        if name == "reference":
            src_rows.append(
                {
                    "Source": "reference (UN/LOCODE, WPI)",
                    "Manifest Rows": "—",
                    "Bronze": "Yes" if any(t.startswith("ref_") for t in bronze) else "No",
                    "Semantics": "Yes"
                    if any(t.startswith("ref_") for t in sem_tables)
                    else "Partial",
                }
            )
            continue
        rows = meta.get("rows", "—") if isinstance(meta, dict) else "—"
        src_rows.append(
            {
                "Source": name,
                "Manifest Rows": rows,
                "Bronze": "Yes" if name in bronze else "No",
                "Semantics": "Yes" if name in sem_tables else "No",
            }
        )
    # Include bronze-only tables not in manifest keys
    known = {r["Source"] for r in src_rows}
    for t in bronze:
        if t not in known and not t.startswith("ref_"):
            src_rows.append(
                {
                    "Source": t,
                    "Manifest Rows": "—",
                    "Bronze": "Yes",
                    "Semantics": "Yes" if t in sem_tables else "No",
                }
            )
    if src_rows:
        st.dataframe(src_rows, use_container_width=True, hide_index=True)
    else:
        st.info("No sources yet — run Bronze ingest from the pipeline rail.")

    if SOURCES.exists():
        st.caption(f"On-disk source root: `{SOURCES.relative_to(ROOT)}`")

    st.markdown("#### Field Semantics")
    st.caption("Agent-proposed meanings, types, and PII tags — HITL decides what becomes the semantic layer.")
    sem_tables = store.load_semantics().get("tables", {})
    if not sem_tables:
        proposals = store.load_proposals()
        if proposals:
            st.warning(
                f"{fields['proposed_fields']} field proposals across {fields['proposed_tables']} tables "
                "await Context / HITL. Open Review to approve."
            )
            t = st.selectbox("Inspect Proposals", sorted(proposals.keys()))
            st.dataframe(proposals.get(t, {}).get("columns", []), use_container_width=True)
        else:
            st.info("No semantic proposals yet. Run pipeline through Context.")
    else:
        t = st.selectbox("Inspect Approved Table", sorted(sem_tables.keys()))
        cols = sem_tables[t].get("columns", [])
        view = [
            {
                "Field": c.get("name"),
                "Meaning": c.get("meaning"),
                "Type Hint": c.get("data_type_hint"),
                "PII": "Yes" if c.get("pii") else "No",
                "Status": str(c.get("status") or "approved").replace("_", " ").title(),
            }
            for c in cols
        ]
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.markdown("#### Transforms (Bronze → Silver)")
    st.caption("How raw sources become AI-ready tables Insight is allowed to query.")
    st.dataframe(_transform_map(), use_container_width=True, hide_index=True)
    st.caption("Open Transforms for masked Silver samples. Gold materializes only after Ask approval.")

    learning = st.session_state.get("pipeline_state", {}).get("context_learning")
    if learning:
        st.success(
            f"HITL learning loop — Source 1 reviews: {learning.get('source1_needs_review')}, "
            f"Source 2 reviews: {learning.get('source2_needs_review')}"
        )

    st.markdown("#### Demo Script")
    st.write(
        "Walk the activation story: Sources & fields → HITL semantics → Transforms → "
        "Ask on governed Silver → Trust / eval proof."
    )
    if st.button("Advance Demo Beat", type="primary"):
        beat = st.session_state.get("demo_beat", 0)
        st.session_state["demo_beat"] = (beat + 1) % 5
        cycle = ["Activation", "Review", "Transforms", "Ask", "Trust"]
        st.session_state["console_mode"] = cycle[st.session_state["demo_beat"]]
        st.rerun()
