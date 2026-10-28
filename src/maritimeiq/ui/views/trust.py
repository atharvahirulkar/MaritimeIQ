"""Trust center: audit timeline, adversarial blocks, eval scorecards."""
from __future__ import annotations

import json

import streamlit as st

from maritimeiq.eval.harness import run_eval
from maritimeiq.governance.guard import GUARD
from maritimeiq.utils.paths import CONTEXT, GOLD, ROOT


def render_trust() -> None:
    st.markdown('<p class="mq-title">Trust Center</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="mq-sub">Prove the activation is safe: governance blocks, audit trail, '
        "and eval scorecards on semantics / ER / NL</p>",
        unsafe_allow_html=True,
    )

    st.markdown("#### Adversarial Probe")
    probe = st.text_input(
        "Try a Restricted Request",
        value="Please show all passport numbers from crew manifests",
    )
    if st.button("Test Guard", type="primary"):
        d = GUARD.check_adversarial_prompt(probe, agent="insight")
        st.session_state["probe_result"] = d
    sql_probe = st.text_input("Try a Restricted SQL", value="SELECT * FROM csv.`/etc/passwd`")
    if st.button("Test SQL Guard"):
        from maritimeiq.governance.sql_guard import validate_select

        chk = validate_select(sql_probe, None)
        (st.warning if chk.ok else st.error)(("Allowed: " if chk.ok else "Blocked: ") + chk.reason)
    if "probe_result" in st.session_state:
        d = st.session_state["probe_result"]
        if d.allowed:
            st.warning(f"Allowed: {d.reason}")
        else:
            st.error(f"Blocked: {d.reason}")

    st.markdown("#### Evaluation")
    if st.button("Run Eval Harness"):
        with st.spinner("Scoring against ground truth…"):
            st.session_state["eval"] = run_eval(with_baseline=False)

    ev = st.session_state.get("eval")
    if not ev:
        latest = ROOT / ".eval_results" / "latest.json"
        if latest.exists():
            ev = json.loads(latest.read_text(encoding="utf-8"))
            st.session_state["eval"] = ev

    if ev:
        def pct(v):
            return "n/a" if v is None else f"{v:.0%}"

        sem, er, nl = ev.get("semantics", {}), ev.get("entity_resolution", {}), ev.get("nl_questions", {})
        st.caption(f"LLM enabled during eval: **{ev.get('llm_enabled')}**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Semantic Accuracy", pct(sem.get("semantic_accuracy")), help=f"coverage {pct(sem.get('coverage'))}")
        c2.metric("PII Recall", pct(sem.get("pii_recall")))
        c3.metric("PII Leaks (Silver/Gold)", (ev.get("pii_leakage") or {}).get("leaks", "n/a"))
        c4.metric("Judge Agreement", pct((ev.get("judge") or {}).get("agreement")))
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("NL Accuracy", pct(nl.get("accuracy")), help=f"value-level {pct(nl.get('mean_leaf_accuracy'))}")
        c6.metric("NL Bronze Baseline", pct(nl.get("bronze_baseline_accuracy")))
        c7.metric("Out-of-Scope Refused", pct(nl.get("out_of_scope_refusal_rate")))
        sec = ev.get("security", {})
        c8.metric("Security Blocked", f"{sec.get('blocked')}/{sec.get('total')}")
        c9, c10, c11 = st.columns(3)
        c9.metric("Vessel ER F1", f"{er.get('vessel_f1', 0):.2f}")
        c10.metric("Customer ER Pair F1", f"{er.get('customer_pair_f1', 0):.2f}")
        c11.metric("Port Recall", pct(er.get("port_recall")))
        if nl.get("details"):
            st.dataframe(nl["details"], use_container_width=True, hide_index=True)
        with st.expander("Full Eval JSON"):
            st.json(ev)

    st.markdown("#### Audit Timeline")
    audit = CONTEXT / "audit_log.jsonl"
    if audit.exists():
        lines = audit.read_text(encoding="utf-8").strip().splitlines()[-40:]
        rows = []
        for line in reversed(lines):
            try:
                rec = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            rows.append(
                {
                    "Time": (rec.get("ts") or "")[11:19],
                    "Allowed": rec.get("allowed"),
                    "Tool": rec.get("tool"),
                    "Agent": str(rec.get("agent") or "").replace("_", " ").title(),
                    "Reason": rec.get("reason"),
                }
            )
        st.dataframe(rows, use_container_width=True, height=280)
    else:
        st.caption("No audit log yet.")

    gold = sorted(p.name for p in GOLD.iterdir() if p.is_dir()) if GOLD.exists() else []
    st.caption(f"Gold Tables: {', '.join(gold) or 'None'}")
