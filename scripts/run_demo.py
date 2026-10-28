#!/usr/bin/env python3
"""Unattended rebuild: Sources → Bronze → Silver → Gold → Ask → eval.

Batch mode is an explicit operator policy, recorded as such:
  * semantic gaps the Judge did not grade "wrong" are approved by `batch-policy`
  * uncertain entity matches stay separate (conservative), ports stay unresolved
  * Gold is approved by `batch-cli --auto-approve-gold` (pass --no-gold to stop at the plan)
Use the Streamlit walkthrough for the human-in-the-loop path.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("MARITIMEIQ_PYTHON", sys.executable)

from maritimeiq.utils.env import load_env

load_env()

from maritimeiq.agents.orchestrator import Orchestrator
from maritimeiq.eval.harness import run_eval
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import ensure_lakehouse_dirs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-gold", action="store_true", help="stop after the Gold investigation plan")
    ap.add_argument("--resume", action="store_true", help="resume from completed stages instead of rebuilding")
    ap.add_argument("--no-eval", action="store_true")
    ap.add_argument("--no-baseline", action="store_true", help="skip the Bronze LLM baseline in eval")
    args = ap.parse_args()

    ensure_lakehouse_dirs()
    FEED.clear()
    print("=== MaritimeIQ batch rebuild ===")
    orch = Orchestrator()
    state = orch.run(
        through="insight",
        fresh=not args.resume,
        state={"walkthrough": False, "auto_approve_gold": not args.no_gold},
    )
    print("Phase:", state.get("phase"), "| completed:", state.get("completed_stages"))
    print("Bronze:", {k: (v or {}).get("rows") for k, v in (state.get("bronze_tables") or {}).items() if isinstance(v, dict)})
    print("Silver:", {k: (v or {}).get("rows") for k, v in (state.get("silver_tables") or {}).items()})
    qc_fail = [k for k, v in (state.get("silver_qc") or {}).items() if not v.get("ok")]
    print("Silver QC failures:", qc_fail or "none")
    gm = state.get("gold_mart") or {}
    print("Gold:", gm.get("materialized") or gm.get("phase"), "errors:", gm.get("errors") or "none")
    ir = state.get("insight_result") or {}
    if ir:
        print("Ask:", ir.get("question"))
        print("  →", ir.get("answer_layer"), ir.get("answer_source"), "|", ir.get("summary") or ir.get("message"))
    if args.no_eval or args.no_gold:
        return 0
    print("Running eval harness…")
    report = run_eval(with_baseline=not args.no_baseline)
    print((ROOT / ".eval_results" / "latest.md").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
