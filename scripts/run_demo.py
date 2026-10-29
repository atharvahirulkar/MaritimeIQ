#!/usr/bin/env python3
"""Unattended demo: Bronze → Gold with agent suggestions standing in for HITL, then eval."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("MARITIMEIQ_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from maritimeiq.utils.env import load_env

load_env()

from maritimeiq.agents.orchestrator import Orchestrator
from maritimeiq.eval.harness import run_eval


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-ingest", action="store_true", help="reuse existing Bronze")
    ap.add_argument("--no-eval", action="store_true")
    args = ap.parse_args()

    orch = Orchestrator()
    state = orch.run_all(skip_ingest=args.skip_ingest)
    print("Open gaps after run:", state.get("open_gaps"), "| auto-accepted:", state.get("auto_accepted"))
    print("Insights:", state.get("insights"))
    print("Gold:", (state.get("gold_mart") or {}).get("materialized"))
    ans = orch.insight.ask("Which 5 ports had the longest average anchorage wait (days) in 2021?")
    print("Ask:", ans.get("answer_layer"), json.dumps((ans.get("rows") or [])[:5], default=str))
    if not args.no_eval:
        print(open(ROOT / ".eval_results" / "latest.md").read() if run_eval() else "")
    print("UI: make ui")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
