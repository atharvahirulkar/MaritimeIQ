#!/usr/bin/env python3
"""One-command demo: rebuild lakehouse from raw sources and print headline metrics."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Prefer Python used to launch this script for PySpark workers
os.environ.setdefault("MARITIMEIQ_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from maritimeiq.utils.env import load_env

load_env()

from maritimeiq.agents.orchestrator import Orchestrator
from maritimeiq.eval.harness import run_eval
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import ensure_lakehouse_dirs


def main() -> int:
    ensure_lakehouse_dirs()
    FEED.clear()
    print("=== MaritimeIQ demo rebuild ===")
    orch = Orchestrator()
    # Batch demo: soft-approve semantics (no HITL pause), build Gold mart, then Ask.
    state = orch.run(
        through="insight",
        state={"walkthrough": False, "auto_approve_gold": True},
    )
    print("Phase:", state.get("phase"))
    print("Bronze:", {k: (v or {}).get("rows") for k, v in (state.get("bronze_tables") or {}).items() if isinstance(v, dict)})
    print("Silver:", {k: (v or {}).get("rows") for k, v in (state.get("silver_tables") or {}).items()})
    print("Gold mart:", (state.get("gold_mart") or {}).get("materialized"))
    ir = state.get("insight_result") or {}
    print("Insight ok:", ir.get("ok"), "layer:", ir.get("answer_layer"), "blocked:", ir.get("blocked"))
    if ir.get("rows"):
        print("Top answer rows:", json.dumps(ir["rows"][:5], default=str))
    print("Running eval harness…")
    report = run_eval()
    print(json.dumps({
        "semantic_accuracy": report["semantics"].get("semantic_accuracy"),
        "pii_recall": report["semantics"].get("pii_recall"),
        "vessel_f1": report["entity_resolution"].get("vessel_f1"),
        "nl_silver": report["nl_questions"].get("silver_accuracy"),
        "nl_bronze_baseline": report["nl_questions"].get("bronze_accuracy_baseline"),
        "security": report["security"],
    }, indent=2))
    print("Eval markdown: .eval_results/latest.md")
    print("UI: streamlit run src/maritimeiq/ui/app.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
