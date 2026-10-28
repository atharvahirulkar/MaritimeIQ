"""Project path helpers."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
CONFIGS = ROOT / "configs"
LAKEHOUSE = ROOT / "lakehouse"
BRONZE = LAKEHOUSE / "bronze"
SILVER = LAKEHOUSE / "silver"
GOLD = LAKEHOUSE / "gold"
CONTEXT = LAKEHOUSE / "context"
CHECKPOINTS = LAKEHOUSE / "checkpoints"
SOURCES = ROOT / "data" / "hackathon" / "sources"
GROUND_TRUTH = ROOT / "data" / "hackathon" / "ground_truth"
POLICY_PATH = CONFIGS / "policy.yaml"
SPARK_CFG = CONFIGS / "spark.yaml"


def ensure_lakehouse_dirs() -> None:
    for p in (BRONZE, SILVER, GOLD, CONTEXT, CHECKPOINTS):
        p.mkdir(parents=True, exist_ok=True)
