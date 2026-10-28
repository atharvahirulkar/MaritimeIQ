"""MCP-style tool layer (single process) with guard enforcement on every call."""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from maritimeiq.governance.guard import GUARD
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, CONTEXT, GOLD, SILVER, ensure_lakehouse_dirs

LAYERS = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD}


class LakehouseTools:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        ensure_lakehouse_dirs()

    def list_tables(self, layer: str = "bronze", agent: str = "orchestrator") -> dict[str, Any]:
        root = LAYERS[layer]
        tables = sorted(p.name for p in root.iterdir() if p.is_dir()) if root.exists() else []
        return {"layer": layer, "tables": tables}

    @staticmethod
    def _qualify(table: str) -> str:
        return table if "." in table else f"bronze.{table}"

    @staticmethod
    def _path(qualified: str) -> Path:
        layer, name = qualified.split(".", 1)
        return LAYERS[layer] / name

    def load(self, table: str) -> DataFrame:
        return self.spark.read.format("delta").load(str(self._path(self._qualify(table))))

    def read_sample(
        self, table: str, n: int = 20, columns: list[str] | None = None, agent: str = "context"
    ) -> dict[str, Any]:
        qualified = self._qualify(table)
        if qualified.startswith("bronze."):
            decision = GUARD.check_bronze_pii_read(qualified, columns, agent)
            if not decision.allowed:
                return {"ok": False, "error": decision.reason, "rows": []}
        path = self._path(qualified)
        if not path.exists():
            return {"ok": False, "error": f"Table not found: {qualified}", "rows": []}
        df = self.spark.read.format("delta").load(str(path))
        if columns:
            df = df.select(*[c for c in columns if c in df.columns])
        rows = [r.asDict(recursive=True) for r in df.limit(n).collect()]
        masked = [GUARD.mask_row(qualified, row) for row in rows]
        return {"ok": True, "table": qualified, "rows": masked, "columns": df.columns}

    def profile_table(self, table: str, sample_rows: int = 5000, agent: str = "context") -> dict[str, Any]:
        """One-pass profile of every business column: null rate, distinct estimate, masked samples."""
        qualified = self._qualify(table)
        df = self.spark.read.format("delta").load(str(self._path(qualified)))
        cols = [c for c in df.columns if not c.startswith("_")]
        sample = df.limit(sample_rows)
        aggs = [F.count(F.lit(1)).alias("__n")]
        for i, c in enumerate(cols):
            aggs.append(F.count(F.col(f"`{c}`")).alias(f"nn{i}"))
            aggs.append(F.approx_count_distinct(F.col(f"`{c}`")).alias(f"nd{i}"))
        stats = sample.agg(*aggs).collect()[0].asDict()
        n = stats["__n"] or 0
        head = [r.asDict() for r in sample.limit(400).collect()]
        profiles: dict[str, Any] = {}
        for i, c in enumerate(cols):
            vals: list[Any] = []
            for r in head:
                v = r.get(c)
                if v is not None and v not in vals:
                    vals.append(v)
                if len(vals) >= 8:
                    break
            vals = [GUARD.mask_row(qualified, {c: v})[c] for v in vals]
            profiles[c] = {
                "stats": {"n": n, "non_null": stats[f"nn{i}"], "nunique": stats[f"nd{i}"],
                          "null_pct": round(1 - (stats[f"nn{i}"] / n), 3) if n else None},
                "samples": [str(v)[:80] for v in vals],
            }
        FEED.emit("lakehouse", f"profiled {qualified} ({len(cols)} columns)", acting_agent=agent)
        return {"ok": True, "table": qualified, "columns": cols, "profiles": profiles}

    def run_spark_sql(self, sql: str, agent: str = "transform", limit: int = 100) -> dict[str, Any]:
        decision = GUARD.check_sql(sql, agent=agent)
        if not decision.allowed:
            return {"ok": False, "error": decision.reason, "rows": []}
        try:
            df = self.spark.sql(sql)
            rows = [r.asDict(recursive=True) for r in df.limit(limit).collect()]
            return {"ok": True, "columns": df.columns, "rows": rows, "count": len(rows)}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e).split("\n")[0][:400], "rows": []}

    def write_delta(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        agent: str = "ingestion",
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        ensure_lakehouse_dirs()
        layer, name = table.split(".", 1) if "." in table else ("bronze", table)
        if layer == "gold":
            decision = GUARD.check_gold_write(approval_id, agent=agent)
            if not decision.allowed:
                return {"ok": False, "error": decision.reason}
        path = LAYERS[layer] / name
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(str(path))
        FEED.emit("lakehouse", f"wrote {layer}.{name} mode={mode}", acting_agent=agent)
        return {"ok": True, "path": str(path), "table": f"{layer}.{name}"}


class ReviewTools:
    """Human approvals required for governed writes (Gold)."""

    def __init__(self) -> None:
        ensure_lakehouse_dirs()
        self.approvals_path = CONTEXT / "approvals.jsonl"

    def get_approval(self, purpose: str, agent: str = "human") -> dict[str, Any]:
        approval_id = f"APPR-{uuid.uuid4().hex[:10]}"
        GUARD.register_approval(approval_id)
        with self.approvals_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"approval_id": approval_id, "purpose": purpose}) + "\n")
        FEED.emit("review", f"approval {approval_id} for {purpose}", acting_agent=agent)
        return {"ok": True, "approval_id": approval_id}


class MCPGateway:
    """Single-process gateway exposing the `lakehouse` and `review` tool namespaces."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.lakehouse = LakehouseTools(spark)
        self.review = ReviewTools()
