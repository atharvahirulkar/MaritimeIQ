"""MCP-style tool layer (single process, three namespaces) with guard enforcement."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from maritimeiq.governance.guard import GUARD
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, CONTEXT, GOLD, SILVER, SOURCES, ensure_lakehouse_dirs


class LakehouseTools:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        ensure_lakehouse_dirs()

    def list_tables(self, layer: str = "bronze", agent: str = "orchestrator") -> dict[str, Any]:
        root = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD}[layer]
        tables = sorted([p.name for p in root.iterdir() if p.is_dir()]) if root.exists() else []
        FEED.emit("lakehouse", f"list_tables({layer}) -> {len(tables)}", acting_agent=agent)
        return {"layer": layer, "tables": tables}

    def _path(self, table: str) -> Path:
        if table.startswith("bronze."):
            return BRONZE / table.split(".", 1)[1]
        if table.startswith("silver."):
            return SILVER / table.split(".", 1)[1]
        if table.startswith("gold."):
            return GOLD / table.split(".", 1)[1]
        # bare name defaults to bronze
        return BRONZE / table

    def read_sample(
        self,
        table: str,
        n: int = 20,
        columns: list[str] | None = None,
        agent: str = "context",
    ) -> dict[str, Any]:
        decision = GUARD.check_bronze_pii_read(table if table.startswith("bronze") else f"bronze.{table}", columns, agent)
        if not decision.allowed:
            return {"ok": False, "error": decision.reason, "rows": []}
        path = self._path(table if "." in table else f"bronze.{table}")
        if not path.exists():
            return {"ok": False, "error": f"Table not found: {path}", "rows": []}
        df = self.spark.read.format("delta").load(str(path))
        if columns:
            df = df.select(*[c for c in columns if c in df.columns])
        rows = [r.asDict(recursive=True) for r in df.limit(n).collect()]
        qualified = table if "." in table else f"bronze.{table}"
        masked = [GUARD.mask_row(qualified, row) for row in rows]
        return {"ok": True, "table": qualified, "rows": masked, "columns": df.columns}

    def profile_column(self, table: str, column: str, agent: str = "context") -> dict[str, Any]:
        path = self._path(table if "." in table else f"bronze.{table}")
        df = self.spark.read.format("delta").load(str(path))
        if column not in df.columns:
            return {"ok": False, "error": f"Column {column} not in {table}"}
        col = F.col(f"`{column}`")
        # Profile a sample to keep large Bronze tables responsive
        sample_df = df.limit(5000)
        stats = sample_df.select(
            F.count(F.lit(1)).alias("n"),
            F.count(col).alias("non_null"),
            F.countDistinct(col).alias("nunique"),
        ).collect()[0].asDict()
        samples = [
            r[0]
            for r in sample_df.select(col).where(col.isNotNull()).limit(15).collect()
        ]
        qualified = table if "." in table else f"bronze.{table}"
        samples = [GUARD.mask_row(qualified, {column: s})[column] for s in samples]
        FEED.emit("lakehouse", f"profile {qualified}.{column}", acting_agent=agent)
        return {"ok": True, "table": qualified, "column": column, "stats": stats, "samples": samples}

    def run_spark_sql(
        self,
        sql: str,
        agent: str = "transform",
        limit: int = 100,
        allowed_tables: set[str] | None = None,
    ) -> dict[str, Any]:
        decision = GUARD.check_sql(sql, agent=agent, allowed_tables=allowed_tables)
        if not decision.allowed:
            return {"ok": False, "error": decision.reason, "rows": [], "blocked": True}
        try:
            df = self.spark.sql(sql)
            rows = [r.asDict(recursive=True) for r in df.limit(limit).collect()]
            # Tool responses never carry cleartext PII, whatever the query selected
            rows = GUARD.mask_result_rows(rows)
            return {"ok": True, "columns": df.columns, "rows": rows, "count": len(rows)}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e), "rows": []}

    def write_delta(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        agent: str = "ingestion",
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        ensure_lakehouse_dirs()
        if table.startswith("gold.") or table.startswith("gold/"):
            decision = GUARD.check_gold_write(approval_id, agent=agent)
            if not decision.allowed:
                return {"ok": False, "error": decision.reason}
            path = GOLD / table.split(".", 1)[-1].replace("gold/", "")
        elif table.startswith("silver."):
            path = SILVER / table.split(".", 1)[1]
        else:
            name = table.split(".", 1)[-1]
            path = BRONZE / name
        (
            df.write.format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .save(str(path))
        )
        FEED.emit("lakehouse", f"wrote {table} mode={mode}", acting_agent=agent, path=str(path))
        return {"ok": True, "path": str(path), "table": table}


class ReferenceTools:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._unlocode: DataFrame | None = None
        self._wpi: DataFrame | None = None
        self._ais_status = {
            "0": "under way using engine",
            "1": "at anchor",
            "5": "moored",
            "15": "undefined",
        }
        self._ais_type = {
            str(i): "cargo" for i in range(70, 80)
        } | {str(i): "tanker" for i in range(80, 90)}

    def _load_unlocode(self) -> DataFrame:
        if self._unlocode is None:
            path = SOURCES / "reference" / "unlocode_code_list.csv"
            self._unlocode = self.spark.read.option("header", True).csv(str(path))
        return self._unlocode

    def _load_wpi(self) -> DataFrame:
        if self._wpi is None:
            path = SOURCES / "reference" / "world_port_index.csv"
            self._wpi = self.spark.read.option("header", True).csv(str(path))
        return self._wpi

    def lookup_unlocode(self, query: str, agent: str = "context") -> dict[str, Any]:
        df = self._load_unlocode()
        q = query.upper().strip()
        cols = df.columns
        # flexible column match
        name_cols = [c for c in cols if "name" in c.lower() or c.upper() in ("LOCODE", "UNLOCODE", "CODE")]
        pred = None
        for c in cols:
            cond = F.upper(F.col(c)).contains(q)
            pred = cond if pred is None else (pred | cond)
        rows = [r.asDict() for r in df.where(pred).limit(10).collect()] if pred is not None else []
        FEED.emit("reference", f"unlocode lookup {query} -> {len(rows)}", acting_agent=agent)
        return {"ok": True, "query": query, "rows": rows, "matched_cols": name_cols}

    def lookup_ais_status(self, code: str, agent: str = "context") -> dict[str, Any]:
        meaning = self._ais_status.get(str(code), "unknown")
        return {"ok": True, "code": str(code), "meaning": meaning}

    def lookup_ais_type(self, code: str, agent: str = "context") -> dict[str, Any]:
        meaning = self._ais_type.get(str(code), "other/unknown")
        return {"ok": True, "code": str(code), "meaning": meaning}

    def lookup_wpi(self, query: str, agent: str = "context") -> dict[str, Any]:
        df = self._load_wpi()
        q = query.upper().strip()
        pred = None
        for c in df.columns:
            cond = F.upper(F.col(c)).contains(q)
            pred = cond if pred is None else (pred | cond)
        rows = [r.asDict() for r in df.where(pred).limit(10).collect()] if pred is not None else []
        return {"ok": True, "query": query, "rows": rows}


class ReviewTools:
    def __init__(self) -> None:
        ensure_lakehouse_dirs()
        self.tasks_path = CONTEXT / "review_tasks.jsonl"
        self.corrections_path = CONTEXT / "corrections.jsonl"
        self.approvals_path = CONTEXT / "approvals.jsonl"

    def create_review_task(self, kind: str, payload: dict[str, Any], agent: str = "context") -> dict[str, Any]:
        import uuid

        task = {"id": f"REV-{uuid.uuid4().hex[:8]}", "kind": kind, "payload": payload, "status": "open"}
        with self.tasks_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(task) + "\n")
        FEED.emit("review", f"created {task['id']} kind={kind}", acting_agent=agent)
        return {"ok": True, "task": task}

    def read_corrections(self, limit: int = 50, agent: str = "context") -> dict[str, Any]:
        if not self.corrections_path.exists():
            return {"ok": True, "corrections": []}
        lines = self.corrections_path.read_text(encoding="utf-8").strip().splitlines()[-limit:]
        return {"ok": True, "corrections": [json.loads(x) for x in lines if x.strip()]}

    def submit_correction(self, correction: dict[str, Any], agent: str = "human") -> dict[str, Any]:
        with self.corrections_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(correction) + "\n")
        FEED.emit("review", "correction stored", acting_agent=agent)
        return {"ok": True}

    def get_approval(self, purpose: str, agent: str = "human", approver: str = "ui-user") -> dict[str, Any]:
        """Issue an approval id. Only a human (UI action / explicit CLI opt-in) may call this —
        agents must never approve their own writes."""
        import uuid
        from datetime import datetime, timezone

        if agent != "human":
            FEED.emit("review", f"approval refused for agent={agent}", level="warn", event_type="guard_block")
            return {"ok": False, "error": "Only a human can issue approvals"}
        approval_id = f"APPR-{uuid.uuid4().hex[:10]}"
        GUARD.register_approval(approval_id)
        rec = {
            "approval_id": approval_id,
            "purpose": purpose,
            "approver": approver,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        with self.approvals_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
        FEED.emit("review", f"approval {approval_id} for {purpose}", acting_agent=agent)
        return {"ok": True, "approval_id": approval_id}


class MCPGateway:
    """Single-process MCP gateway exposing three namespaces."""

    def __init__(self, spark: SparkSession) -> None:
        self.lakehouse = LakehouseTools(spark)
        self.reference = ReferenceTools(spark)
        self.review = ReviewTools()
        self.spark = spark

    def call(self, namespace: str, tool: str, agent: str = "orchestrator", **kwargs: Any) -> dict[str, Any]:
        ns = getattr(self, namespace, None)
        if ns is None:
            return {"ok": False, "error": f"Unknown namespace {namespace}"}
        fn = getattr(ns, tool, None)
        if fn is None:
            return {"ok": False, "error": f"Unknown tool {namespace}.{tool}"}
        kwargs.setdefault("agent", agent)
        return fn(**kwargs)
