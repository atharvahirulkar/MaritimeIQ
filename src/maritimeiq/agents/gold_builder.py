"""Act 2: investigate Silver + semantics → relational Gold mart + catalog."""
from __future__ import annotations

from typing import Any

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONTEXT, GOLD, SILVER, ensure_lakehouse_dirs


class GoldBuilderAgent:
    """Build a small star-style Gold mart from activated Silver."""

    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()

    def _silver_names(self) -> list[str]:
        if not SILVER.exists():
            return []
        return sorted(p.name for p in SILVER.iterdir() if p.is_dir())

    def _load_df(self, name: str):
        return self.spark.read.format("delta").load(str(SILVER / name))

    def _semantics_summary(self) -> dict[str, Any]:
        sem = self.store.load_semantics()
        glossary = []
        for table, meta in (sem.get("tables") or {}).items():
            for col in meta.get("columns") or []:
                glossary.append(
                    {
                        "table": table,
                        "column": col.get("name"),
                        "meaning": col.get("meaning"),
                        "pii": col.get("pii"),
                    }
                )
        return {"glossary": glossary[:200], "table_count": len(sem.get("tables") or {})}

    def propose_mart(self) -> dict[str, Any]:
        """Return catalog proposal describing relational Gold tables."""
        silver = set(self._silver_names())
        semantics = self._semantics_summary()
        proposals: list[dict[str, Any]] = []

        if "master_vessel" in silver or "vessel_aliases" in silver:
            proposals.append(
                {
                    "table_name": "gold.dim_vessel",
                    "kind": "dimension",
                    "grain": "one row per master_vessel_id",
                    "answers": ["vessel identity", "aliases", "MMSI linkage"],
                    "join_keys": ["master_vessel_id"],
                    "source": "master_vessel / vessel_aliases",
                }
            )
        if "master_port" in silver:
            proposals.append(
                {
                    "table_name": "gold.dim_port",
                    "kind": "dimension",
                    "grain": "one row per master_port / UNLOCODE",
                    "answers": ["port identity", "country"],
                    "join_keys": ["master_port"],
                    "source": "master_port",
                }
            )
        if "master_customer" in silver or "customer_record_map" in silver:
            proposals.append(
                {
                    "table_name": "gold.dim_customer",
                    "kind": "dimension",
                    "grain": "one row per master_customer_id",
                    "answers": ["shipper / consignee identity"],
                    "join_keys": ["master_customer_id"],
                    "source": "master_customer",
                }
            )
        if "port_agent_events" in silver:
            proposals.append(
                {
                    "table_name": "gold.fact_anchorage_wait",
                    "kind": "fact",
                    "grain": "ANC→BRT wait pair per vessel/port",
                    "answers": ["anchorage wait", "congestion", "longest wait"],
                    "join_keys": ["master_vessel_id", "master_port"],
                    "source": "port_agent_events",
                }
            )
            proposals.append(
                {
                    "table_name": "gold.monthly_port_congestion",
                    "kind": "aggregate",
                    "grain": "port × month",
                    "answers": ["average wait by month", "busiest months"],
                    "join_keys": ["master_port", "month"],
                    "source": "fact_anchorage_wait",
                }
            )
        if "container_events" in silver:
            proposals.append(
                {
                    "table_name": "gold.fact_container_holds",
                    "kind": "fact",
                    "grain": "HOLD events by port/carrier",
                    "answers": ["customs holds", "equipment reassignments"],
                    "join_keys": ["master_port", "carrier"],
                    "source": "container_events",
                }
            )
        proposals.append(
            {
                "table_name": "gold.v_ops_overview",
                "kind": "view",
                "grain": "wide analyst surface joining dims + key facts",
                "answers": ["cross-entity ops questions for Text-to-SQL"],
                "join_keys": ["master_vessel_id", "master_port"],
                "source": "gold dims + facts",
            }
        )

        FEED.emit(
            "insight",
            f"Gold mart proposed: {len(proposals)} tables from {len(silver)} Silver tables",
            event_type="gold_mart_proposed",
            n=len(proposals),
            semantics_tables=semantics["table_count"],
        )
        return {
            "ok": True,
            "silver_tables": sorted(silver),
            "semantics": semantics,
            "proposals": proposals,
            "primary_view": "gold.v_ops_overview",
        }

    def _register_temps(self) -> None:
        for name in self._silver_names():
            try:
                self._load_df(name).createOrReplaceTempView(f"silver_{name}")
            except Exception:  # noqa: BLE001
                continue

    def _write_gold_df(self, df, table: str, approval_id: str) -> dict[str, Any]:
        return self.gateway.lakehouse.write_delta(
            df, f"gold.{table}", agent="insight", approval_id=approval_id
        )

    def materialize(self, approve: bool = True) -> dict[str, Any]:
        """Build Gold dims/facts/overview and persist gold_catalog.yaml."""
        ensure_lakehouse_dirs()
        proposal = self.propose_mart()
        if not approve:
            return {**proposal, "materialized": [], "phase": "gold_proposed"}

        approval = self.gateway.review.get_approval("gold.relational_mart", agent="human")
        approval_id = approval["approval_id"]
        decision = GUARD.check_gold_write(approval_id, agent="insight")
        if not decision.allowed:
            return {"ok": False, "error": decision.reason, **proposal}

        self._register_temps()
        silver = set(self._silver_names())
        written: list[str] = []
        errors: list[str] = []

        def ok_write(df, name: str) -> None:
            res = self._write_gold_df(df, name, approval_id)
            if res.get("ok"):
                written.append(f"gold.{name}")
            else:
                errors.append(f"{name}: {res.get('error')}")

        # Dimensions
        if "master_vessel" in silver:
            try:
                ok_write(self._load_df("master_vessel"), "dim_vessel")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"dim_vessel: {exc}")
        elif "vessel_aliases" in silver:
            try:
                ok_write(self._load_df("vessel_aliases"), "dim_vessel")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"dim_vessel: {exc}")

        if "master_port" in silver:
            try:
                ok_write(self._load_df("master_port"), "dim_port")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"dim_port: {exc}")

        if "master_customer" in silver:
            try:
                ok_write(self._load_df("master_customer"), "dim_customer")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"dim_customer: {exc}")

        # Anchorage wait fact
        if "port_agent_events" in silver:
            try:
                wait_sql = """
                WITH ordered AS (
                  SELECT master_vessel_id, vessel_name, master_port, event_code, event_ts,
                         lead(event_code) OVER (
                           PARTITION BY coalesce(master_vessel_id, vessel_name), master_port
                           ORDER BY event_ts
                         ) AS next_code,
                         lead(event_ts) OVER (
                           PARTITION BY coalesce(master_vessel_id, vessel_name), master_port
                           ORDER BY event_ts
                         ) AS next_ts
                  FROM silver_port_agent_events
                  WHERE event_code IN ('ANC','BRT') AND event_ts IS NOT NULL
                )
                SELECT master_vessel_id, vessel_name, master_port,
                       event_ts AS anc_ts, next_ts AS brt_ts,
                       (unix_timestamp(next_ts) - unix_timestamp(event_ts)) / 3600.0 AS wait_h
                FROM ordered
                WHERE event_code = 'ANC' AND next_code = 'BRT'
                  AND (unix_timestamp(next_ts) - unix_timestamp(event_ts)) BETWEEN 0 AND 60*24*40
                """
                wait_df = self.spark.sql(wait_sql)
                ok_write(wait_df, "fact_anchorage_wait")
                wait_df.createOrReplaceTempView("gold_fact_anchorage_wait")
                monthly = self.spark.sql(
                    """
                    SELECT master_port,
                           date_format(anc_ts, 'yyyy-MM') AS month,
                           round(avg(wait_h) / 24.0, 2) AS avg_wait_days,
                           count(*) AS pairs
                    FROM gold_fact_anchorage_wait
                    GROUP BY 1, 2
                    """
                )
                ok_write(monthly, "monthly_port_congestion")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"anchorage facts: {exc}")

        if "container_events" in silver:
            try:
                holds = (
                    self._load_df("container_events")
                    .where(F.col("event_code").isin("HOLD", "ASGN"))
                    .groupBy("master_port", "carrier", "event_code")
                    .agg(F.count(F.lit(1)).alias("event_count"))
                )
                ok_write(holds, "fact_container_holds")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"fact_container_holds: {exc}")

        # Wide overview view (persisted as Delta table for Spark SQL)
        try:
            parts = []
            if (GOLD / "fact_anchorage_wait").exists() or "gold.fact_anchorage_wait" in written:
                try:
                    self.spark.read.format("delta").load(str(GOLD / "fact_anchorage_wait")).createOrReplaceTempView(
                        "g_wait"
                    )
                    parts.append("wait")
                except Exception:  # noqa: BLE001
                    pass
            if (GOLD / "dim_vessel").exists() or "gold.dim_vessel" in written:
                try:
                    self.spark.read.format("delta").load(str(GOLD / "dim_vessel")).createOrReplaceTempView(
                        "g_vessel"
                    )
                    parts.append("vessel")
                except Exception:  # noqa: BLE001
                    pass
            if (GOLD / "dim_port").exists() or "gold.dim_port" in written:
                try:
                    self.spark.read.format("delta").load(str(GOLD / "dim_port")).createOrReplaceTempView("g_port")
                    parts.append("port")
                except Exception:  # noqa: BLE001
                    pass

            if "wait" in parts:
                overview_sql = "SELECT w.* "
                if "vessel" in parts:
                    # best-effort left join if key exists
                    overview_sql = """
                    SELECT w.*, v.*
                    FROM g_wait w
                    LEFT JOIN g_vessel v ON w.master_vessel_id = v.master_vessel_id
                    """
                else:
                    overview_sql = "SELECT * FROM g_wait"
                overview = self.spark.sql(overview_sql)
                # Deduplicate column names for Delta
                seen = set()
                rename = []
                for c in overview.columns:
                    if c in seen:
                        rename.append(F.col(c).alias(f"{c}_dup"))
                    else:
                        seen.add(c)
                        rename.append(F.col(c))
                overview = overview.select(*rename)
                ok_write(overview, "v_ops_overview")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"v_ops_overview: {exc}")

        catalog = {
            "primary_view": "gold.v_ops_overview",
            "semantics_tables": proposal["semantics"]["table_count"],
            "tables": proposal["proposals"],
            "materialized": written,
            "errors": errors,
        }
        (CONTEXT / "gold_catalog.yaml").write_text(
            yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8"
        )
        FEED.emit(
            "insight",
            f"Gold mart materialized: {len(written)} tables",
            event_type="gold_mart_built",
            tables=written,
        )
        return {
            "ok": len(written) > 0,
            "phase": "gold_done",
            "materialized": written,
            "errors": errors,
            "catalog": catalog,
            "proposals": proposal["proposals"],
        }

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        approve = state.get("auto_approve_gold", False)
        # Walkthrough: materialize when user confirms (approve True from UI Continue on gold step)
        result = self.materialize(approve=True if state.get("walkthrough") else approve or True)
        state["gold_mart"] = result
        state["phase"] = "gold_done" if result.get("ok") else state.get("phase", "silver_done")
        return state
