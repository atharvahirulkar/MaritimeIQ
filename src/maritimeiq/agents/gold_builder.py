"""Gold builder: turn Silver + discovery findings into an AI-ready relational mart.

Gold = conformed dimensions + facts + one aggregate per selected finding + a wide
analyst view, described in ``gold_catalog.yaml`` (grain, columns, join keys) so the
Text-to-SQL agent can answer from Gold first.
"""
from __future__ import annotations

import shutil
from typing import Any

import yaml
from pyspark.sql import SparkSession

from maritimeiq.agents.discovery import INSIGHTS, DiscoveryAgent
from maritimeiq.agents.vocab import SILVER_COLUMN_DOCS
from maritimeiq.context_store.store import ContextStore
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import CONTEXT, GOLD, ensure_lakehouse_dirs

GOLD_CATALOG = CONTEXT / "gold_catalog.yaml"

BASE_TABLES: list[dict[str, Any]] = [
    {"name": "dim_vessel", "kind": "dimension", "grain": "one row per master_vessel_id", "requires": "silver_master_vessel",
     "description": "Resolved vessels (IMO-centric) with name/MMSI history counts",
     "sql": "SELECT master_vessel_id, imo, primary_name AS vessel_name, name_count, mmsi_count, mmsi_list, sources FROM silver_master_vessel"},
    {"name": "dim_port", "kind": "dimension", "grain": "one row per UN/LOCODE", "requires": "silver_master_port",
     "description": "Ports (UN/LOCODE) with name and country",
     "sql": "SELECT master_port, port_name, country FROM silver_master_port"},
    {"name": "dim_customer", "kind": "dimension", "grain": "one row per master_customer_id", "requires": "silver_master_customer",
     "description": "Resolved customers (tax id hashed)",
     "sql": "SELECT master_customer_id, legal_name, crm_record_count FROM silver_master_customer"},
    {"name": "fact_port_call", "kind": "fact", "grain": "one row per vessel port call", "requires": "derived_port_calls",
     "description": "Vessel port calls with anchorage (ANC) and berthing (BRT) times and wait in days",
     "sql": "SELECT * FROM derived_port_calls"},
    {"name": "fact_container_event", "kind": "fact", "grain": "one row per container milestone", "requires": "silver_container_events",
     "description": "Unified container milestones (canonical event_code, port, booking, carrier)",
     "sql": "SELECT carrier, container_id, booking_ref, event_code, empty_indicator, master_port, event_ts, "
            "master_vessel_id, teu FROM silver_container_events"},
    {"name": "fact_shipment_line", "kind": "fact", "grain": "one row per B/L container line", "requires": "silver_bills_of_lading",
     "description": "Bill of lading container lines with POL/POD, TEU, shipper and consignee customers",
     "sql": "SELECT BLNumber AS bl_number, booking_ref, container_id, Carrier AS carrier, pol, pod, issue_date, teu, "
            "gross_weight_kg, ShipperName AS shipper_name, shipper_customer_id, ConsigneeName AS consignee_name, "
            "consignee_customer_id, master_vessel_id FROM silver_bills_of_lading"},
    {"name": "v_port_call_overview", "kind": "view", "grain": "one row per vessel port call (wide)",
     "requires": "derived_port_calls",
     "description": "Port calls joined to vessel and port dimensions — the single wide surface for cross-entity questions",
     "sql": """SELECT c.port_call_id, c.master_vessel_id, v.imo, v.primary_name AS vessel_name, v.name_count,
                      c.master_port, p.port_name, p.country, c.anc_ts, c.brt_ts, c.arrival_ts, c.wait_days,
                      year(c.arrival_ts) AS year, date_format(c.arrival_ts, 'yyyy-MM') AS month
               FROM derived_port_calls c
               LEFT JOIN silver_master_vessel v ON c.master_vessel_id = v.master_vessel_id
               LEFT JOIN silver_master_port p ON c.master_port = p.master_port"""},
]

JOIN_KEYS = {"master_vessel_id": "dim_vessel", "master_port": "dim_port", "pol": "dim_port", "pod": "dim_port",
             "shipper_customer_id": "dim_customer", "consignee_customer_id": "dim_customer",
             "master_customer_id": "dim_customer"}
DIM_KEYS = {"dim_vessel": "master_vessel_id", "dim_port": "master_port", "dim_customer": "master_customer_id"}


class GoldBuilderAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()
        self.discovery = DiscoveryAgent(spark, gateway)

    def plan(self) -> list[dict[str, Any]]:
        """Gold tables to build: base mart + one aggregate per selected finding."""
        findings = (self.store.load_json(INSIGHTS, {}) or {}).get("findings") or []
        plan = [dict(t) for t in BASE_TABLES]
        for f in findings:
            if f.get("selected"):
                plan.append({
                    "name": f["gold_table"], "kind": "aggregate", "grain": f.get("grain"), "requires": None,
                    "description": f"{f['title']} — {f.get('headline') or f['why']}", "sql": f["sql"],
                    "finding_id": f["id"],
                })
        return plan

    def _columns_doc(self, cols: list[str]) -> dict[str, str]:
        docs = {}
        for c in cols:
            if c in SILVER_COLUMN_DOCS:
                docs[c] = SILVER_COLUMN_DOCS[c]
            elif c in JOIN_KEYS:
                docs[c] = f"Join key to gold.{JOIN_KEYS[c]}"
            else:
                docs[c] = ""
        return docs

    def materialize(self) -> dict[str, Any]:
        ensure_lakehouse_dirs()
        approval_id = self.gateway.review.get_approval("gold.relational_mart", agent="human")["approval_id"]
        views = self.discovery.register_views()
        available = {f"silver_{k}" for k in views} | {k for k in views if k.startswith("derived_")}
        written, errors, catalog_tables = [], [], []
        for t in self.plan():
            if t.get("requires") and t["requires"] not in available:
                continue
            try:
                df = self.spark.sql(t["sql"])
                res = self.gateway.lakehouse.write_delta(df, f"gold.{t['name']}", agent="gold_builder", approval_id=approval_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{t['name']}: {str(exc).splitlines()[0][:200]}")
                continue
            if not res.get("ok"):
                errors.append(f"{t['name']}: {res.get('error')}")
                continue
            written.append(t["name"])
            keys = [c for c in df.columns if c in JOIN_KEYS]
            catalog_tables.append({
                "table_name": f"gold.{t['name']}", "kind": t["kind"], "grain": t.get("grain"),
                "description": t["description"], "columns": self._columns_doc(df.columns),
                "join_keys": {k: f"gold.{JOIN_KEYS[k]}" for k in keys}, "finding_id": t.get("finding_id"),
            })
        catalog = {
            "primary_view": "gold.v_port_call_overview" if "v_port_call_overview" in written else None,
            "tables": catalog_tables,
            "relationships": [
                {"from": f"{t['table_name']}.{k}", "to": f"{dim}.{DIM_KEYS[dim.split('.')[-1]]}"}
                for t in catalog_tables if t["kind"] != "dimension"
                for k, dim in t["join_keys"].items()
            ],
            "materialized": [f"gold.{n}" for n in written],
            "errors": errors,
        }
        GOLD_CATALOG.write_text(yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8")
        FEED.emit("gold_builder", f"Gold mart materialized: {len(written)} tables", event_type="gold_mart_built",
                  tables=written, errors=errors)
        return {"ok": bool(written), "materialized": written, "errors": errors}

    def run(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(state or {})
        for p in GOLD.iterdir() if GOLD.exists() else []:
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
        state["gold_mart"] = self.materialize()
        return state


def load_gold_catalog() -> dict[str, Any]:
    if not GOLD_CATALOG.exists():
        return {"tables": [], "materialized": []}
    return yaml.safe_load(GOLD_CATALOG.read_text(encoding="utf-8")) or {"tables": []}
