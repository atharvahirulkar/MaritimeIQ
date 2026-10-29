"""Entity resolution: vessel / port / customer masters that connect disparate sources.

Uncertain matches become HITL entity gaps; human decisions are replayed on every run.
"""
from __future__ import annotations

import hashlib
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from rapidfuzz import fuzz, process

from maritimeiq.agents.vocab import PortResolver, clean_imo, normalize_vessel_name
from maritimeiq.context_store.store import ContextStore
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, ensure_lakehouse_dirs

ENTITY_GAPS = "entity_gaps.json"


def _sid(prefix: str, *parts: Any) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return f"{prefix}-{hashlib.sha1(raw.encode()).hexdigest()[:8].upper()}"


def apply_entity_decision(gap_id: str, decision: str, value: str | None = None) -> dict[str, Any]:
    """approve/edit link the record to a master id (replayed on the next ER run); reject keeps it separate."""
    store = ContextStore()
    gap = next((g for g in store.load_gaps(ENTITY_GAPS) if g["id"] == gap_id), None)
    if gap is None:
        return {"ok": False, "error": "gap not found"}
    if decision == "approve":
        target = gap.get("suggestion")
    elif decision == "edit":
        target = (value or "").strip().upper()
        prefix = "VES-" if gap.get("entity") == "vessel" else "CUST"
        if not target.startswith(prefix):
            return {"ok": False, "error": f"'{value}' is not a {gap.get('entity')} master id ({prefix}…)"}
    else:
        target = None
    store.resolve_gap(ENTITY_GAPS, gap_id, decision, target)
    store.append_correction({"kind": "entity_gap", "entity": gap.get("entity"), "id": gap_id,
                             "action": decision, "value": target})
    return {"ok": True}


class EntityResolutionAgent:
    LINK_AT = 90
    REVIEW_AT = 75

    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()
        ensure_lakehouse_dirs()

    def _decisions(self) -> dict[str, dict[str, Any]]:
        return {g["id"]: g for g in self.store.load_gaps(ENTITY_GAPS) if g.get("status") == "resolved"}

    def run(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(state or {})
        FEED.emit("entity_resolution", "Resolving vessels, ports, customers across sources")
        decisions = self._decisions()
        vessels = self.resolve_vessels(decisions)
        ports = self.resolve_ports()
        customers = self.resolve_customers(decisions)
        gaps = vessels.pop("gaps") + customers.pop("gaps")
        # Keep prior human decisions; refresh open gaps from this run.
        merged = list(decisions.values()) + [g for g in gaps if g["id"] not in decisions]
        self.store.save_gaps(ENTITY_GAPS, merged)
        state["masters"] = {"vessels": vessels, "ports": ports, "customers": customers}
        open_n = sum(1 for g in merged if g.get("status") == "open")
        FEED.emit(
            "entity_resolution",
            f"masters vessels={vessels['count']} ports={ports['count']} customers={customers['count']} "
            f"open_entity_gaps={open_n}",
            event_type="er_complete",
        )
        return state

    # --- vessels ----------------------------------------------------------------
    def _vessel_identities(self) -> list[dict[str, Any]]:
        def load(name: str):
            p = BRONZE / name
            return self.spark.read.format("delta").load(str(p)) if p.exists() else None

        frames = []
        ais = load("ais_positions")
        if ais is not None:
            frames.append(ais.select(
                F.col("MMSI").cast("string").alias("mmsi"), F.col("IMO").cast("string").alias("imo"),
                F.col("VesselName").alias("name"), F.col("CallSign").alias("call_sign"),
                F.lit("ais_positions").alias("src"),
            ))
        pac = load("port_agent_calls")
        if pac is not None:
            cols = set(pac.columns)

            def coal(*names: str):
                parts = [F.col(n).cast("string") for n in names if n in cols]
                return F.coalesce(*parts) if parts else F.lit(None).cast("string")

            vjson = F.col("vessel") if "vessel" in cols else F.lit(None).cast("string")
            frames.append(pac.select(
                F.lit(None).cast("string").alias("mmsi"),
                F.coalesce(coal("imo", "imo_no"), F.get_json_object(vjson, "$.imo")).alias("imo"),
                F.coalesce(coal("vslName", "ship", "vessel_name"), F.get_json_object(vjson, "$.name")).alias("name"),
                coal("callSign").alias("call_sign"), F.lit("port_agent_calls").alias("src"),
            ))
        cs = load("carrier_schedules")
        if cs is not None:
            frames.append(cs.select(
                F.lit(None).cast("string").alias("mmsi"), F.col("imo").cast("string").alias("imo"),
                F.col("vessel_name").alias("name"), F.lit(None).cast("string").alias("call_sign"),
                F.lit("carrier_schedules").alias("src"),
            ))
        if not frames:
            return []
        df = frames[0]
        for f in frames[1:]:
            df = df.unionByName(f)
        return [r.asDict() for r in df.dropna(how="all", subset=["mmsi", "imo", "name"]).dropDuplicates().collect()]

    def resolve_vessels(self, decisions: dict[str, dict[str, Any]]) -> dict[str, Any]:
        clusters: dict[str, dict[str, Any]] = {}
        no_imo: list[dict[str, Any]] = []
        for r in self._vessel_identities():
            imo = clean_imo(r.get("imo"))
            name_n = normalize_vessel_name(r.get("name"))
            mmsi = str(r.get("mmsi") or "").strip()
            if imo:
                cl = clusters.setdefault(imo, {
                    "master_vessel_id": f"VES-{imo}", "imo": imo,
                    "mmsis": set(), "names": set(), "call_signs": set(), "sources": set(),
                })
                if mmsi:
                    cl["mmsis"].add(mmsi)
                if name_n:
                    cl["names"].add(name_n)
                if r.get("call_sign"):
                    cl["call_signs"].add(str(r["call_sign"]).strip())
                cl["sources"].add(r["src"])
            elif name_n or mmsi:
                no_imo.append({**r, "name_n": name_n, "mmsi": mmsi})

        mmsi_index = {m: k for k, cl in clusters.items() for m in cl["mmsis"]}
        name_index = {n: k for k, cl in clusters.items() for n in cl["names"]}
        names = list(name_index.keys())
        gaps: list[dict[str, Any]] = []
        seen_gap: set[str] = set()

        def attach(key: str, r: dict[str, Any]) -> None:
            cl = clusters[key]
            if r.get("mmsi"):
                cl["mmsis"].add(r["mmsi"])
            if r.get("name_n"):
                cl["names"].add(r["name_n"])
            cl["sources"].add(r["src"])

        for r in no_imo:
            if r["mmsi"] and r["mmsi"] in mmsi_index:
                attach(mmsi_index[r["mmsi"]], r)
                continue
            best = process.extractOne(r["name_n"], names, scorer=fuzz.token_sort_ratio) if r["name_n"] and names else None
            score = best[1] if best else 0
            key = name_index[best[0]] if best else None
            gap_id = f"vessel:{r['mmsi']}:{r['name_n']}"
            decision = decisions.get(gap_id)
            if decision:
                if decision["decision"] in {"approve", "edit"}:
                    target = decision.get("resolved_value") or decision.get("suggestion")
                    tkey = next((k for k, cl in clusters.items() if cl["master_vessel_id"] == target), key)
                    if tkey:
                        attach(tkey, r)
                        continue
                key = None
            elif key and score >= self.LINK_AT:
                attach(key, r)
                continue
            elif key and score >= self.REVIEW_AT:
                if gap_id not in seen_gap:
                    seen_gap.add(gap_id)
                    gaps.append({
                        "id": gap_id, "kind": "entity", "entity": "vessel", "status": "open",
                        "table": r["src"], "field": "vessel identity",
                        "detail": f"'{r.get('name')}' (MMSI {r['mmsi'] or '—'}, no IMO) looks like "
                                  f"{clusters[key]['master_vessel_id']} {sorted(clusters[key]['names'])[:3]}",
                        "suggestion": clusters[key]["master_vessel_id"], "score": round(score / 100, 2),
                    })
                attach(key, r)
                continue
            vid = _sid("VES-N", r["mmsi"] or r["name_n"])
            clusters.setdefault(vid, {
                "master_vessel_id": vid, "imo": None, "mmsis": set(), "names": set(),
                "call_signs": set(), "sources": set(),
            })
            attach(vid, r)

        masters, aliases = [], []
        for cl in clusters.values():
            masters.append({
                "master_vessel_id": cl["master_vessel_id"], "imo": cl["imo"],
                "primary_name": sorted(cl["names"])[0] if cl["names"] else None,
                "name_count": len(cl["names"]), "mmsi_count": len(cl["mmsis"]),
                "mmsi_list": ",".join(sorted(cl["mmsis"])), "call_signs": ",".join(sorted(cl["call_signs"])),
                "source_count": len(cl["sources"]), "sources": ",".join(sorted(cl["sources"])),
            })
            for n in cl["names"]:
                aliases.append({"master_vessel_id": cl["master_vessel_id"], "alias_type": "name", "alias": n})
            for m in cl["mmsis"]:
                aliases.append({"master_vessel_id": cl["master_vessel_id"], "alias_type": "mmsi", "alias": m})

        schema = ("master_vessel_id STRING, imo STRING, primary_name STRING, name_count INT, mmsi_count INT, "
                  "mmsi_list STRING, call_signs STRING, source_count INT, sources STRING")
        self.gateway.lakehouse.write_delta(self.spark.createDataFrame(masters, schema), "silver.master_vessel", agent="entity_resolution")
        self.gateway.lakehouse.write_delta(
            self.spark.createDataFrame(aliases, "master_vessel_id STRING, alias_type STRING, alias STRING"),
            "silver.vessel_aliases", agent="entity_resolution",
        )
        return {"count": len(masters), "gaps": gaps}

    # --- ports ------------------------------------------------------------------
    def resolve_ports(self) -> dict[str, Any]:
        resolver = PortResolver(self.store.load_mappings().get("port"))
        names = resolver.port_names()
        aliases = {(a, c, "reference") for a, c in resolver.name_index.items()}
        aliases |= {(a, c, "human") for a, c in resolver.overrides.items() if c}
        for _, code, _ in aliases:
            names.setdefault(code, code)
        ports = [(c, n, c[:2]) for c, n in sorted(names.items())]
        self.gateway.lakehouse.write_delta(
            self.spark.createDataFrame(ports, "master_port STRING, port_name STRING, country STRING"),
            "silver.master_port", agent="entity_resolution",
        )
        self.gateway.lakehouse.write_delta(
            self.spark.createDataFrame(sorted(aliases), "alias STRING, master_port STRING, alias_source STRING"),
            "silver.port_aliases", agent="entity_resolution",
        )
        return {"count": len(ports), "aliases": len(aliases)}

    # --- customers --------------------------------------------------------------
    def resolve_customers(self, decisions: dict[str, dict[str, Any]]) -> dict[str, Any]:
        path = BRONZE / "customers"
        if not path.exists():
            return {"count": 0, "gaps": []}
        crm = self.spark.read.format("delta").load(str(path))
        cols = {c.lower(): c for c in crm.columns}
        id_col = cols.get("cust_id") or crm.columns[0]
        name_col = cols.get("account_name") or next((c for c in crm.columns if "name" in c.lower()), crm.columns[1])
        tax_col = next((c for c in crm.columns if "tax" in c.lower()), None)
        del_col = next((c for c in crm.columns if "deleted" in c.lower()), None)
        df = crm
        if del_col:
            df = df.where(F.coalesce(F.col(del_col), F.lit("false")).isin("false", "False", "0", "N", "n"))
        rows = [r.asDict() for r in df.select(id_col, name_col, *([tax_col] if tax_col else [])).collect()]

        masters: dict[str, dict[str, Any]] = {}
        mapping: list[dict[str, Any]] = []
        gaps: list[dict[str, Any]] = []

        def add(mid: str, rec: dict[str, Any], tax: str | None, review: bool = False) -> None:
            m = masters.setdefault(mid, {"master_customer_id": mid, "legal_name": rec.get(name_col),
                                         "tax_id_hash": self._hash(tax) if tax else None, "crm_record_count": 0})
            m["crm_record_count"] += 1
            mapping.append({"crm_record_id": str(rec.get(id_col)), "master_customer_id": mid,
                            "account_name": rec.get(name_col), "needs_review": review})

        no_tax = []
        for r in rows:
            tax = str(r.get(tax_col) or "").strip() if tax_col else ""
            if tax and tax.lower() not in {"null", "none", "nan", "-", "n/a"}:
                add(_sid("CUST", tax), r, tax)
            else:
                no_tax.append(r)

        for r in no_tax:
            name = str(r.get(name_col) or "").upper()
            index = {(m["legal_name"] or "").upper(): mid for mid, m in masters.items()}
            best = process.extractOne(name, list(index.keys()), scorer=fuzz.token_sort_ratio) if index and name else None
            score = best[1] if best else 0
            target = index[best[0]] if best else None
            gap_id = f"customer:{r.get(id_col)}"
            decision = decisions.get(gap_id)
            if decision:
                if decision["decision"] in {"approve", "edit"}:
                    tid = decision.get("resolved_value") or decision.get("suggestion")
                    if tid in masters:
                        add(tid, r, None)
                        continue
                add(_sid("CUST-N", r.get(id_col)), r, None)
            elif target and score >= self.LINK_AT:
                add(target, r, None)
            elif target and score >= 80:
                gaps.append({
                    "id": gap_id, "kind": "entity", "entity": "customer", "status": "open",
                    "table": "customers", "field": "customer identity",
                    "detail": f"CRM '{r.get(name_col)}' (no tax id) may be '{masters[target]['legal_name']}'",
                    "suggestion": target, "score": round(score / 100, 2),
                })
                add(target, r, None, review=True)
            else:
                add(_sid("CUST-N", r.get(id_col)), r, None)

        mschema = "master_customer_id STRING, legal_name STRING, tax_id_hash STRING, crm_record_count INT"
        self.gateway.lakehouse.write_delta(self.spark.createDataFrame(list(masters.values()), mschema),
                                           "silver.master_customer", agent="entity_resolution")
        self.gateway.lakehouse.write_delta(
            self.spark.createDataFrame(mapping, "crm_record_id STRING, master_customer_id STRING, account_name STRING, needs_review BOOLEAN"),
            "silver.customer_record_map", agent="entity_resolution",
        )
        return {"count": len(masters), "mapped": len(mapping), "gaps": gaps}

    @staticmethod
    def _hash(v: str) -> str:
        from maritimeiq.governance.guard import GUARD

        return GUARD.hash_pii(v)
