"""Entity resolution for vessels, ports, customers."""
from __future__ import annotations

import re
import uuid
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from rapidfuzz import fuzz

from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, CONTEXT, SILVER, SOURCES, ensure_lakehouse_dirs


def normalize_vessel_name(name: str | None) -> str:
    if not name:
        return ""
    s = name.upper().strip()
    s = re.sub(r"^(M/?V|MV|MT|SS)\s+", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_port(text: str | None) -> str:
    if not text:
        return ""
    s = text.upper().strip()
    aliases = {
        "POLB": "USLGB",
        "POLA": "USLAX",
        "LONG BEACH": "USLGB",
        "LOS ANGELES": "USLAX",
        "SAN PEDRO": "USLAX",
        "LA/LB": "USLAX",
        "HKG": "HKHKG",
        "HONGKONG": "HKHKG",
        "HONG KONG": "HKHKG",
        "SINGAPORE": "SGSIN",
        "ROTTERDAM": "NLRTM",
        "ANTWERP": "BEANR",
        "HAMBURG": "DEHAM",
        "BUSAN": "KRPUS",
        "PUSAN": "KRPUS",
        "QINGDAO": "CNTAO",
        "TSINGTAO": "CNTAO",
        "SHANGHAI": "CNSHA",
        "NINGBO": "CNNGB",
        "YANTIAN": "CNYTN",
        "KAOHSIUNG": "TWKHH",
        "PORT KLANG": "MYPKG",
        "TANJUNG PELEPAS": "MYTPP",
        "JEBEL ALI": "AEJEA",
        "DUBAI": "AEJEA",
        "LAEM CHABANG": "THLCH",
        "XIAMEN": "CNXMN",
        "NANSHA": "CNNSA",
        "TIANJIN": "CNTXG",
    }
    if s in aliases:
        return aliases[s]
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{3}", s):
        return s
    for k, v in aliases.items():
        if k in s:
            return v
    return s


def clean_imo(val: str | None) -> str | None:
    if val is None:
        return None
    s = re.sub(r"[^0-9]", "", str(val))
    return s if len(s) == 7 else (s or None)


class EntityResolutionAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        ensure_lakehouse_dirs()

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        FEED.emit("entity_resolution", "Resolving vessels, ports, customers")
        vessels = self.resolve_vessels()
        ports = self.resolve_ports()
        customers = self.resolve_customers()
        state["masters"] = {
            "vessels": vessels,
            "ports": ports,
            "customers": customers,
        }
        state["phase"] = "er_done"
        # uncertain pairs for HITL
        uncertain = vessels.get("uncertain", [])
        for u in uncertain[:20]:
            self.gateway.review.create_review_task("entity_match", u, agent="entity_resolution")
        FEED.emit(
            "entity_resolution",
            f"masters vessels={vessels.get('count')} ports={ports.get('count')} customers={customers.get('count')} uncertain={len(uncertain)}",
        )
        return state

    def resolve_vessels(self) -> dict[str, Any]:
        ais = self.spark.read.format("delta").load(str(BRONZE / "ais_positions"))
        # Collect distinct identity tuples
        idents = (
            ais.select(
                F.col("MMSI").alias("mmsi"),
                F.col("IMO").alias("imo"),
                F.col("VesselName").alias("name"),
                F.col("CallSign").alias("call_sign"),
            )
            .dropna(how="all")
            .dropDuplicates()
        )
        rows = [r.asDict() for r in idents.collect()]
        # Cluster by cleaned IMO first
        clusters: dict[str, dict[str, Any]] = {}
        uncertain: list[dict[str, Any]] = []
        no_imo: list[dict[str, Any]] = []

        for r in rows:
            imo = clean_imo(r.get("imo"))
            name_n = normalize_vessel_name(r.get("name"))
            mmsi = str(r.get("mmsi") or "").strip()
            if imo:
                key = f"imo:{imo}"
                if key not in clusters:
                    clusters[key] = {
                        "master_vessel_id": f"VES-{imo}",
                        "imo": imo,
                        "mmsis": set(),
                        "names": set(),
                        "call_signs": set(),
                    }
                if mmsi:
                    clusters[key]["mmsis"].add(mmsi)
                if name_n:
                    clusters[key]["names"].add(name_n)
                if r.get("call_sign"):
                    clusters[key]["call_signs"].add(str(r["call_sign"]).strip())
            else:
                no_imo.append({**r, "name_n": name_n})

        # Attach no-IMO via fuzzy name / MMSI
        for r in no_imo:
            best_key = None
            best_score = 0
            for key, cl in clusters.items():
                score = 0
                if r.get("mmsi") and str(r["mmsi"]) in cl["mmsis"]:
                    score = 100
                else:
                    for n in cl["names"]:
                        score = max(score, fuzz.token_sort_ratio(r["name_n"], n))
                if score > best_score:
                    best_score = score
                    best_key = key
            if best_key and best_score >= 90:
                cl = clusters[best_key]
                if r.get("mmsi"):
                    cl["mmsis"].add(str(r["mmsi"]))
                if r.get("name_n"):
                    cl["names"].add(r["name_n"])
            elif best_key and 75 <= best_score < 90:
                uncertain.append(
                    {
                        "left": r,
                        "right": {"master_vessel_id": clusters[best_key]["master_vessel_id"], "names": list(clusters[best_key]["names"])},
                        "score": best_score,
                    }
                )
                # still link for MVP continuity; HITL can override
                cl = clusters[best_key]
                if r.get("mmsi"):
                    cl["mmsis"].add(str(r["mmsi"]))
                if r.get("name_n"):
                    cl["names"].add(r["name_n"])
            else:
                vid = f"VES-{uuid.uuid4().hex[:8]}"
                clusters[f"tmp:{vid}"] = {
                    "master_vessel_id": vid,
                    "imo": None,
                    "mmsis": {str(r["mmsi"])} if r.get("mmsi") else set(),
                    "names": {r["name_n"]} if r.get("name_n") else set(),
                    "call_signs": set(),
                }

        master_rows = []
        alias_rows = []
        for cl in clusters.values():
            master_rows.append(
                {
                    "master_vessel_id": cl["master_vessel_id"],
                    "imo": cl["imo"],
                    "primary_name": sorted(cl["names"])[0] if cl["names"] else None,
                    "mmsi_list": ",".join(sorted(cl["mmsis"])),
                    "call_signs": ",".join(sorted(cl["call_signs"])),
                }
            )
            for n in cl["names"]:
                alias_rows.append({"master_vessel_id": cl["master_vessel_id"], "alias_name": n, "imo": cl["imo"]})
            for m in cl["mmsis"]:
                alias_rows.append({"master_vessel_id": cl["master_vessel_id"], "alias_mmsi": m, "imo": cl["imo"]})

        mdf = self.spark.createDataFrame(master_rows)
        self.gateway.lakehouse.write_delta(mdf, "silver.master_vessel", agent="entity_resolution")
        if alias_rows:
            # normalize keys
            keys = sorted({k for r in alias_rows for k in r})
            unified = [{k: r.get(k) for k in keys} for r in alias_rows]
            adf = self.spark.createDataFrame(unified)
            self.gateway.lakehouse.write_delta(adf, "silver.vessel_aliases", agent="entity_resolution")
        mdf.createOrReplaceTempView("master_vessel")
        return {"count": len(master_rows), "uncertain": uncertain}

    def resolve_ports(self) -> dict[str, Any]:
        ref = self.spark.read.option("header", True).csv(str(SOURCES / "reference" / "unlocode_code_list.csv"))
        cols = {c.lower(): c for c in ref.columns}
        name_col = cols.get("name") or cols.get("namewodiacritics") or list(ref.columns)[0]
        # Official UN/LOCODE seed files use Country + Location
        if "country" in cols and "location" in cols:
            ports_df = ref.select(
                F.concat(F.col(cols["country"]), F.col(cols["location"])).alias("master_port"),
                F.col(name_col).alias("name"),
            )
        elif "locode" in cols or "unlocode" in cols:
            code_col = cols.get("locode") or cols.get("unlocode")
            ports_df = ref.select(F.col(code_col).alias("master_port"), F.col(name_col).alias("name"))
        else:
            ports_df = ref.select(F.lit(None).alias("master_port"), F.col(name_col).alias("name"))

        ports = [r.asDict() for r in ports_df.collect()]
        alias_extras = [
            ("POLB", "USLGB"),
            ("POLA", "USLAX"),
            ("LONG BEACH", "USLGB"),
            ("LOS ANGELES", "USLAX"),
            ("SAN PEDRO", "USLAX"),
            ("HONGKONG", "HKHKG"),
            ("HKG", "HKHKG"),
            ("AE JEA", "AEJEA"),
            ("DUBAI", "AEJEA"),
        ]
        rows = []
        for p in ports:
            mp = p.get("master_port")
            if not mp:
                continue
            rows.append({"master_port": mp, "alias": mp, "name": p.get("name")})
            if p.get("name"):
                rows.append({"master_port": mp, "alias": normalize_port(p["name"]), "name": p.get("name")})
                rows.append({"master_port": mp, "alias": str(p["name"]).upper(), "name": p.get("name")})
        for alias, canon in alias_extras:
            rows.append({"master_port": canon, "alias": alias, "name": canon})
        pdf = self.spark.createDataFrame(rows).dropDuplicates(["master_port", "alias"])
        self.gateway.lakehouse.write_delta(pdf, "silver.master_port", agent="entity_resolution")
        pdf.createOrReplaceTempView("master_port")
        return {"count": pdf.select("master_port").distinct().count()}

    def resolve_customers(self) -> dict[str, Any]:
        crm = self.spark.read.format("delta").load(str(BRONZE / "customers"))
        # Identify columns
        cols = {c.lower(): c for c in crm.columns}
        id_col = cols.get("cust_id") or list(crm.columns)[0]
        name_col = next(
            (
                c
                for c in crm.columns
                if c.lower() in {"account_name", "account name"} or "account_name" in c.lower()
            ),
            None,
        )
        if name_col is None:
            name_col = next((c for c in crm.columns if "name" in c.lower()), crm.columns[1])
        tax_col = next((c for c in crm.columns if "tax" in c.lower()), None)
        del_col = next((c for c in crm.columns if "isdeleted" in c.lower() or "deleted" in c.lower()), None)

        df = crm
        if del_col:
            df = df.where(F.coalesce(F.col(del_col), F.lit("false")).isin("false", "False", "0", "N", "n"))

        rows = [r.asDict() for r in df.select(id_col, name_col, *([tax_col] if tax_col else [])).collect()]
        masters: list[dict[str, Any]] = []
        mapping: list[dict[str, Any]] = []
        # Group by tax id then fuzzy name
        by_tax: dict[str, list[dict[str, Any]]] = {}
        no_tax: list[dict[str, Any]] = []
        for r in rows:
            tax = (str(r.get(tax_col)).strip() if tax_col and r.get(tax_col) else "") or ""
            if tax and tax.lower() not in {"null", "none", "nan", "-", "n/a"}:
                by_tax.setdefault(tax, []).append(r)
            else:
                no_tax.append(r)

        def new_master(recs: list[dict[str, Any]], tax: str | None) -> str:
            mid = f"CUST-{uuid.uuid4().hex[:8]}"
            names = [str(x.get(name_col)) for x in recs if x.get(name_col)]
            masters.append(
                {
                    "master_customer_id": mid,
                    "legal_name": names[0] if names else None,
                    "tax_id_hash": tax,
                    "crm_record_count": len(recs),
                }
            )
            for x in recs:
                mapping.append(
                    {
                        "crm_record_id": str(x.get(id_col)),
                        "master_customer_id": mid,
                        "account_name": str(x.get(name_col)) if x.get(name_col) else None,
                    }
                )
            return mid

        for tax, recs in by_tax.items():
            new_master(recs, tax)

        # Fuzzy merge no-tax into existing names
        for r in no_tax:
            name = str(r.get(name_col) or "")
            best = None
            best_score = 0
            for m in masters:
                score = fuzz.token_sort_ratio(name.upper(), (m["legal_name"] or "").upper())
                if score > best_score:
                    best_score = score
                    best = m
            if best and best_score >= 90:
                mapping.append(
                    {
                        "crm_record_id": str(r.get(id_col)),
                        "master_customer_id": best["master_customer_id"],
                        "account_name": name,
                    }
                )
                best["crm_record_count"] += 1
            elif best and 80 <= best_score < 90:
                # uncertain — still link for MVP, create HITL task via caller
                mapping.append(
                    {
                        "crm_record_id": str(r.get(id_col)),
                        "master_customer_id": best["master_customer_id"],
                        "account_name": name,
                        "uncertain": True,
                        "score": best_score,
                    }
                )
            else:
                new_master([r], None)

        mdf = self.spark.createDataFrame(masters) if masters else self.spark.createDataFrame([], "master_customer_id STRING")
        mapdf = self.spark.createDataFrame(mapping) if mapping else self.spark.createDataFrame([], "crm_record_id STRING")
        self.gateway.lakehouse.write_delta(mdf, "silver.master_customer", agent="entity_resolution")
        self.gateway.lakehouse.write_delta(mapdf, "silver.customer_record_map", agent="entity_resolution")
        mdf.createOrReplaceTempView("master_customer")
        return {"count": len(masters), "mapped": len(mapping)}
