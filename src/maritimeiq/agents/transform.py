"""Transformation agent: Bronze + approved semantics + masters → governed Silver."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from rapidfuzz import fuzz, process

from maritimeiq.agents.entity_resolution import (
    clean_imo,
    company_core_name,
    normalize_company_name,
    normalize_vessel_name,
)
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, CONTEXT, SILVER, ensure_lakehouse_dirs
from maritimeiq.utils.ports import build_resolver

SILVER_QC_PATH = CONTEXT / "silver_qc.json"

# Bronze tables with a purpose-built Silver transform; anything else gets the generic path
KNOWN_TABLES = {
    "ais_positions",
    "ais_live",
    "port_agent_calls",
    "carrier_schedules",
    "bills_of_lading",
    "container_events",
    "customers",
    "crew_manifests",
}

PORT_CALL_EVENTS = {
    "10": "ETA", "20": "ANC", "30": "BRT", "40": "COM", "50": "CMP", "60": "SLD", "99": "OMIT",
    "ETA": "ETA", "ETA UPDATE": "ETA", "ETA NOTICE": "ETA",
    "ANC": "ANC", "EOSP": "ANC", "ANCHORED - AWAITING BERTH": "ANC", "ANCHORED": "ANC",
    "BRT": "BRT", "ALL FAST": "BRT", "BERTHED": "BRT",
    "COM": "COM", "COMMENCED CARGO": "COM",
    "CMP": "CMP", "COMPLETED CARGO": "CMP", "COMPLETED": "CMP",
    "SLD": "SLD", "SAILED": "SLD",
    "OMIT": "OMIT", "CALL CANCELLED": "OMIT", "CALL OMITTED": "OMIT",
}

DCSA_EVENTS = {c: c for c in ("GTOT", "GTIN", "LOAD", "DISC", "STUF", "STRP", "ROLL", "HOLD", "RELS", "CANC", "ASGN", "ONHR", "OFHR", "RPRD")}
EDI_EVENTS = {
    "EMPTY TO SHIPPER": "GTOT", "GATE OUT FULL": "GTOT", "GATE OUT": "GTOT",
    "EMPTY RETURNED": "GTIN", "GATE IN FULL": "GTIN", "GATE IN": "GTIN",
    "LOADED ON VESSEL": "LOAD", "EMPTY LOADED": "LOAD", "LOADED": "LOAD",
    "DISCHARGED": "DISC", "EMPTY DISCHARGED": "DISC",
    "STUFFED": "STUF", "STRIPPED": "STRP",
    "ROLLED OVER": "ROLL", "ROLLED": "ROLL",
    "CUSTOMS HOLD": "HOLD", "CUSTOMS RELEASED": "RELS", "RELEASED": "RELS",
    "BOOKING CANCELLED": "CANC", "CANCELLED": "CANC",
    "EQUIPMENT REASSIGNED": "ASGN", "ON-HIRE": "ONHR", "OFF-HIRE": "OFHR", "REPAIRED": "RPRD",
}
PIPE_EVENTS = {
    "O": "GTOT", "I": "GTIN", "L": "LOAD", "D": "DISC", "S": "STUF", "U": "STRP", "R": "ROLL",
    "H": "HOLD", "H2": "RELS", "X": "CANC", "A": "ASGN", "N": "ONHR", "F": "OFHR", "P": "RPRD",
}
EMPTY_FLAGS = {"EMPTY": "EMPTY", "E": "EMPTY", "MT": "EMPTY", "LADEN": "LADEN", "FULL": "LADEN", "F": "LADEN", "FL": "LADEN", "L": "LADEN"}

PII_NAME_HINTS = ("passport", "email", "phone", "tax", "dob", "birth", "contact", "street", "owner",
                  "surname", "family", "given", "first_name", "seaman", "cdc")


def _hash_udf(salt: str):
    def _h(v: str | None) -> str | None:
        if v is None or str(v).strip() == "":
            return None
        return "tok_" + hashlib.sha256(f"{salt}:{v}".encode()).hexdigest()[:16]

    return F.udf(_h, StringType())


def _offset_hours(tz_col):
    """'+08' / '-08' / 'UTC-8' / '+05:30' → signed hours (double)."""
    s = F.regexp_replace(F.upper(F.trim(tz_col)), "^UTC", "")
    sign = F.when(s.startswith("-"), F.lit(-1.0)).otherwise(F.lit(1.0))
    body = F.regexp_replace(s, "^[+-]", "")
    hours = F.split(body, ":").getItem(0).cast("double")
    mins = F.coalesce(F.split(body, ":").getItem(1).cast("double"), F.lit(0.0))
    return sign * (hours + mins / 60.0)


def _local_to_utc(ts_col, offset_col):
    return F.to_timestamp(F.from_unixtime(F.unix_timestamp(ts_col) - (offset_col * 3600).cast("long")))


def _teu(size_col):
    """ISO 6346 size code or trade shorthand → TEU (20' = 1, 40'/45' = 2)."""
    s = F.upper(F.regexp_replace(size_col, "[^0-9A-Za-z]", ""))
    return (
        F.when(s.rlike("^(20|22|2[0-9A-Z])"), F.lit(1))
        .when(s.rlike("^(40|42|45|4[0-9A-Z]|L)"), F.lit(2))
        .otherwise(F.lit(None))
    )


class TransformationAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        ensure_lakehouse_dirs()
        self.hash_fn = _hash_udf(GUARD.salt)
        self.store = ContextStore()

    # ------------------------------------------------------------------ run
    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        FEED.emit("transform", "Building Silver tables from Bronze + approved semantics + masters")
        self.semantics = (self.store.load_semantics() or {}).get("tables", {})
        self.resolver = build_resolver(self.spark)
        self.qc: dict[str, Any] = {}
        builders: dict[str, Callable[[], dict[str, Any]]] = {
            "port_timezones": self.silver_port_timezones,
            "ais_positions": self.silver_ais_positions,
            "ais_live": self.silver_ais_live,
            "port_agent_events": self.silver_port_agent_events,
            "carrier_schedules": self.silver_carrier_schedules,
            "bills_of_lading": self.silver_bills_of_lading,
            "container_events": self.silver_container_events,
            "customers": self.silver_customers,
            "crew_manifests": self.silver_crew_manifests,
        }
        results: dict[str, Any] = {}
        for name, fn in builders.items():
            try:
                results[name] = fn()
            except Exception as exc:  # noqa: BLE001
                results[name] = {"ok": False, "error": str(exc)[:300], "rows": 0}
                self.qc[name] = {"ok": False, "checks": [{"check": "build", "ok": False, "detail": str(exc)[:300]}]}
                FEED.emit("transform", f"Silver {name} failed: {exc}", level="error")
        # New / unknown sources still flow through a semantics-driven generic Silver
        for p in sorted(BRONZE.iterdir()) if BRONZE.exists() else []:
            if p.is_dir() and p.name not in KNOWN_TABLES and not p.name.startswith("ref_"):
                results[p.name] = self.silver_generic(p.name)
        SILVER_QC_PATH.write_text(json.dumps(self.qc, indent=2, default=str), encoding="utf-8")
        state["silver_tables"] = results
        state["silver_qc"] = self.qc
        state["phase"] = "silver_done"
        failed = [k for k, v in self.qc.items() if not v.get("ok")]
        FEED.emit(
            "transform",
            f"Silver complete — {len(results)} tables, QC failures: {failed or 'none'}",
            event_type="silver_complete",
            tables={k: v.get("rows") for k, v in results.items()},
        )
        return state

    # -------------------------------------------------------------- helpers
    def _bronze(self, name: str) -> DataFrame:
        return self.spark.read.format("delta").load(str(BRONZE / name))

    def _silver(self, name: str) -> DataFrame:
        return self.spark.read.format("delta").load(str(SILVER / name))

    def _map_distinct(self, df: DataFrame, src: Any, out: str, fn: Callable[[Any], Any], dtype: str = "string") -> DataFrame:
        """Apply a Python resolver once per distinct value (driver side), then join back."""
        key = f"__k_{out}"
        df = df.withColumn(key, (src if not isinstance(src, str) else F.col(src)).cast("string"))
        values = [r[0] for r in df.select(key).distinct().collect()]
        rows = [(v, fn(v)) for v in values if v is not None]
        if not rows:
            return df.withColumn(out, F.lit(None).cast(dtype)).drop(key)
        mdf = self.spark.createDataFrame(rows, f"{key} string, {out} {dtype}")
        return df.join(F.broadcast(mdf), on=key, how="left").drop(key)

    def _resolve_port(self, df: DataFrame, src: Any, out: str = "master_port") -> DataFrame:
        return self._map_distinct(df, src, out, self.resolver.code)

    def _vessel_lookup(self) -> dict[str, dict[str, str]]:
        if getattr(self, "_vlookup", None) is None:
            aliases = self._silver("vessel_aliases").collect()
            by_type: dict[str, dict[str, set[str]]] = {"name": {}, "mmsi": {}, "call_sign": {}}
            for a in aliases:
                by_type.setdefault(a.alias_type, {}).setdefault(a.alias, set()).add(a.master_vessel_id)
            # Only unambiguous aliases link; a name reused by two ships is not an identity
            self._vlookup = {t: {k: next(iter(v)) for k, v in d.items() if len(v) == 1} for t, d in by_type.items()}
            self._imo_lookup = {r.imo: r.master_vessel_id for r in self._silver("master_vessel").collect() if r.imo}
        return self._vlookup

    def _vessel_by_name(self, name: Any) -> str | None:
        names = self._vessel_lookup()["name"]
        n = normalize_vessel_name(name)
        if not n:
            return None
        if n in names:
            return names[n]
        hit = process.extractOne(n, list(names), scorer=fuzz.ratio)
        return names[hit[0]] if hit and hit[1] >= 90 else None

    def _attach_vessel(self, df: DataFrame, imo_col: str | None, name_col: str | None, call_sign_col: str | None = None) -> DataFrame:
        """master_vessel_id by IMO, then call sign, then (fuzzy) name alias."""
        self._vessel_lookup()
        parts = []
        if imo_col:
            df = self._map_distinct(df, imo_col, "__mv_imo", lambda v: self._imo_lookup.get(clean_imo(v) or ""))
            parts.append(F.col("__mv_imo"))
        if call_sign_col:
            cs = self._vlookup.get("call_sign", {})
            df = self._map_distinct(df, call_sign_col, "__mv_cs", lambda v: cs.get(str(v).strip()))
            parts.append(F.col("__mv_cs"))
        if name_col:
            df = self._map_distinct(df, name_col, "__mv_name", self._vessel_by_name)
            parts.append(F.col("__mv_name"))
        df = df.withColumn("master_vessel_id", F.coalesce(*parts) if parts else F.lit(None).cast("string"))
        return df.drop(*[c for c in ("__mv_imo", "__mv_cs", "__mv_name") if c in df.columns])

    def _port_offsets(self) -> DataFrame:
        return self._silver("port_timezones").select("master_port", F.col("utc_offset_h").alias("__port_off"))

    def _semantic_pii(self, bronze_table: str) -> set[str]:
        cols = (self.semantics.get(bronze_table) or {}).get("columns") or []
        return {c["name"] for c in cols if c.get("pii") and c.get("name")}

    def _semantic_code_map(self, bronze_table: str, columns: set[str]) -> dict[str, str]:
        """Human/LLM-approved code dictionaries extend the built-in event maps."""
        out: dict[str, str] = {}
        for c in (self.semantics.get(bronze_table) or {}).get("columns") or []:
            if c.get("name") in columns and isinstance(c.get("code_map"), dict):
                for k, v in c["code_map"].items():
                    canon = PORT_CALL_EVENTS.get(str(v).strip().upper())
                    if canon:
                        out[str(k).strip().upper()] = canon
        return out

    def _mask_pii(self, df: DataFrame, bronze_table: str) -> tuple[DataFrame, list[str]]:
        """Hash + drop cleartext for every semantic-layer PII column and name-hinted PII column."""
        tagged = {c.lower() for c in self._semantic_pii(bronze_table)}
        masked = []
        for c in list(df.columns):
            cl = c.lower()
            if cl.startswith("_") or cl.endswith("_masked"):
                continue
            if cl in tagged or any(h in cl for h in PII_NAME_HINTS):
                df = df.withColumn(f"{c}_masked", self.hash_fn(F.col(c).cast("string"))).drop(c)
                masked.append(c)
        return df, masked

    def _qc(self, df: DataFrame, name: str, key_cols: list[str], masked: list[str]) -> dict[str, Any]:
        n = df.count()
        checks = [{"check": "rows > 0", "ok": n > 0, "detail": n}]
        if n:
            for k in key_cols:
                if k in df.columns:
                    nulls = df.where(F.col(k).isNull()).count()
                    rate = nulls / n
                    checks.append({"check": f"{k} null rate < 20%", "ok": rate < 0.2, "detail": round(rate, 4)})
            # No cleartext email / passport-like strings may survive into Silver
            str_cols = [c for c, t in df.dtypes if t == "string" and not c.startswith("_") and not c.endswith("_masked")]
            if str_cols:
                pat = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
                cond = None
                for c in str_cols:
                    e = F.col(c).rlike(pat)
                    cond = e if cond is None else (cond | e)
                leaks = df.limit(20000).where(cond).count()
                checks.append({"check": "no cleartext emails", "ok": leaks == 0, "detail": leaks})
        result = {"ok": all(c["ok"] for c in checks), "rows": n, "checks": checks, "masked_columns": masked}
        self.qc[name] = result
        return result

    def _write_silver(self, df: DataFrame, name: str, bronze_table: str | None = None, key_cols: list[str] | None = None) -> dict[str, Any]:
        masked: list[str] = []
        if bronze_table:
            df, masked = self._mask_pii(df, bronze_table)
            GUARD.register_pii(f"silver.{name}", [f"{c}_masked" for c in masked])
        self.gateway.lakehouse.write_delta(df, f"silver.{name}", agent="transform")
        out = self._silver(name)
        out.createOrReplaceTempView(f"silver_{name}")
        qc = self._qc(out, name, key_cols or [], masked)
        return {"ok": qc["ok"], "rows": qc["rows"], "table": name, "masked": masked}

    # -------------------------------------------------------------- tables
    def silver_port_timezones(self) -> dict[str, Any]:
        """Learn each port's UTC offset from sources that state it explicitly."""
        obs = []
        ce = self._bronze("container_events")
        if "tz" in ce.columns:
            obs.append(ce.where(F.col("tz").isNotNull()).select(F.col("loc").alias("raw"), _offset_hours(F.col("tz")).alias("off")))
        if "eventDateTime" in ce.columns:
            obs.append(
                ce.where(F.col("eventDateTime").rlike(r"[+-]\d\d:\d\d$")).select(
                    F.col("UNLocationCode").alias("raw"),
                    _offset_hours(F.regexp_extract("eventDateTime", r"([+-]\d\d:\d\d)$", 1)).alias("off"),
                )
            )
        pa = self._bronze("port_agent_calls")
        if "tz" in pa.columns:
            obs.append(pa.where(F.col("tz").isNotNull()).select(F.col("port").alias("raw"), _offset_hours(F.col("tz")).alias("off")))
        df = obs[0]
        for o in obs[1:]:
            df = df.unionByName(o)
        df = self._resolve_port(df, "raw")
        tz = (
            df.where(F.col("master_port").isNotNull() & F.col("off").isNotNull())
            .groupBy("master_port", "off")
            .count()
        )
        from pyspark.sql.window import Window

        w = Window.partitionBy("master_port").orderBy(F.col("count").desc())
        best = tz.withColumn("rn", F.row_number().over(w)).where("rn = 1").select(
            "master_port", F.col("off").alias("utc_offset_h"), F.col("count").alias("observations")
        )
        return self._write_silver(best, "port_timezones", key_cols=["utc_offset_h"])

    def silver_ais_positions(self) -> dict[str, Any]:
        df = self._bronze("ais_positions")
        nav = F.lower(F.coalesce(F.col("NavStatus").cast("string"), F.lit("")))
        df = (
            df.withColumn("lat", F.col("LAT").cast("double"))
            .withColumn("lon", F.col("LON").cast("double"))
            .withColumn("sog", F.col("SOG").cast("double"))
            .withColumn(
                "nav_status",
                F.when(nav.isin("1", "at anchor", "anchor"), F.lit(1))
                .when(nav.isin("5", "moored", "at berth"), F.lit(5))
                .when(nav.isin("0", "under way using engine", "under way"), F.lit(0))
                .otherwise(F.col("NavStatus").cast("int")),
            )
            .withColumn("event_ts", F.to_timestamp("BaseDateTime"))
            .where(~((F.col("lat") == 0) & (F.col("lon") == 0)))
            .withColumn("sog_clean", F.when(F.col("sog") >= 102.3, F.lit(None)).otherwise(F.col("sog")))
        )
        df = self._attach_vessel(df, "IMO", "VesselName", "CallSign")
        df = self._resolve_port(df, "Destination", "destination_port")
        out = df.select(
            "master_vessel_id",
            F.col("MMSI").cast("string").alias("mmsi"),
            F.regexp_replace(F.coalesce(F.col("IMO"), F.lit("")), "[^0-9]", "").alias("imo"),
            F.col("VesselName").alias("vessel_name"),
            "event_ts", "lat", "lon", "sog_clean", "nav_status", "destination_port",
            F.col("CallSign").alias("call_sign"),
        )
        return self._write_silver(out, "ais_positions", "ais_positions", ["master_vessel_id", "event_ts"])

    def silver_ais_live(self) -> dict[str, Any]:
        df = self._bronze("ais_live")
        df = (
            df.withColumn("lat", F.col("LAT").cast("double"))
            .withColumn("lon", F.col("LON").cast("double"))
            .withColumn("nav_status", F.col("Status").cast("int"))
            .withColumn("event_ts", F.to_timestamp("BaseDateTime"))
        )
        df = self._attach_vessel(df, "IMO", "VesselName")
        out = df.select(
            "master_vessel_id",
            F.col("MMSI").cast("string").alias("mmsi"),
            F.regexp_replace(F.coalesce(F.col("IMO"), F.lit("")), "[^0-9]", "").alias("imo"),
            F.col("VesselName").alias("vessel_name"),
            "event_ts", "lat", "lon", "nav_status",
            F.col("SOG").cast("double").alias("sog"),
        )
        return self._write_silver(out, "ais_live", "ais_live", ["event_ts"])

    def silver_port_agent_events(self) -> dict[str, Any]:
        raw = self._bronze("port_agent_calls")
        cols = set(raw.columns)

        def col(name: str):
            return F.col(name) if name in cols else F.lit(None).cast("string")

        def coal(*names: str):
            parts = [F.col(n).cast("string") for n in names if n in cols]
            return F.coalesce(*parts) if parts else F.lit(None).cast("string")

        vessel_json_name = F.get_json_object(col("vessel"), "$.name")
        vessel_json_imo = F.get_json_object(col("vessel"), "$.imo")
        df = raw.select(
            col("agency").alias("agency"),
            coal("msgId", "message_id", "id", "ref").alias("message_id"),
            F.coalesce(col("vslName"), col("ship"), col("vessel_name"), vessel_json_name, col("vessel")).alias("vessel_name"),
            F.coalesce(col("imo").cast("string"), col("imo_no").cast("string"), vessel_json_imo).alias("imo_raw"),
            coal("port", "port_code", "port_locode", "location").alias("port_raw"),
            coal("evt", "event_code", "status", "status_code").alias("event_raw"),
            col("ts").cast("string").alias("ts_epoch"),
            col("evtTimeLocal").cast("string").alias("ts_local_tz"),
            col("tz").cast("string").alias("tz"),
            col("event_time").cast("string").alias("ts_iso"),
            col("datetime").cast("string").alias("ts_local_dmy"),
            coal("voy", "voyage", "voyage_ref").alias("voyage_raw"),
            coal("terminal").alias("terminal"),
        )
        mapping = {**PORT_CALL_EVENTS, **self._semantic_code_map("port_agent_calls", {"evt", "event_code", "status", "status_code"})}

        def map_event(code: Any) -> str | None:
            if code is None:
                return None
            c = str(code).strip().upper()
            if c in mapping:
                return mapping[c]
            for k, v in mapping.items():
                if len(k) > 3 and k in c:
                    return v
            return None

        df = self._map_distinct(df, "event_raw", "event_code", map_event)
        df = self._resolve_port(df, "port_raw")
        df = self._attach_vessel(df, "imo_raw", "vessel_name")
        df = df.join(F.broadcast(self._port_offsets()), "master_port", "left")
        # Each agency states time differently; normalize all to UTC
        df = df.withColumn(
            "event_ts",
            F.coalesce(
                F.when(F.col("ts_epoch").rlike(r"^\d{10}$"), F.to_timestamp(F.from_unixtime(F.col("ts_epoch").cast("long")))),
                F.when(F.col("ts_epoch").rlike(r"^\d{13}$"), F.to_timestamp(F.from_unixtime((F.col("ts_epoch").cast("long") / 1000).cast("long")))),
                F.when(F.col("ts_iso").isNotNull(), F.to_timestamp(F.col("ts_iso"))),
                F.when(
                    F.col("ts_local_tz").isNotNull(),
                    _local_to_utc(F.to_timestamp("ts_local_tz"), _offset_hours(F.col("tz"))),
                ),
                F.when(
                    F.col("ts_local_dmy").isNotNull(),
                    _local_to_utc(F.to_timestamp("ts_local_dmy", "dd/MM/yyyy HH:mm"), F.coalesce(F.col("__port_off"), F.lit(0.0))),
                ),
            ),
        )
        voyage = F.upper(F.regexp_extract(F.col("voyage_raw"), r"^0*([0-9]+[A-Z]?)", 1))
        out = (
            df.withColumn("voyage", F.when(voyage == "", None).otherwise(voyage))
            .withColumn("imo", F.udf(clean_imo, StringType())(F.col("imo_raw")))
            .withColumn("vessel_name_norm", F.udf(normalize_vessel_name, StringType())(F.col("vessel_name")))
            .where(F.col("event_code").isNotNull())
            .dropDuplicates(["agency", "message_id"])
            .select(
                "message_id", "agency", "master_vessel_id", "vessel_name", "vessel_name_norm", "imo", "master_port", "port_raw",
                "terminal", "event_code", "event_ts", "voyage",
            )
        )
        return self._write_silver(out, "port_agent_events", "port_agent_calls", ["event_ts", "master_port", "master_vessel_id"])

    def silver_carrier_schedules(self) -> dict[str, Any]:
        df = self._bronze("carrier_schedules")
        df = self._resolve_port(df, "port_locode")
        df = self._attach_vessel(df, "imo", "vessel_name")
        df = df.join(F.broadcast(self._port_offsets()), "master_port", "left")
        tz_flag = F.upper(F.coalesce(F.col("schedule_tz") if "schedule_tz" in df.columns else F.lit(None), F.lit("UTC")))

        def parse(c):
            return F.when(F.length(F.col(c)) == 12, F.to_timestamp(F.col(c), "yyyyMMddHHmm")).when(
                F.length(F.col(c)) == 10, F.to_timestamp(F.col(c), "yyMMddHHmm")
            )

        def to_utc(c):
            return F.when(tz_flag == "LOCAL", _local_to_utc(parse(c), F.coalesce(F.col("__port_off"), F.lit(0.0)))).otherwise(parse(c))

        voyage = F.upper(F.regexp_extract(F.col("voyage"), r"^0*([0-9]+[A-Z]?)", 1))
        out = df.select(
            F.col("scac").alias("carrier"),
            "service",
            voyage.alias("voyage"),
            "vessel_name",
            F.udf(clean_imo, StringType())(F.col("imo")).alias("imo"),
            "master_vessel_id",
            "master_port",
            "terminal",
            to_utc("eta_raw").alias("eta_ts"),
            to_utc("etd_raw").alias("etd_ts"),
            F.when(F.col("status") == "X", "OMITTED").when(F.col("status") == "C", "CHANGED").otherwise("PLANNED").alias("status"),
            F.regexp_extract(F.col("_source_file"), r"_(\d{4}Q\d)", 1).alias("publication"),
        )
        return self._write_silver(out, "carrier_schedules", "carrier_schedules", ["eta_ts", "master_port"])

    def _customer_resolver(self) -> Callable[[Any], str | None]:
        cmap = self._silver("customer_record_map").select("account_name_norm", "master_customer_id").distinct().collect()
        by_norm: dict[str, set[str]] = {}
        for r in cmap:
            if r.account_name_norm:
                by_norm.setdefault(r.account_name_norm, set()).add(r.master_customer_id)
        index = {k: next(iter(v)) for k, v in by_norm.items() if len(v) == 1}
        keys = list(index)
        # "Gonzalez Chemicals" vs "Gonzalez Chemicals Co., Ltd." — match on the core name when unique
        core: dict[str, set[str]] = {}
        for k, mids in by_norm.items():
            core.setdefault(company_core_name(k), set()).update(mids)
        core_index = {k: next(iter(v)) for k, v in core.items() if k and len(v) == 1}

        def resolve(name: Any) -> str | None:
            n = normalize_company_name(name)
            if not n:
                return None
            if n in index:
                return index[n]
            if company_core_name(n) in core_index:
                return core_index[company_core_name(n)]
            hit = process.extractOne(n, keys, scorer=fuzz.token_sort_ratio) if keys else None
            return index[hit[0]] if hit and hit[1] >= 90 else None

        return resolve

    def silver_bills_of_lading(self) -> dict[str, Any]:
        df = self._bronze("bills_of_lading")
        df = self._resolve_port(df, "PortOfLoading", "pol")
        df = self._resolve_port(df, "PortOfDischarge", "pod")
        resolve_customer = self._customer_resolver()
        df = self._map_distinct(df, "ShipperName", "shipper_master_id", resolve_customer)
        df = self._map_distinct(df, "ConsigneeName", "consignee_master_id", resolve_customer)
        df = self._attach_vessel(df, None, "Vessel")
        issue = F.coalesce(
            F.to_date("IssueDate", "MM/dd/yyyy"),
            F.to_date("IssueDate", "dd/MM/yyyy"),
            F.to_date("IssueDate", "dd-MMM-yyyy"),
            F.to_date("IssueDate", "yyyy-MM-dd"),
            F.to_date("IssueDate", "yyyyMMdd"),
        )
        df = (
            df.withColumn("issue_date", issue)
            .withColumn("container_id", F.upper(F.regexp_replace(F.col("ContainerNumber"), "[^A-Za-z0-9]", "")))
            .withColumn("teu", _teu(F.col("ContainerType")))
            .withColumn("gross_weight_kg", F.col("GrossWeight").cast("double"))
            .withColumn("bl_version", F.coalesce(F.col("version").cast("int"), F.lit(1)))
            .withColumnRenamed("BLNumber", "bl_number")
            .withColumnRenamed("BookingRef", "booking_ref")
            .withColumnRenamed("Carrier", "carrier")
            .withColumnRenamed("ShipperName", "shipper_name")
            .withColumnRenamed("ConsigneeName", "consignee_name")
            .withColumnRenamed("ContainerType", "container_type_raw")
            .withColumnRenamed("Vessel", "vessel_name")
            .withColumnRenamed("Voyage", "voyage")
        )
        lineage = [c for c in df.columns if c.startswith("_")]
        drop = {"IssueDate", "ContainerNumber", "GrossWeight", "version", "PortOfLoading", "PortOfDischarge", *lineage}
        out = df.select(*[c for c in df.columns if c not in drop], "_source_file", "_row_hash")
        return self._write_silver(out, "bills_of_lading", "bills_of_lading", ["issue_date", "pol", "pod", "teu"])

    def silver_container_events(self) -> dict[str, Any]:
        df = self._bronze("container_events")

        def unify_event(feed: Any, dcsa: Any, edi: Any, pipe: Any) -> str | None:
            f = (feed or "").lower()
            if f == "dcsa":
                return DCSA_EVENTS.get(str(dcsa or "").strip().upper())
            if f == "edi_semicolon":
                return EDI_EVENTS.get(str(edi or "").strip().upper())
            return PIPE_EVENTS.get(str(pipe or "").strip().upper())

        def unify_empty(dcsa: Any, edi: Any, pipe: Any) -> str | None:
            for v in (dcsa, edi, pipe):
                if v is not None and str(v).strip().upper() in EMPTY_FLAGS:
                    return EMPTY_FLAGS[str(v).strip().upper()]
            return None

        df = (
            df.withColumn(
                "event_code",
                F.udf(unify_event, StringType())(F.col("feed_format"), F.col("equipmentEventTypeCode"), F.col("STAT"), F.col("evt")),
            )
            .withColumn("empty_indicator", F.udf(unify_empty, StringType())(F.col("emptyIndicatorCode"), F.col("FE"), F.col("mt_fl")))
            .withColumn(
                "container_id",
                F.regexp_replace(F.upper(F.coalesce(F.col("equipmentReference"), F.col("CNTR"), F.col("container"))), "[^A-Z0-9]", ""),
            )
            .withColumn("port_raw", F.coalesce(F.col("UNLocationCode"), F.col("LOC"), F.col("loc")))
            .withColumn("booking_ref", F.coalesce(F.col("carrierBookingReference"), F.col("BKG"), F.col("bkg_ref") if "bkg_ref" in df.columns else F.lit(None)))
            .withColumn("bl_number", F.col("transportDocumentReference"))
            .withColumn("carrier", F.col("carrier_scac"))
            .withColumn("size_type", F.coalesce(F.col("ISOEquipmentCode"), F.col("SZTP")))
            .where(F.col("event_code").isNotNull() & (F.col("container_id") != ""))
        )
        df = self._resolve_port(df, "port_raw")
        df = df.join(F.broadcast(self._port_offsets()), "master_port", "left")
        regions = self.spark.createDataFrame(list(self.resolver.regions.items()) or [("", "")], "master_port string, region string")
        df = df.join(F.broadcast(regions), "master_port", "left")
        # dcsa: ISO offset · edi: local, no zone (use port offset) · pipe: local + tz column
        df = df.withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp(F.col("eventDateTime")),
                _local_to_utc(F.to_timestamp(F.col("EVTDT"), "yyyyMMddHHmm"), F.coalesce(F.col("__port_off"), F.lit(0.0))),
                _local_to_utc(
                    F.to_timestamp(F.concat_ws(" ", F.col("evt_date"), F.col("evt_time")), "dd-MM-yy HHmm"),
                    F.coalesce(_offset_hours(F.col("tz")), F.col("__port_off"), F.lit(0.0)),
                ),
            ),
        )
        df = self._attach_vessel(df, "vesselIMONumber", "VSL", "vsl_callsign")
        voyage = F.upper(F.regexp_extract(F.coalesce(F.col("carrierVoyageNumber"), F.col("VOY"), F.col("voyage")), r"^0*([0-9]+[A-Z]?)", 1))
        out = df.select(
            "carrier", "feed_format", "container_id", "event_code", "empty_indicator", "master_port", "region",
            "booking_ref", "bl_number", "event_ts", "master_vessel_id",
            F.when(voyage == "", None).otherwise(voyage).alias("voyage"),
            "size_type", _teu(F.col("size_type")).alias("teu"),
            F.col("eventRemarks").alias("remarks"),
        )
        return self._write_silver(out, "container_events", "container_events", ["event_ts", "master_port", "empty_indicator"])

    def silver_customers(self) -> dict[str, Any]:
        crm = self._bronze("customers")
        cmap = self._silver("customer_record_map").select("crm_record_id", "master_customer_id")
        id_col = next((c for c in crm.columns if c.lower() in {"cust_id", "custid"}), crm.columns[0])
        df = crm.join(cmap, crm[id_col].cast("string") == cmap.crm_record_id, "left").drop("crm_record_id")
        if "Country" in df.columns:
            country_map = {"USA": "US", "UNITED STATES": "US", "U S": "US", "US": "US", "U S A": "US",
                           "UK": "GB", "UNITED KINGDOM": "GB", "GREAT BRITAIN": "GB"}

            def norm_country(v: Any) -> str | None:
                if v is None:
                    return None
                k = re.sub(r"[^A-Z ]", " ", str(v).upper())
                k = re.sub(r"\s+", " ", k).strip()
                return country_map.get(k, k)

            df = self._map_distinct(df, "Country", "country_norm", norm_country)
        if "IsDeleted" in df.columns:
            df = df.withColumn("is_deleted", F.lower(F.col("IsDeleted")).isin("true", "1", "y", "yes"))
        return self._write_silver(df, "customers", "customers", ["master_customer_id"])

    def silver_crew_manifests(self) -> dict[str, Any]:
        df = self._bronze("crew_manifests")
        if "port_meta" in df.columns:
            df = self._resolve_port(df, "port_meta")
        df = self._attach_vessel(df, "imo_meta" if "imo_meta" in df.columns else None, "vessel_name_meta" if "vessel_name_meta" in df.columns else None)
        # Unify the per-sheet column variants before masking
        def first(*names: str):
            parts = [F.col(n) for n in names if n in df.columns]
            return F.coalesce(*parts) if parts else F.lit(None).cast("string")

        df = (
            df.withColumn("rank", first("Rank", "Rank_or_rating"))
            .withColumn("nationality", first("Nationality"))
            .withColumn("passport_number", first("Passport", "Passport_No"))
            .withColumn("date_of_birth", first("DOB", "Date_of_birth"))
            .withColumn("family_name", first("Family_Name", "Surname"))
            .withColumn("given_name", first("First_Name", "Given_names"))
            .withColumn("seaman_book_number", first("Seaman_s_book_No", "CDC_No"))
        )
        drop = [c for c in ("Rank", "Rank_or_rating", "Nationality", "Passport", "Passport_No", "DOB", "Date_of_birth",
                            "Family_Name", "Surname", "First_Name", "Given_names", "Seaman_s_book_No", "CDC_No") if c in df.columns]
        out = self._write_silver(df.drop(*drop), "crew_manifests", "crew_manifests", ["master_vessel_id"])
        # Record the source columns behind the unified masked ones (lineage for audits / eval)
        sources = [c for c in drop if any(h in c.lower() for h in PII_NAME_HINTS)]
        self.qc["crew_manifests"]["masked_columns"] = sorted(set(self.qc["crew_manifests"]["masked_columns"]) | set(sources))
        return out

    def silver_generic(self, table: str) -> dict[str, Any]:
        """Semantics-driven Silver for sources without a dedicated transform."""
        df = self._bronze(table)
        for c, t in df.dtypes:
            if t == "string" and not c.startswith("_"):
                df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))
        hints = {c["name"]: (c.get("data_type_hint") or "").lower() for c in (self.semantics.get(table) or {}).get("columns") or []}
        for c, hint in hints.items():
            if c not in df.columns:
                continue
            if any(k in hint for k in ("timestamp", "datetime")):
                df = df.withColumn(c, F.to_timestamp(F.col(c)))
            elif hint in {"int", "integer", "long", "bigint"}:
                df = df.withColumn(c, F.col(c).cast("long"))
            elif hint in {"double", "float", "decimal", "number", "numeric"}:
                df = df.withColumn(c, F.col(c).cast("double"))
        return self._write_silver(df, table, table)
