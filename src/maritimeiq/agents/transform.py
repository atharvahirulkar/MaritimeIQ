"""Transformation agent: Bronze + semantic layer + HITL mappings + masters → Silver.

Every table records what was cleaned (duplicates, unparsed timestamps, unresolved keys,
masked PII) into ``cleanup_report.json``; Silver column semantics and cross-table
relationships are written back into the semantic layer for Gold and Ask.
"""
from __future__ import annotations

import hashlib
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from rapidfuzz import fuzz, process

from maritimeiq.agents.vocab import (
    CONTAINER_EVENTS,
    PORT_EVENTS,
    SILVER_COLUMN_DOCS,
    PortResolver,
    map_container_event,
    map_port_event,
    normalize_vessel_name,
)
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, SILVER, ensure_lakehouse_dirs

CLEANUP_REPORT = "cleanup_report.json"
PII_NAME_HINTS = ["passport", "dob", "birth", "surname", "family", "given", "first_name", "seaman",
                  "cdc", "email", "phone", "contact", "street", "tax", "owner"]

TABLE_DOCS = {
    "ais_positions": "Vessel AIS position reports (history), linked to master_vessel",
    "ais_live": "Live AIS snapshot for LA/Long Beach",
    "port_agent_events": "Port call milestones (ETA, anchored, berthed, cargo ops, sailed) from port agents",
    "carrier_schedules": "Published carrier sailing schedules (ETA/ETD per port call)",
    "bills_of_lading": "Bill of lading container lines (latest amendment), shipper/consignee linked to customers",
    "container_events": "Unified container milestones from DCSA, EDI, and legacy carrier feeds",
    "customers": "CRM accounts linked to master_customer",
    "crew_manifests": "Crew lists per vessel arrival (personal fields masked)",
    "master_vessel": "One row per resolved vessel (IMO-centric), with name/MMSI counts",
    "vessel_aliases": "Vessel name/MMSI aliases → master_vessel_id",
    "master_port": "One row per UN/LOCODE port",
    "port_aliases": "Port aliases (names, abbreviations, human mappings) → master_port",
    "master_customer": "One row per resolved customer",
    "customer_record_map": "CRM record → master_customer_id",
}

ENTITY_KEYS = {
    "master_vessel_id": ("master_vessel", "master_vessel_id"),
    "master_port": ("master_port", "master_port"),
    "pol": ("master_port", "master_port"),
    "pod": ("master_port", "master_port"),
    "destination_port": ("master_port", "master_port"),
    "master_customer_id": ("master_customer", "master_customer_id"),
    "shipper_customer_id": ("master_customer", "master_customer_id"),
    "consignee_customer_id": ("master_customer", "master_customer_id"),
}


def _imo(c: Column) -> Column:
    s = F.regexp_replace(F.regexp_replace(F.trim(c.cast("string")), r"\.0$", ""), "[^0-9]", "")
    return F.when(F.length(s) == 7, s)


def _vname(c: Column) -> Column:
    s = F.upper(F.trim(c.cast("string")))
    s = F.regexp_replace(s, r"^(M/?V|MT|SS)\s+", "")
    s = F.regexp_replace(s, r"[^A-Z0-9 ]", " ")
    return F.trim(F.regexp_replace(s, r"\s+", " "))


def _ts(*exprs: Column) -> Column:
    return F.coalesce(*exprs)


def _teu(c: Column) -> Column:
    size = F.regexp_extract(c.cast("string"), r"^(\d{2})", 1)
    return F.when(size == "20", F.lit(1)).when(size.isin("40", "45"), F.lit(2))


class TransformationAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        self.store = ContextStore()
        ensure_lakehouse_dirs()
        salt = GUARD.salt
        self.hash_fn = F.udf(
            lambda v: None if v is None else "tok_" + hashlib.sha256(f"{salt}:{v}".encode()).hexdigest()[:16],
            StringType(),
        )

    # --- orchestration ----------------------------------------------------------
    def run(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(state or {})
        FEED.emit("transform", "Building Silver from Bronze + semantics + HITL mappings")
        self.mappings = self.store.load_mappings()
        self.resolver = PortResolver(self.mappings["port"])
        self.report: dict[str, Any] = {}
        self.sources: dict[str, str] = {}
        self._prep: dict[str, dict[str, Any]] = {}
        builders = [
            ("ais_positions", self.silver_ais_positions),
            ("ais_live", self.silver_ais_live),
            ("port_agent_calls", self.silver_port_agent_events),
            ("carrier_schedules", self.silver_carrier_schedules),
            ("bills_of_lading", self.silver_bills_of_lading),
            ("container_events", self.silver_container_events),
            ("customers", self.silver_customers),
            ("crew_manifests", self.silver_crew_manifests),
        ]
        for bronze, fn in builders:
            if (BRONZE / bronze).exists():
                fn()
        self.store.save_json(CLEANUP_REPORT, self.report)
        self._write_silver_semantics()
        state["silver_tables"] = {k: v.get("silver_rows") for k, v in self.report.items()}
        FEED.emit("transform", "Silver complete", event_type="silver_complete", tables=state["silver_tables"])
        return state

    # --- shared helpers ---------------------------------------------------------
    def _bronze(self, name: str, silver: str) -> DataFrame:
        df = self.spark.read.format("delta").load(str(BRONZE / name))
        n = df.count()
        if "_row_hash" in df.columns:
            df = df.dropDuplicates(["_row_hash"])
        kept = df.count()
        self._prep[silver] = {"source": name, "bronze_rows": n, "duplicates_removed": n - kept}
        self.sources[silver] = name
        return df

    def _silver(self, name: str) -> DataFrame | None:
        p = SILVER / name
        return self.spark.read.format("delta").load(str(p)) if p.exists() else None

    @staticmethod
    def _lit_map(mapping: dict[str, str | None]) -> Column | None:
        items = [x for k, v in mapping.items() if v for x in (F.lit(k), F.lit(v))]
        return F.create_map(*items) if items else None

    def _resolve_ports(self, df: DataFrame, src: Column, out: str) -> DataFrame:
        df = df.withColumn("__p", F.upper(F.trim(src.cast("string"))))
        vals = [r[0] for r in df.select("__p").distinct().collect() if r[0]]
        m = self._lit_map({v: self.resolver.resolve(v)[0] for v in vals})
        df = df.withColumn(out, m[F.col("__p")] if m is not None else F.lit(None).cast("string"))
        return df.drop("__p")

    def _map_codes(self, df: DataFrame, raw: Column, out: str, domain: str, feed: str | None = None) -> DataFrame:
        overrides = self.mappings[domain]
        key = F.upper(F.trim(raw.cast("string")))
        if feed:
            key = F.when(key.isNotNull(), F.concat(F.coalesce(F.col(feed), F.lit("")), F.lit("|"), key))
        df = df.withColumn("__k", key)
        mapping = {}
        for r in df.select("__k").distinct().collect():
            k = r[0]
            if not k:
                continue
            f, v = k.split("|", 1) if feed else (None, k)
            mapping[k] = (map_container_event(f, v, overrides) if domain == "container_event"
                          else map_port_event(v, overrides))
        m = self._lit_map(mapping)
        df = df.withColumn(out, m[F.col("__k")] if m is not None else F.lit(None).cast("string"))
        return df.drop("__k")

    def _attach_vessel(self, df: DataFrame, imo: Column | None, name: Column | None) -> DataFrame:
        masters = self._silver("master_vessel")
        df = df.withColumn("imo", _imo(imo) if imo is not None else F.lit(None).cast("string"))
        if masters is None:
            return df.withColumn("master_vessel_id", F.lit(None).cast("string"))
        df = df.join(
            F.broadcast(masters.select("master_vessel_id", F.col("imo").alias("__mimo")).where(F.col("__mimo").isNotNull())),
            F.col("imo") == F.col("__mimo"), "left",
        ).drop("__mimo")
        aliases = self._silver("vessel_aliases")
        if name is not None and aliases is not None:
            unique = (
                aliases.where(F.col("alias_type") == "name")
                .groupBy("alias").agg(F.countDistinct("master_vessel_id").alias("n"), F.first("master_vessel_id").alias("__avid"))
                .where("n = 1").select(F.col("alias").alias("__alias"), "__avid")
            )
            df = (
                df.withColumn("__vn", _vname(name))
                .join(F.broadcast(unique), F.col("__vn") == F.col("__alias"), "left")
                .withColumn("master_vessel_id", F.coalesce("master_vessel_id", "__avid"))
                .drop("__vn", "__alias", "__avid")
            )
        return df

    def _customer_map(self, names: list[str]) -> dict[str, str]:
        crm = self._silver("customer_record_map")
        if crm is None or not names:
            return {}
        accounts = {}
        for r in crm.select("account_name", "master_customer_id").collect():
            if r[0]:
                accounts.setdefault(normalize_vessel_name(r[0]), r[1])
        keys = list(accounts.keys())
        out = {}
        for n in names:
            best = process.extractOne(normalize_vessel_name(n), keys, scorer=fuzz.token_set_ratio)
            if best and best[1] >= 92:
                out[n] = accounts[best[0]]
        return out

    def _mask_pii(self, df: DataFrame, bronze: str, silver: str) -> DataFrame:
        sem = (self.store.load_semantics().get("tables", {}).get(bronze) or {}).get("columns") or []
        tagged = {c["name"] for c in sem if c.get("pii") and (c.get("pii_class") or "direct") != "quasi"}
        cols = [
            c for c in df.columns
            if not c.startswith("_") and not c.endswith("_masked")
            and (c in tagged or any(k in c.lower() for k in PII_NAME_HINTS))
        ]
        for c in cols:
            df = df.withColumn(f"{c}_masked", self.hash_fn(F.col(c).cast("string"))).drop(c)
        self._prep.setdefault(silver, {})["pii_masked"] = cols
        return df

    def _write(self, df: DataFrame, name: str, checks: dict[str, Column] | None = None) -> None:
        checks = checks or {}
        aggs = [F.count(F.lit(1)).alias("__n")] + [
            F.sum(F.when(expr, 1).otherwise(0)).alias(label) for label, expr in checks.items()
        ]
        stats = df.agg(*aggs).collect()[0].asDict()
        self.gateway.lakehouse.write_delta(df, f"silver.{name}", agent="transform")
        self.report[name] = {
            **self._prep.get(name, {}),
            "silver_rows": stats["__n"],
            "issues_remaining": {k: int(stats[k] or 0) for k in checks},
        }
        FEED.emit("transform", f"silver.{name}: {stats['__n']} rows", table=name)

    # --- tables -----------------------------------------------------------------
    def silver_ais_positions(self) -> None:
        df = self._bronze("ais_positions", "ais_positions")
        df = df.withColumn("lat", F.col("LAT").cast("double")).withColumn("lon", F.col("LON").cast("double"))
        island = df.where((F.col("lat") == 0) & (F.col("lon") == 0)).count()
        df = df.where(~((F.col("lat") == 0) & (F.col("lon") == 0)))
        self._prep["ais_positions"]["null_island_dropped"] = island
        nav = F.lower(F.coalesce(F.col("NavStatus").cast("string"), F.lit("")))
        df = (
            df.withColumn("sog", F.col("SOG").cast("double"))
            .withColumn("sog_clean", F.when(F.col("sog") >= 102.3, F.lit(None)).otherwise(F.col("sog")))
            .withColumn("nav_status", F.when(nav.isin("1", "at anchor"), 1).when(nav.isin("5", "moored"), 5)
                        .when(nav.isin("0", "under way using engine"), 0).otherwise(F.col("NavStatus").cast("int")))
            .withColumn("event_ts", F.to_timestamp("BaseDateTime"))
        )
        df = self._attach_vessel(df, F.col("IMO"), F.col("VesselName"))
        df = self._resolve_ports(df, F.col("Destination"), "destination_port")
        out = df.select(
            "master_vessel_id", F.col("MMSI").alias("mmsi"), "imo", F.col("VesselName").alias("vessel_name"),
            "event_ts", "lat", "lon", "sog_clean", F.col("COG").cast("double").alias("cog"), "nav_status",
            "destination_port", F.col("Destination").alias("destination_raw"), F.col("CallSign").alias("call_sign"),
            F.col("Draft").cast("double").alias("draft_m"), "schema_version", "_row_hash",
        )
        self._write(out, "ais_positions", {
            "unlinked_vessel": F.col("master_vessel_id").isNull(),
            "unresolved_destination": F.col("destination_port").isNull() & F.col("destination_raw").isNotNull(),
            "unparsed_ts": F.col("event_ts").isNull(),
        })

    def silver_ais_live(self) -> None:
        df = self._bronze("ais_live", "ais_live")
        df = self._attach_vessel(df, F.col("IMO"), F.col("VesselName"))
        out = df.select(
            "master_vessel_id", F.col("MMSI").alias("mmsi"), "imo", F.col("VesselName").alias("vessel_name"),
            F.to_timestamp("BaseDateTime").alias("event_ts"), F.col("LAT").cast("double").alias("lat"),
            F.col("LON").cast("double").alias("lon"), F.col("SOG").cast("double").alias("sog"),
            F.col("Status").cast("int").alias("nav_status"),
        )
        self._write(out, "ais_live", {"unlinked_vessel": F.col("master_vessel_id").isNull()})

    def silver_port_agent_events(self) -> None:
        raw = self._bronze("port_agent_calls", "port_agent_events")
        cols = set(raw.columns)

        def coal(*names: str) -> Column:
            parts = [F.col(n).cast("string") for n in names if n in cols]
            return F.coalesce(*parts) if parts else F.lit(None).cast("string")

        def local_ts(c: Column) -> Column:
            return _ts(*(F.to_timestamp(c, f) for f in ("dd/MM/yyyy HH:mm", "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss",
                                                       "dd-MM-yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ss")))

        def epoch(c: Column) -> Column:
            return (F.when(c.rlike(r"^\d{13}$"), F.timestamp_seconds((c.cast("long") / 1000).cast("long")))
                    .when(c.rlike(r"^\d{10}$"), F.timestamp_seconds(c.cast("long"))))

        vjson = F.col("vessel") if "vessel" in cols else F.lit(None).cast("string")
        df = raw.select(
            coal("agency").alias("agency"), coal("msgId", "message_id", "id", "ref").alias("message_id"),
            F.coalesce(coal("vslName", "ship", "vessel_name"), F.get_json_object(vjson, "$.name")).alias("vessel_name"),
            F.coalesce(coal("imo", "imo_no"), F.get_json_object(vjson, "$.imo")).alias("imo_raw"),
            coal("port", "port_code", "port_locode", "location").alias("port_raw"), coal("terminal").alias("terminal"),
            coal("evt", "event_code", "status", "status_code").alias("event_raw"),
            coal("event_time").alias("iso_time"), coal("ts").alias("epoch_time"),
            coal("datetime", "evtTimeLocal").alias("local_time"), coal("tz").alias("tz"),
            coal("eta").alias("eta_iso"), coal("eta_ts").alias("eta_epoch"), coal("eta_local").alias("eta_local"),
            coal("voy", "voyage", "voyage_ref").alias("voyage"), "_row_hash",
        )
        offset = F.coalesce(F.regexp_extract(F.col("tz"), r"([+-]\d{1,2})", 1).cast("int"), F.lit(0))
        df = df.withColumn("__off", offset)

        def to_utc(local: Column) -> Column:
            return F.timestamp_seconds(F.unix_timestamp(local) - F.col("__off") * 3600)

        df = (
            df.withColumn("event_ts", _ts(F.to_timestamp("iso_time"), epoch(F.col("epoch_time")), to_utc(local_ts(F.col("local_time")))))
            .withColumn("eta_reported_ts", _ts(F.to_timestamp("eta_iso"), epoch(F.col("eta_epoch")), to_utc(local_ts(F.col("eta_local")))))
        )
        df = self._map_codes(df, F.col("event_raw"), "event_code", "port_event")
        df = self._resolve_ports(df, F.col("port_raw"), "master_port")
        df = self._attach_vessel(df, F.col("imo_raw"), F.col("vessel_name"))
        out = df.select(
            "message_id", "agency", "master_vessel_id", "vessel_name", "imo", "master_port", "port_raw", "terminal",
            "event_code", "event_raw", "event_ts", "eta_reported_ts", "voyage", "_row_hash",
        )
        self._write(out, "port_agent_events", {
            "unlinked_vessel": F.col("master_vessel_id").isNull(),
            "unresolved_port": F.col("master_port").isNull() & F.col("port_raw").isNotNull(),
            "unmapped_event": F.col("event_code").isNull() & F.col("event_raw").isNotNull(),
            "unparsed_ts": F.col("event_ts").isNull(),
        })

    def silver_carrier_schedules(self) -> None:
        df = self._bronze("carrier_schedules", "carrier_schedules")

        def sched_ts(c: str) -> Column:
            return (F.when(F.col(c).rlike(r"^\d{12}$"), F.to_timestamp(c, "yyyyMMddHHmm"))
                    .when(F.col(c).rlike(r"^\d{10}$"), F.to_timestamp(c, "yyMMddHHmm")))

        df = self._resolve_ports(df, F.col("port_locode"), "master_port")
        df = self._attach_vessel(df, F.col("imo"), F.col("vessel_name"))
        out = df.select(
            F.col("scac").alias("carrier"), "service", "voyage", "vessel_name", "imo", "master_vessel_id", "master_port",
            "terminal", sched_ts("eta_raw").alias("eta_ts"), sched_ts("etd_raw").alias("etd_ts"),
            F.col("status").alias("schedule_status"), "_row_hash",
        )
        self._write(out, "carrier_schedules", {
            "unlinked_vessel": F.col("master_vessel_id").isNull(),
            "unresolved_port": F.col("master_port").isNull(),
            "unparsed_ts": F.col("eta_ts").isNull(),
        })

    def silver_bills_of_lading(self) -> None:
        df = self._bronze("bills_of_lading", "bills_of_lading")
        w = Window.partitionBy("BLNumber", "ContainerNumber").orderBy(F.col("version").cast("int").desc_nulls_last())
        before = df.count()
        df = df.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")
        self._prep["bills_of_lading"]["superseded_amendments_removed"] = before - df.count()
        df = self._resolve_ports(df, F.col("PortOfLoading"), "pol")
        df = self._resolve_ports(df, F.col("PortOfDischarge"), "pod")
        names = [r[0] for r in df.select("ShipperName").union(df.select("ConsigneeName")).distinct().collect() if r[0]]
        cmap = self._lit_map(self._customer_map(names))
        null = F.lit(None).cast("string")
        df = (
            df.withColumn("shipper_customer_id", cmap[F.col("ShipperName")] if cmap is not None else null)
            .withColumn("consignee_customer_id", cmap[F.col("ConsigneeName")] if cmap is not None else null)
            .withColumn("issue_date", _ts(*(F.to_date("IssueDate", f) for f in
                                             ("yyyy-MM-dd", "yyyyMMdd", "dd/MM/yyyy", "MM/dd/yyyy", "dd-MMM-yyyy"))))
            .withColumn("container_id", F.regexp_replace(F.upper(F.col("ContainerNumber")), "[^A-Z0-9]", ""))
            .withColumn("booking_ref", F.col("BookingRef"))
            .withColumn("teu", _teu(F.col("ContainerType")))
            .withColumn("gross_weight_kg", F.col("GrossWeight").cast("double"))
        )
        df = self._attach_vessel(df, None, F.col("Vessel"))
        df = df.drop("PortOfLoading", "PortOfDischarge", "IssueDate", "GrossWeight", "imo",
                     "_source_file", "_ingested_at", "_source_system")
        df = self._mask_pii(df, "bills_of_lading", "bills_of_lading")
        self._write(df, "bills_of_lading", {
            "unresolved_pol": F.col("pol").isNull(),
            "unresolved_pod": F.col("pod").isNull(),
            "unlinked_shipper": F.col("shipper_customer_id").isNull(),
            "unparsed_issue_date": F.col("issue_date").isNull(),
        })

    def silver_container_events(self) -> None:
        df = self._bronze("container_events", "container_events")
        df = self._map_codes(df, F.coalesce(F.col("equipmentEventTypeCode"), F.col("STAT"), F.col("evt")),
                             "event_code", "container_event", feed="feed_format")
        ind = F.upper(F.trim(F.coalesce(F.col("emptyIndicatorCode"), F.col("FE"), F.col("mt_fl"))))
        df = df.withColumn("empty_indicator", F.when(ind.isin("EMPTY", "E", "MT"), "EMPTY")
                           .when(ind.isin("LADEN", "L", "F", "FL", "FULL"), "LADEN"))
        df = self._resolve_ports(df, F.coalesce(F.col("UNLocationCode"), F.col("LOC"), F.col("loc")), "master_port")
        df = (
            df.withColumn("event_ts", _ts(
                F.to_timestamp("eventDateTime"), F.to_timestamp("EVTDT", "yyyyMMddHHmm"),
                F.to_timestamp(F.concat_ws(" ", "evt_date", "evt_time"), "dd-MM-yy HHmm"),
            ))
            .withColumn("container_id", F.regexp_replace(F.upper(F.coalesce("equipmentReference", "CNTR", "container")), "[^A-Z0-9]", ""))
            .withColumn("iso_type", F.coalesce("ISOEquipmentCode", "SZTP"))
        )
        df = self._attach_vessel(df, F.col("vesselIMONumber"), F.col("VSL"))
        out = df.select(
            F.col("carrier_scac").alias("carrier"), "feed_format", "container_id", "event_code",
            F.coalesce("equipmentEventTypeCode", "STAT", "evt").alias("event_raw"), "empty_indicator", "master_port",
            F.coalesce("carrierBookingReference", "BKG", "bkg_ref").alias("booking_ref"), "event_ts",
            "imo", "master_vessel_id", F.col("VSL").alias("vessel_name"),
            F.coalesce("carrierVoyageNumber", "VOY", "voyage").alias("voyage"), "iso_type",
            _teu(F.col("iso_type")).alias("teu"), "_row_hash",
        )
        self._write(out, "container_events", {
            "unmapped_event": F.col("event_code").isNull() & F.col("event_raw").isNotNull(),
            "unresolved_port": F.col("master_port").isNull(),
            "unparsed_ts": F.col("event_ts").isNull(),
        })

    def silver_customers(self) -> None:
        crm = self._bronze("customers", "customers")
        cmap = self._silver("customer_record_map")
        id_col = next((c for c in crm.columns if c.lower() == "cust_id"), crm.columns[0])
        df = crm
        if cmap is not None:
            df = df.join(cmap.select("crm_record_id", "master_customer_id"), F.col(id_col).cast("string") == F.col("crm_record_id"), "left").drop("crm_record_id")
        if "IsDeleted" in df.columns:
            df = df.withColumn("is_deleted", F.lower(F.col("IsDeleted")).isin("true", "1", "y")).drop("IsDeleted")
        if "Crt_Dt" in df.columns:
            df = df.withColumn("created_date", F.to_date("Crt_Dt", "MM/dd/yyyy")).drop("Crt_Dt")
        df = df.drop("_source_file", "_ingested_at", "_source_system")
        df = self._mask_pii(df, "customers", "customers")
        self._write(df, "customers", {"unlinked_customer": F.col("master_customer_id").isNull()})

    def silver_crew_manifests(self) -> None:
        df = self._bronze("crew_manifests", "crew_manifests")
        df = self._resolve_ports(df, F.col("port_meta"), "master_port")
        df = self._attach_vessel(df, F.col("imo_meta"), F.col("vessel_name_meta"))
        rank = [c for c in ("Rank", "Rank_or_rating") if c in df.columns]
        if rank:
            df = df.withColumn("rank", F.coalesce(*[F.col(c) for c in rank])).drop(*[c for c in rank if c != "rank"])
        if "arrival_date_meta" in df.columns:
            df = df.withColumn("arrival_date", _ts(*(F.to_date("arrival_date_meta", f) for f in ("dd.MM.yyyy", "dd/MM/yyyy", "yyyy-MM-dd"))))
        df = df.withColumnRenamed("vessel_name_meta", "vessel_name").withColumnRenamed("port_meta", "port_raw")
        df = df.drop("imo_meta", "arrival_date_meta", "_source_file", "_ingested_at", "_source_system")
        df = self._mask_pii(df, "crew_manifests", "crew_manifests")
        self._write(df, "crew_manifests", {
            "unlinked_vessel": F.col("master_vessel_id").isNull(),
            "unresolved_port": F.col("master_port").isNull() & F.col("port_raw").isNotNull(),
        })

    # --- semantic layer for Silver ---------------------------------------------
    def _write_silver_semantics(self) -> None:
        sem = self.store.load_semantics()
        bronze_cols = {
            t: {str(c.get("name", "")).lower(): c for c in meta.get("columns") or []}
            for t, meta in (sem.get("tables") or {}).items()
        }
        silver: dict[str, Any] = {}
        for p in sorted(SILVER.iterdir()):
            if not p.is_dir():
                continue
            cols = self.spark.read.format("delta").load(str(p)).columns
            src = self.sources.get(p.name)
            docs = {}
            for c in cols:
                if c.startswith("_"):
                    continue
                if c in SILVER_COLUMN_DOCS:
                    docs[c] = SILVER_COLUMN_DOCS[c]
                elif c.endswith("_masked"):
                    docs[c] = f"Salted hash of PII field {c[:-7]} (joinable, not readable)"
                else:
                    docs[c] = (bronze_cols.get(src or "", {}).get(c.lower()) or {}).get("meaning") or ""
            entry: dict[str, Any] = {"source": src, "description": TABLE_DOCS.get(p.name, ""), "columns": docs}
            if "event_code" in cols:
                entry["code_map"] = {"event_code": PORT_EVENTS if p.name == "port_agent_events" else CONTAINER_EVENTS}
            silver[p.name] = entry
        sem["silver_tables"] = silver
        sem["relationships"] = self._relationships(silver)
        self.store.save_semantics(sem)

    @staticmethod
    def _relationships(silver: dict[str, Any]) -> list[dict[str, str]]:
        rels = []
        for t, meta in silver.items():
            for c in meta["columns"]:
                target = ENTITY_KEYS.get(c)
                if target and target[0] in silver and target[0] != t:
                    rels.append({"from": f"{t}.{c}", "to": f"{target[0]}.{target[1]}", "type": "many_to_one"})
        def has(t: str, c: str) -> bool:
            return t in silver and c in silver[t]["columns"]
        for (a, ac), (b, bc), label in [
            (("container_events", "booking_ref"), ("bills_of_lading", "booking_ref"), "booking"),
            (("container_events", "container_id"), ("bills_of_lading", "container_id"), "container"),
            (("port_agent_events", "master_vessel_id"), ("carrier_schedules", "master_vessel_id"), "vessel call (+ master_port)"),
            (("crew_manifests", "master_vessel_id"), ("port_agent_events", "master_vessel_id"), "vessel arrival"),
        ]:
            if has(a, ac) and has(b, bc):
                rels.append({"from": f"{a}.{ac}", "to": f"{b}.{bc}", "type": "many_to_many", "via": label})
        return rels
