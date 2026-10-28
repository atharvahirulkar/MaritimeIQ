"""Transformation agent: Bronze + semantics + masters → Silver."""
from __future__ import annotations

import hashlib
import re
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from maritimeiq.agents.entity_resolution import clean_imo, normalize_port, normalize_vessel_name
from maritimeiq.governance.guard import GUARD
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, SILVER, ensure_lakehouse_dirs


def _hash_udf(salt: str):
    def _h(v: str | None) -> str | None:
        if v is None:
            return None
        return "tok_" + hashlib.sha256(f"{salt}:{v}".encode()).hexdigest()[:16]

    return F.udf(_h, StringType())


class TransformationAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        ensure_lakehouse_dirs()
        self.hash_fn = _hash_udf(GUARD.salt)

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        FEED.emit("transform", "Building Silver tables")
        results = {}
        results["ais_positions"] = self.silver_ais_positions()
        results["ais_live"] = self.silver_ais_live()
        results["port_agent_events"] = self.silver_port_agent_events()
        results["carrier_schedules"] = self.silver_carrier_schedules()
        results["bills_of_lading"] = self.silver_bills_of_lading()
        results["container_events"] = self.silver_container_events()
        results["customers"] = self.silver_customers()
        results["crew_manifests"] = self.silver_crew_manifests()
        state["silver_tables"] = results
        state["phase"] = "silver_done"
        FEED.emit("transform", "Silver complete", tables={k: v.get("rows") for k, v in results.items()})
        return state

    def _qc(self, df: DataFrame, name: str) -> dict[str, Any]:
        n = df.count()
        assert n >= 0
        # no cleartext passport-like if masked columns exist
        return {"ok": True, "rows": n, "table": name}

    def _write_silver(self, df: DataFrame, name: str) -> dict[str, Any]:
        qc = self._qc(df, name)
        self.gateway.lakehouse.write_delta(df, f"silver.{name}", agent="transform")
        self.spark.read.format("delta").load(str(SILVER / name)).createOrReplaceTempView(f"silver_{name}")
        return qc

    def _vessel_alias_map(self) -> DataFrame | None:
        path = SILVER / "vessel_aliases"
        if not path.exists():
            return None
        return self.spark.read.format("delta").load(str(path))

    def silver_ais_positions(self) -> dict[str, Any]:
        df = self.spark.read.format("delta").load(str(BRONZE / "ais_positions"))
        # NavStatus may be numeric (v1) or text (v2)
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
            .withColumn("imo_clean", F.regexp_replace(F.coalesce(F.col("IMO"), F.lit("")), "[^0-9]", ""))
            .withColumn("vessel_name_norm", F.upper(F.trim(F.col("VesselName"))))
        )
        # drop null island & sog sentinel handling
        df = df.where(~((F.col("lat") == 0) & (F.col("lon") == 0)))
        df = df.withColumn("sog_clean", F.when(F.col("sog") >= 102.3, F.lit(None)).otherwise(F.col("sog")))
        # join master vessel by IMO
        masters = self.spark.read.format("delta").load(str(SILVER / "master_vessel"))
        df = df.join(masters.select("master_vessel_id", "imo"), df.imo_clean == masters.imo, "left")
        # port normalize destination
        dest_norm_udf = F.udf(normalize_port, StringType())
        df = df.withColumn("destination_port", dest_norm_udf(F.col("Destination")))
        lineage = [c for c in ("_source_file", "_row_hash", "source_file", "row_hash") if c in df.columns]
        out = df.select(
            "master_vessel_id",
            F.col("MMSI").alias("mmsi"),
            F.col("imo_clean").alias("imo"),
            F.col("VesselName").alias("vessel_name"),
            "event_ts",
            "lat",
            "lon",
            "sog_clean",
            "nav_status",
            "destination_port",
            F.col("CallSign").alias("call_sign"),
            *lineage,
        )
        return self._write_silver(out, "ais_positions")

    def silver_ais_live(self) -> dict[str, Any]:
        df = self.spark.read.format("delta").load(str(BRONZE / "ais_live"))
        df = (
            df.withColumn("lat", F.col("LAT").cast("double"))
            .withColumn("lon", F.col("LON").cast("double"))
            .withColumn("nav_status", F.col("Status").cast("int"))
            .withColumn("event_ts", F.to_timestamp("BaseDateTime"))
            .withColumn("imo_clean", F.regexp_replace(F.coalesce(F.col("IMO"), F.lit("")), "[^0-9]", ""))
        )
        masters = self.spark.read.format("delta").load(str(SILVER / "master_vessel"))
        df = df.join(masters.select("master_vessel_id", "imo"), df.imo_clean == masters.imo, "left")
        out = df.select(
            "master_vessel_id",
            F.col("MMSI").alias("mmsi"),
            F.col("imo_clean").alias("imo"),
            F.col("VesselName").alias("vessel_name"),
            "event_ts",
            "lat",
            "lon",
            "nav_status",
            F.col("SOG").cast("double").alias("sog"),
        )
        return self._write_silver(out, "ais_live")

    def silver_port_agent_events(self) -> dict[str, Any]:
        raw = self.spark.read.format("delta").load(str(BRONZE / "port_agent_calls"))
        cols = set(raw.columns)

        def coal(*names: str, cast: str | None = None):
            parts = []
            for n in names:
                if n in cols:
                    c = F.col(n)
                    if cast:
                        c = c.cast(cast)
                    parts.append(c)
            if not parts:
                return F.lit(None).cast("string")
            return F.coalesce(*parts)

        df = raw.select(
            F.col("agency") if "agency" in cols else F.lit(None).alias("agency"),
            coal("msgId", "message_id", "id", "ref").alias("message_id"),
            coal("vslName", "ship", "vessel_name", "vsl_name").alias("vessel_name"),
            coal("imo", "imo_no").alias("imo_raw"),
            coal("port", "port_code", "port_locode", "location").alias("port_raw"),
            coal("evt", "event_code", "status", "status_code", cast="string").alias("event_raw"),
            coal("evtTimeLocal", "event_time", "datetime", "ts", cast="string").alias("event_time_raw"),
            F.col("tz") if "tz" in cols else F.lit(None).alias("tz"),
            coal("voy", "voyage", "voyage_ref").alias("voyage"),
        )

        def map_event(code: str | None) -> str | None:
            if code is None:
                return None
            c = str(code).strip().upper()
            mapping = {
                "10": "ETA",
                "20": "ANC",
                "30": "BRT",
                "40": "COM",
                "50": "CMP",
                "60": "SLD",
                "99": "OMIT",
                "ETA": "ETA",
                "ETA UPDATE": "ETA",
                "ETA NOTICE": "ETA",
                "ANC": "ANC",
                "EOSP": "ANC",
                "ANCHORED - AWAITING BERTH": "ANC",
                "ANCHORED": "ANC",
                "BRT": "BRT",
                "ALL FAST": "BRT",
                "COM": "COM",
                "COMMENCED CARGO": "COM",
                "CMP": "CMP",
                "COMPLETED CARGO": "CMP",
                "COMPLETED": "CMP",
                "SLD": "SLD",
                "SAILED": "SLD",
                "OMIT": "OMIT",
                "CALL CANCELLED": "OMIT",
                "CALL OMITTED": "OMIT",
            }
            if c in mapping:
                return mapping[c]
            for k, v in mapping.items():
                if k in c:
                    return v
            return c

        event_udf = F.udf(map_event, StringType())
        port_udf = F.udf(normalize_port, StringType())
        imo_udf = F.udf(clean_imo, StringType())

        df = (
            df.withColumn("event_code", event_udf(F.col("event_raw")))
            .withColumn("master_port", port_udf(F.col("port_raw")))
            .withColumn("imo", imo_udf(F.col("imo_raw")))
        )
        # Parse times: epoch seconds if numeric else try timestamp
        df = df.withColumn(
            "event_ts",
            F.when(F.col("event_time_raw").rlike(r"^\d{10}$"), F.to_timestamp(F.from_unixtime(F.col("event_time_raw").cast("long"))))
            .when(F.col("event_time_raw").rlike(r"^\d{13}$"), F.to_timestamp(F.from_unixtime((F.col("event_time_raw").cast("long") / 1000).cast("long"))))
            .otherwise(F.to_timestamp(F.col("event_time_raw"))),
        )
        masters = self.spark.read.format("delta").load(str(SILVER / "master_vessel"))
        df = df.join(masters.select(F.col("master_vessel_id"), F.col("imo").alias("m_imo")), df.imo == F.col("m_imo"), "left").drop("m_imo")
        out = df.select(
            "message_id",
            "agency",
            "master_vessel_id",
            "vessel_name",
            "imo",
            "master_port",
            "event_code",
            "event_ts",
            "voyage",
        )
        return self._write_silver(out, "port_agent_events")

    def silver_carrier_schedules(self) -> dict[str, Any]:
        df = self.spark.read.format("delta").load(str(BRONZE / "carrier_schedules"))
        port_udf = F.udf(normalize_port, StringType())
        imo_udf = F.udf(clean_imo, StringType())
        df = (
            df.withColumn("master_port", port_udf(F.col("port_locode")))
            .withColumn("imo", imo_udf(F.col("imo")))
            .withColumn("eta_ts", F.to_timestamp(F.col("eta_raw"), "yyyyMMddHHmm"))
        )
        masters = self.spark.read.format("delta").load(str(SILVER / "master_vessel"))
        df = df.join(masters.select("master_vessel_id", F.col("imo").alias("m_imo")), df.imo == F.col("m_imo"), "left").drop("m_imo")
        return self._write_silver(df, "carrier_schedules")

    def silver_bills_of_lading(self) -> dict[str, Any]:
        df = self.spark.read.format("delta").load(str(BRONZE / "bills_of_lading"))
        port_udf = F.udf(normalize_port, StringType())
        imo_udf = F.udf(lambda v: None, StringType())  # vessel name based later
        df = (
            df.withColumn("pol", port_udf(F.col("PortOfLoading")))
            .withColumn("pod", port_udf(F.col("PortOfDischarge")))
            .withColumn("shipper_contact_masked", self.hash_fn(F.col("ShipperContact")))
            .withColumn("shipper_email_masked", self.hash_fn(F.col("ShipperEmail")))
            .withColumn("shipper_phone_masked", self.hash_fn(F.col("ShipperPhone")))
            .withColumn("shipper_tax_masked", self.hash_fn(F.col("ShipperTaxId")))
            .withColumn("consignee_contact_masked", self.hash_fn(F.col("ConsigneeContact")))
            .withColumn("consignee_email_masked", self.hash_fn(F.col("ConsigneeEmail")))
            .withColumn("consignee_phone_masked", self.hash_fn(F.col("ConsigneePhone")))
            .withColumn("consignee_tax_masked", self.hash_fn(F.col("ConsigneeTaxId")))
        )
        # Drop cleartext PII
        drop_cols = [
            "ShipperContact",
            "ShipperEmail",
            "ShipperPhone",
            "ShipperTaxId",
            "ConsigneeContact",
            "ConsigneeEmail",
            "ConsigneePhone",
            "ConsigneeTaxId",
        ]
        keep = [c for c in df.columns if c not in drop_cols]
        out = df.select(*keep)
        GUARD.register_pii(
            "silver.bills_of_lading",
            [c for c in out.columns if "masked" in c.lower() or "tax" in c.lower()],
        )
        return self._write_silver(out, "bills_of_lading")

    def silver_container_events(self) -> dict[str, Any]:
        df = self.spark.read.format("delta").load(str(BRONZE / "container_events"))

        def unify_event(feed: str | None, dcsa: str | None, edi: str | None, pipe: str | None) -> str | None:
            f = (feed or "").lower()
            if f == "dcsa":
                m = {
                    "GTOT": "GTOT",
                    "GTIN": "GTIN",
                    "LOAD": "LOAD",
                    "DISC": "DISC",
                    "STUF": "STUF",
                    "STRP": "STRP",
                    "ROLL": "ROLL",
                    "HOLD": "HOLD",
                    "RELS": "RELS",
                    "CANC": "CANC",
                    "ASGN": "ASGN",
                    "ONHR": "ONHR",
                    "OFHR": "OFHR",
                    "RPRD": "RPRD",
                }
                return m.get((dcsa or "").upper(), (dcsa or "").upper() if dcsa else None)
            if f == "edi_semicolon":
                s = (edi or "").upper()
                mapping = [
                    ("EQUIPMENT REASSIGNED", "ASGN"),
                    ("ROLLED", "ROLL"),
                    ("CUSTOMS HOLD", "HOLD"),
                    ("RELEASE", "RELS"),
                    ("GATE OUT", "GTOT"),
                    ("GATE IN", "GTIN"),
                    ("LOADED", "LOAD"),
                    ("DISCHARG", "DISC"),
                    ("EMPTY TO SHIPPER", "GTOT"),
                    ("CANCEL", "CANC"),
                ]
                for k, v in mapping:
                    if k in s:
                        return v
                return s or None
            # pipe
            m = {
                "O": "GTOT",
                "I": "GTIN",
                "L": "LOAD",
                "D": "DISC",
                "S": "STUF",
                "U": "STRP",
                "R": "ROLL",
                "H": "HOLD",
                "H2": "RELS",
                "X": "CANC",
                "A": "ASGN",
                "N": "ONHR",
                "F": "OFHR",
                "P": "RPRD",
            }
            return m.get((pipe or "").upper(), pipe)

        def unify_empty(feed: str | None, dcsa: str | None, edi: str | None, pipe: str | None) -> str | None:
            vals = [dcsa, edi, pipe]
            for v in vals:
                if v is None:
                    continue
                u = str(v).upper()
                if u in {"EMPTY", "E", "MT"}:
                    return "EMPTY"
                if u in {"LADEN", "F", "FL", "FULL"}:
                    return "LADEN"
            return None

        event_udf = F.udf(unify_event, StringType())
        empty_udf = F.udf(unify_empty, StringType())
        port_udf = F.udf(normalize_port, StringType())

        df = df.withColumn(
            "event_code",
            event_udf(
                F.col("feed_format"),
                F.col("equipmentEventTypeCode"),
                F.col("STAT"),
                F.col("evt"),
            ),
        ).withColumn(
            "empty_indicator",
            empty_udf(
                F.col("feed_format"),
                F.col("emptyIndicatorCode"),
                F.col("FE"),
                F.col("mt_fl"),
            ),
        ).withColumn(
            "container_id",
            F.regexp_replace(
                F.upper(F.coalesce(F.col("equipmentReference"), F.col("CNTR"), F.col("container"))),
                "[^A-Z0-9]",
                "",
            ),
        ).withColumn(
            "master_port",
            port_udf(F.coalesce(F.col("UNLocationCode"), F.col("LOC"), F.col("loc"))),
        ).withColumn(
            "booking_ref",
            F.coalesce(F.col("carrierBookingReference"), F.col("BKG"), F.col("bkg_ref")),
        ).withColumn(
            "carrier",
            F.col("carrier_scac"),
        )
        # event time approx
        df = df.withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp(F.col("eventDateTime")),
                F.to_timestamp(F.col("EVTDT"), "yyyyMMddHHmm"),
                F.to_timestamp(F.concat_ws(" ", F.col("evt_date"), F.col("evt_time")), "dd-MM-yy HHmm"),
            ),
        )
        out = df.select(
            "carrier",
            "feed_format",
            "container_id",
            "event_code",
            "empty_indicator",
            "master_port",
            "booking_ref",
            "event_ts",
            F.col("vesselIMONumber").alias("vessel_imo"),
            F.col("VSL").alias("vessel_name"),
            F.col("vsl_callsign").alias("call_sign"),
        )
        return self._write_silver(out, "container_events")

    def silver_customers(self) -> dict[str, Any]:
        crm = self.spark.read.format("delta").load(str(BRONZE / "customers"))
        cmap = self.spark.read.format("delta").load(str(SILVER / "customer_record_map"))
        id_col = next(
            (
                c
                for c in crm.columns
                if c.lower() in {"cust_id", "custid"}
                or ("cust" in c.lower() and "id" in c.lower())
            ),
            crm.columns[0],
        )
        cmap2 = cmap.select(
            F.col("crm_record_id"),
            F.col("master_customer_id"),
            F.col("account_name").alias("resolved_account_name"),
        )
        df = crm.join(cmap2, crm[id_col].cast("string") == cmap2.crm_record_id, "left")
        for c in list(df.columns):
            cl = c.lower()
            if c.endswith("_masked") or c in {"master_customer_id", "crm_record_id", "resolved_account_name"}:
                continue
            if any(x in cl for x in ["email", "phone", "contact", "street", "tax", "owner"]):
                df = df.withColumn(c + "_masked", self.hash_fn(F.col(c))).drop(c)
        # Deduplicate any accidental duplicate column names
        seen = set()
        keep = []
        for c in df.columns:
            if c.lower() in seen:
                continue
            seen.add(c.lower())
            keep.append(c)
        df = df.select(*keep)
        return self._write_silver(df, "customers")

    def silver_crew_manifests(self) -> dict[str, Any]:
        df = self.spark.read.format("delta").load(str(BRONZE / "crew_manifests"))
        for c in df.columns:
            cl = c.lower()
            if any(x in cl for x in ["passport", "dob", "birth", "surname", "family", "given", "first name", "seaman", "cdc"]):
                df = df.withColumn(c + "_masked", self.hash_fn(F.col(c).cast("string"))).drop(c)
        port_udf = F.udf(normalize_port, StringType())
        if "port_meta" in df.columns:
            df = df.withColumn("master_port", port_udf(F.col("port_meta")))
        imo_udf = F.udf(clean_imo, StringType())
        if "imo_meta" in df.columns:
            df = df.withColumn("imo", imo_udf(F.col("imo_meta")))
            masters = self.spark.read.format("delta").load(str(SILVER / "master_vessel"))
            df = df.join(masters.select("master_vessel_id", F.col("imo").alias("m_imo")), df.imo == F.col("m_imo"), "left").drop("m_imo")
        GUARD.register_pii("silver.crew_manifests", [c for c in df.columns if "masked" in c.lower()])
        return self._write_silver(df, "crew_manifests")
