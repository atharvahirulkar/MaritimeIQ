"""Entity resolution for vessels, ports, customers — with a HITL queue for uncertain matches."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from rapidfuzz import fuzz, process

from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, CONTEXT, SILVER, ensure_lakehouse_dirs
from maritimeiq.utils.ports import PortResolver, build_resolver, clean_port_text, save_human_alias

ER_REVIEW_PATH = CONTEXT / "er_review.json"
ER_DECISIONS_PATH = CONTEXT / "er_decisions.json"

# Name variants within this similarity are typos of one name; below it, a different name (rename)
SAME_NAME_SCORE = 80
# A name / MMSI must be seen this often to count as real rather than AIS noise
MIN_NAME_SHARE = 0.08


def normalize_vessel_name(name: str | None) -> str:
    if not name:
        return ""
    s = str(name).upper().strip()
    s = re.sub(r"^(M/?V|MV|MT|SS)\.?\s+", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_company_name(name: str | None) -> str:
    if not name:
        return ""
    s = str(name).upper()
    s = re.sub(r"[.,()']", " ", s)
    s = re.sub(r"&", " AND ", s)
    replacements = {
        r"\bLIMITED\b": "LTD",
        r"\bCOMPANY\b": "CO",
        r"\bCORPORATION\b": "CORP",
        r"\bINCORPORATED\b": "INC",
        r"\bS\s?D\s?N\s?B\s?H\s?D\b": "SDN BHD",
        r"\bP\s?T\s?E\b": "PTE",
    }
    for pat, rep in replacements.items():
        s = re.sub(pat, rep, s)
    return re.sub(r"\s+", " ", s).strip()


LEGAL_SUFFIXES = {
    "LTD", "CO", "CORP", "INC", "LLC", "PLC", "GMBH", "AG", "BV", "NV", "SA", "SAS", "SRL", "SPA",
    "SDN", "BHD", "PTE", "PT", "TBK", "FZE", "FZCO", "LLP", "KK", "AB", "AS", "OY", "LIMITED", "PVT",
    "INDUSTRIAL", "TRADING", "INTERNATIONAL", "INTL", "HOLDINGS", "GROUP", "AND",
}


def company_core_name(name: str | None) -> str:
    """Normalized name without legal-form / generic suffix tokens."""
    tokens = normalize_company_name(name).split()
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def normalize_port(text: str | None) -> str:
    """Back-compat helper: cleaned text (use PortResolver for LOCODE resolution)."""
    return clean_port_text(text)


def clean_imo(val: str | None) -> str | None:
    if val is None:
        return None
    s = re.sub(r"[^0-9]", "", str(val))
    return s if len(s) == 7 else None


def _sid(*parts: Any) -> str:
    return hashlib.sha1("|".join("" if p is None else str(p) for p in parts).encode()).hexdigest()[:10]


def load_er_decisions() -> dict[str, dict[str, Any]]:
    if not ER_DECISIONS_PATH.exists():
        return {}
    try:
        return json.loads(ER_DECISIONS_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def load_er_review() -> list[dict[str, Any]]:
    if not ER_REVIEW_PATH.exists():
        return []
    try:
        return json.loads(ER_REVIEW_PATH.read_text(encoding="utf-8")).get("items", [])
    except Exception:  # noqa: BLE001
        return []


def open_er_items() -> list[dict[str, Any]]:
    decisions = load_er_decisions()
    return [i for i in load_er_review() if i["id"] not in decisions]


def record_er_decision(item_id: str, action: str, target: str | None = None, approver: str = "human") -> None:
    """action: link | separate (vessel/customer) · map | not_port (port)."""
    decisions = load_er_decisions()
    decisions[item_id] = {"action": action, "target": target, "approver": approver}
    ER_DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    ER_DECISIONS_PATH.write_text(json.dumps(decisions, indent=2), encoding="utf-8")
    if item_id.startswith("port:"):
        save_human_alias(item_id.split(":", 1)[1], target if action == "map" else None)


def _cluster_names(counts: dict[str, int]) -> list[dict[str, Any]]:
    """Group typo variants; each cluster's label is its most frequent spelling."""
    clusters: list[dict[str, Any]] = []
    for name, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        for cl in clusters:
            if fuzz.ratio(name, cl["label"]) >= SAME_NAME_SCORE:
                cl["n"] += n
                cl["variants"].append(name)
                break
        else:
            clusters.append({"label": name, "n": n, "variants": [name]})
    return clusters


class EntityResolutionAgent:
    def __init__(self, spark: SparkSession, gateway: MCPGateway) -> None:
        self.spark = spark
        self.gateway = gateway
        ensure_lakehouse_dirs()

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        FEED.emit("entity_resolution", "Resolving vessels, ports, customers")
        self.decisions = load_er_decisions()
        self.review: list[dict[str, Any]] = []
        ports = self.resolve_ports()
        vessels = self.resolve_vessels()
        customers = self.resolve_customers()
        ER_REVIEW_PATH.write_text(json.dumps({"items": self.review}, indent=2, default=str), encoding="utf-8")
        open_n = len([i for i in self.review if i["id"] not in self.decisions])
        state["masters"] = {"vessels": vessels, "ports": ports, "customers": customers, "open_review": open_n}
        state["phase"] = "er_done"
        if open_n:
            FEED.emit(
                "entity_resolution",
                f"HITL waiting on {open_n} uncertain entity matches",
                event_type="hitl_waiting",
                count=open_n,
            )
        FEED.emit(
            "entity_resolution",
            f"masters vessels={vessels.get('count')} ports={ports.get('count')} "
            f"customers={customers.get('count')} open_review={open_n}",
            event_type="er_complete",
        )
        return state

    def _queue(self, item: dict[str, Any]) -> None:
        dec = self.decisions.get(item["id"])
        item["status"] = "resolved" if dec else "open"
        item["decision"] = dec
        self.review.append(item)
        if not dec:
            self.gateway.review.create_review_task("entity_match", item, agent="entity_resolution")

    # ------------------------------------------------------------------ ports
    def resolve_ports(self) -> dict[str, Any]:
        resolver = build_resolver(self.spark)
        self.resolver = resolver
        raw_counts = self._raw_port_strings()
        unresolved = 0
        for raw, n in sorted(raw_counts.items(), key=lambda kv: -kv[1]):
            code, how = resolver.resolve(raw)
            if how != "unresolved":
                continue
            unresolved += 1
            self._queue(
                {
                    "id": f"port:{clean_port_text(raw)}",
                    "kind": "port",
                    "left": {"raw": raw, "rows": n},
                    "candidates": resolver.candidates(raw, 3),
                    "score": (resolver.candidates(raw, 1) or [{"score": 0}])[0]["score"],
                    "question": f"Which port is '{raw}'? ({n} source rows)",
                }
            )

        rows = []
        for code in sorted(resolver.locodes):
            rows.append(
                {
                    "master_port": code,
                    "port_name": resolver.port_names.get(code, code),
                    "region": resolver.regions.get(code),
                    "country": code[:2],
                }
            )
        alias_rows = [{"alias": a, "master_port": c, "source": "reference"} for a, c in resolver.names.items()]
        alias_rows += [{"alias": a, "master_port": c, "source": "human"} for a, c in resolver.human_aliases.items() if c]
        if not rows:
            return {"count": 0, "unresolved": unresolved}
        pdf = self.spark.createDataFrame(rows)
        self.gateway.lakehouse.write_delta(pdf, "silver.master_port", agent="entity_resolution")
        if alias_rows:
            self.gateway.lakehouse.write_delta(
                self.spark.createDataFrame(alias_rows), "silver.port_aliases", agent="entity_resolution"
            )
        return {"count": len(rows), "unresolved": unresolved}

    def _raw_port_strings(self) -> dict[str, int]:
        sources = {
            "port_agent_calls": ["port", "port_code", "port_locode", "location"],
            "container_events": ["UNLocationCode", "LOC", "loc"],
            "bills_of_lading": ["PortOfLoading", "PortOfDischarge"],
            "carrier_schedules": ["port_locode"],
            "ais_positions": ["Destination"],
            "crew_manifests": ["port_meta"],
        }
        counts: dict[str, int] = {}
        for table, cols in sources.items():
            path = BRONZE / table
            if not path.exists():
                continue
            df = self.spark.read.format("delta").load(str(path))
            present = [c for c in cols if c in df.columns]
            for c in present:
                for r in df.groupBy(F.col(c).alias("v")).count().collect():
                    if r.v is None:
                        continue
                    key = clean_port_text(r.v)
                    if key:
                        counts[r.v] = counts.get(r.v, 0) + r["count"]
        # Collapse case variants onto one representative raw string
        merged: dict[str, int] = {}
        rep: dict[str, str] = {}
        for raw, n in counts.items():
            k = clean_port_text(raw)
            rep.setdefault(k, raw)
            merged[rep[k]] = merged.get(rep[k], 0) + n
        return merged

    # ---------------------------------------------------------------- vessels
    def resolve_vessels(self) -> dict[str, Any]:
        ais = self.spark.read.format("delta").load(str(BRONZE / "ais_positions"))
        idents = (
            ais.groupBy(
                F.col("MMSI").cast("string").alias("mmsi"),
                F.col("IMO").alias("imo"),
                F.col("VesselName").alias("name"),
                F.col("CallSign").alias("call_sign"),
            )
            .agg(F.count(F.lit(1)).alias("n"), F.min("BaseDateTime").alias("first_seen"))
            .collect()
        )
        clusters: dict[str, dict[str, Any]] = {}
        no_imo: list[dict[str, Any]] = []
        for r in idents:
            imo = clean_imo(r["imo"])
            row = {
                "mmsi": (r["mmsi"] or "").strip(),
                "name_n": normalize_vessel_name(r["name"]),
                "call_sign": (r["call_sign"] or "").strip(),
                "n": int(r["n"]),
                "first_seen": r["first_seen"],
            }
            if not imo:
                no_imo.append(row)
                continue
            cl = clusters.setdefault(
                f"imo:{imo}",
                {"master_vessel_id": f"VES-{imo}", "imo": imo, "names": {}, "mmsis": {}, "call_signs": {}, "first": {}},
            )
            self._absorb(cl, row)

        # Attach no-IMO observations via MMSI (exact) or name (fuzzy, HITL when uncertain)
        mmsi_index = {m: k for k, cl in clusters.items() for m in cl["mmsis"]}
        name_index = {n: k for k, cl in clusters.items() for n in cl["names"]}
        for r in no_imo:
            key = mmsi_index.get(r["mmsi"]) if r["mmsi"] else None
            score = 100 if key else 0
            if key is None and r["name_n"] and name_index:
                hit = process.extractOne(r["name_n"], list(name_index), scorer=fuzz.ratio)
                if hit:
                    key, score = name_index[hit[0]], hit[1]
            item_id = f"vessel:{_sid(r['mmsi'], r['name_n'])}"
            if key and score >= 90:
                self._absorb(clusters[key], r)
                continue
            if key and score >= 75:
                self._queue(
                    {
                        "id": item_id,
                        "kind": "vessel",
                        "left": {"mmsi": r["mmsi"], "name": r["name_n"], "positions": r["n"]},
                        "candidates": [
                            {
                                "master_vessel_id": clusters[key]["master_vessel_id"],
                                "imo": clusters[key]["imo"],
                                "names": sorted(clusters[key]["names"])[:5],
                            }
                        ],
                        "score": score,
                        "question": f"Is AIS vessel '{r['name_n']}' (MMSI {r['mmsi']}) the same ship as "
                        f"{clusters[key]['master_vessel_id']}?",
                    }
                )
                dec = self.decisions.get(item_id)
                if dec and dec.get("action") == "link":
                    self._absorb(clusters[key], r)
                    continue
            # Conservative default: unmatched / unconfirmed → its own master
            vid = f"VES-N{_sid(r['mmsi'], r['name_n'])[:8]}"
            cl = clusters.setdefault(
                f"tmp:{vid}",
                {"master_vessel_id": vid, "imo": None, "names": {}, "mmsis": {}, "call_signs": {}, "first": {}},
            )
            self._absorb(cl, r)

        master_rows, alias_rows = [], []
        renamed = mmsi_changed = 0
        for cl in clusters.values():
            total = sum(cl["names"].values()) or 1
            name_clusters = [c for c in _cluster_names(cl["names"]) if c["n"] / total >= MIN_NAME_SHARE]
            name_clusters.sort(key=lambda c: min(cl["first"].get(v, "9") for v in c["variants"]))
            real_mmsis = [m for m, n in cl["mmsis"].items() if n / max(1, sum(cl["mmsis"].values())) >= MIN_NAME_SHARE]
            is_renamed = len(name_clusters) > 1
            renamed += int(is_renamed)
            mmsi_changed += int(is_renamed and len(real_mmsis) > 1)
            current = name_clusters[-1]["label"] if name_clusters else (max(cl["names"], key=cl["names"].get) if cl["names"] else None)
            master_rows.append(
                {
                    "master_vessel_id": cl["master_vessel_id"],
                    "imo": cl["imo"],
                    "primary_name": current,
                    "former_names": ",".join(c["label"] for c in name_clusters[:-1]) or None,
                    "name_count": len(name_clusters),
                    "renamed": is_renamed,
                    "mmsi_list": ",".join(sorted(real_mmsis or cl["mmsis"])),
                    "mmsi_count": len(real_mmsis),
                    "call_signs": ",".join(sorted(cl["call_signs"])),
                    "position_count": total,
                }
            )
            for n in cl["names"]:
                alias_rows.append({"master_vessel_id": cl["master_vessel_id"], "alias_type": "name", "alias": n, "imo": cl["imo"]})
            for m in cl["mmsis"]:
                alias_rows.append({"master_vessel_id": cl["master_vessel_id"], "alias_type": "mmsi", "alias": m, "imo": cl["imo"]})
            for c in cl["call_signs"]:
                alias_rows.append({"master_vessel_id": cl["master_vessel_id"], "alias_type": "call_sign", "alias": c, "imo": cl["imo"]})

        alias_rows += self._imo_named_aliases(clusters)
        alias_rows += self._review_source_vessel_names(clusters)

        mdf = self.spark.createDataFrame(
            master_rows,
            "master_vessel_id string, imo string, primary_name string, former_names string, name_count int, "
            "renamed boolean, mmsi_list string, mmsi_count int, call_signs string, position_count long",
        )
        self.gateway.lakehouse.write_delta(mdf, "silver.master_vessel", agent="entity_resolution")
        adf = self.spark.createDataFrame(
            alias_rows, "master_vessel_id string, alias_type string, alias string, imo string"
        )
        self.gateway.lakehouse.write_delta(adf, "silver.vessel_aliases", agent="entity_resolution")
        return {"count": len(master_rows), "renamed": renamed, "renamed_and_mmsi_changed": mmsi_changed}

    def _imo_named_aliases(self, clusters: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """Other sources pair a vessel name with an IMO (schedules, Harborline, crew lists).

        Those names become aliases of the IMO master (they do not count toward rename detection),
        so name-only sources like Bluewater can still be linked.
        """
        by_imo = {cl["imo"]: cl for cl in clusters.values() if cl.get("imo")}
        sources = {
            "carrier_schedules": [("imo", "vessel_name")],
            "port_agent_calls": [("imo", "vslName"), ("imo_no", "ship")],
            "crew_manifests": [("imo_meta", "vessel_name_meta")],
        }
        rows: list[dict[str, Any]] = []
        for table, pairs in sources.items():
            path = BRONZE / table
            if not path.exists():
                continue
            df = self.spark.read.format("delta").load(str(path))
            for imo_col, name_col in pairs:
                if imo_col not in df.columns or name_col not in df.columns:
                    continue
                for r in df.select(F.col(imo_col).cast("string").alias("i"), F.col(name_col).alias("n")).distinct().collect():
                    cl = by_imo.get(clean_imo(r.i) or "")
                    name = normalize_vessel_name(r.n)
                    if cl and name and name not in cl["names"]:
                        cl["names_extra"] = cl.get("names_extra", set()) | {name}
            if "vessel" in df.columns:  # Harborline v2: {"imo":..., "name":...}
                for r in df.select(
                    F.get_json_object("vessel", "$.imo").alias("i"), F.get_json_object("vessel", "$.name").alias("n")
                ).distinct().collect():
                    cl = by_imo.get(clean_imo(r.i) or "")
                    name = normalize_vessel_name(r.n)
                    if cl and name and name not in cl["names"]:
                        cl["names_extra"] = cl.get("names_extra", set()) | {name}
        for cl in by_imo.values():
            for n in sorted(cl.get("names_extra", set())):
                rows.append({"master_vessel_id": cl["master_vessel_id"], "alias_type": "name", "alias": n, "imo": cl["imo"]})
                cl["names"].setdefault(n, 0)
        return rows

    def _review_source_vessel_names(self, clusters: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """Vessel names typed in other sources that do not confidently match an AIS master.

        Returns alias rows for names a human already linked.
        """
        name_index = {n: cl["master_vessel_id"] for cl in clusters.values() for n in cl["names"]}
        by_id = {cl["master_vessel_id"]: cl for cl in clusters.values()}
        sources = {
            "port_agent_calls": ["vslName", "ship", "vessel_name"],
            "bills_of_lading": ["Vessel"],
            "container_events": ["VSL"],
        }
        counts: dict[str, int] = {}
        for table, cols in sources.items():
            path = BRONZE / table
            if not path.exists():
                continue
            df = self.spark.read.format("delta").load(str(path))
            for c in [c for c in cols if c in df.columns]:
                for r in df.groupBy(F.col(c).alias("v")).count().collect():
                    n = normalize_vessel_name(r.v)
                    if n:
                        counts[n] = counts.get(n, 0) + r["count"]
        linked: list[dict[str, Any]] = []
        keys = list(name_index)
        for name, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            if name in name_index or n < 5 or not keys:
                continue
            best = process.extractOne(name, keys, scorer=fuzz.ratio)
            if best and best[1] >= 90:
                continue  # Transform links these automatically
            subset = process.extractOne(name, keys, scorer=fuzz.token_set_ratio)
            cand = subset if subset and subset[1] >= 90 else best
            if not cand or cand[1] < 75:
                continue
            mid = name_index[cand[0]]
            item_id = f"vessel_name:{_sid(name)}"
            self._queue(
                {
                    "id": item_id,
                    "kind": "vessel_name",
                    "left": {"name": name, "rows": n},
                    "candidates": [{"master_vessel_id": mid, "imo": by_id[mid]["imo"], "names": [cand[0]]}],
                    "score": round(cand[1]),
                    "question": f"Is '{name}' ({n} source rows) the vessel {cand[0]} ({mid})?",
                }
            )
            dec = self.decisions.get(item_id)
            if dec and dec.get("action") == "link":
                linked.append({"master_vessel_id": dec.get("target") or mid, "alias_type": "name", "alias": name, "imo": by_id[mid]["imo"]})
        return linked

    @staticmethod
    def _absorb(cl: dict[str, Any], r: dict[str, Any]) -> None:
        if r.get("name_n"):
            cl["names"][r["name_n"]] = cl["names"].get(r["name_n"], 0) + r["n"]
            fs = r.get("first_seen") or "9"
            cl["first"][r["name_n"]] = min(cl["first"].get(r["name_n"], "9"), fs)
        if r.get("mmsi"):
            cl["mmsis"][r["mmsi"]] = cl["mmsis"].get(r["mmsi"], 0) + r["n"]
        if r.get("call_sign"):
            cl["call_signs"][r["call_sign"]] = cl["call_signs"].get(r["call_sign"], 0) + r["n"]

    # -------------------------------------------------------------- customers
    def resolve_customers(self) -> dict[str, Any]:
        crm = self.spark.read.format("delta").load(str(BRONZE / "customers"))
        cols = {c.lower(): c for c in crm.columns}
        id_col = cols.get("cust_id") or crm.columns[0]
        name_col = cols.get("account_name") or next((c for c in crm.columns if "name" in c.lower()), crm.columns[1])
        tax_col = next((c for c in crm.columns if "tax" in c.lower()), None)
        del_col = next((c for c in crm.columns if "deleted" in c.lower()), None)
        type_col = cols.get("acct_type")
        df = crm
        if del_col:
            df = df.where(F.lower(F.coalesce(F.col(del_col), F.lit("false"))).isin("false", "0", "n", "no"))
        sel = [id_col, name_col] + ([tax_col] if tax_col else []) + ([type_col] if type_col else [])
        rows = [r.asDict() for r in df.select(*sel).collect()]

        def tax_of(r: dict[str, Any]) -> str:
            t = re.sub(r"\s", "", str(r.get(tax_col) or "")) if tax_col else ""
            # Placeholders (TBA, n/a, -, null…) are not identities: a real tax id carries digits
            return t.upper() if len(re.findall(r"\d", t)) >= 4 else ""

        groups: dict[str, list[dict[str, Any]]] = {}
        no_tax: list[dict[str, Any]] = []
        for r in rows:
            (groups.setdefault(f"tax:{tax_of(r)}", []) if tax_of(r) else no_tax).append(r)

        # Group no-tax records by normalized name, then attach to tax groups when confident
        masters: dict[str, dict[str, Any]] = {}
        for key, recs in groups.items():
            masters[key] = {"recs": recs, "tax": key.split(":", 1)[1]}
        norm_index = {}
        for key, m in masters.items():
            for r in m["recs"]:
                norm_index.setdefault(normalize_company_name(r.get(name_col)), key)
        for r in no_tax:
            nn = normalize_company_name(r.get(name_col))
            key, score = norm_index.get(nn), 100
            if key is None and norm_index:
                # Fuzzy on core names: shared "TRADING CO LTD" tails must not make two companies look alike
                core_index = {company_core_name(k): v for k, v in norm_index.items() if company_core_name(k)}
                core = company_core_name(nn)
                if core in core_index:
                    key, score = core_index[core], 96
                else:
                    hit = process.extractOne(core or nn, list(core_index), scorer=fuzz.token_sort_ratio)
                    key, score = (core_index[hit[0]], hit[1]) if hit else (None, 0)
            item_id = f"customer:{r.get(id_col)}"
            if key and score >= 92:
                masters[key]["recs"].append(r)
                continue
            if key and score >= 80:
                sample = masters[key]["recs"][0]
                self._queue(
                    {
                        "id": item_id,
                        "kind": "customer",
                        "left": {"crm_record_id": str(r.get(id_col)), "name": r.get(name_col)},
                        "candidates": [{"group": key, "name": sample.get(name_col)}],
                        "score": score,
                        "question": f"Is CRM account '{r.get(name_col)}' the same company as '{sample.get(name_col)}'?",
                    }
                )
                dec = self.decisions.get(item_id)
                if dec and dec.get("action") == "link":
                    masters[key]["recs"].append(r)
                    continue
            own = f"name:{nn}"
            masters.setdefault(own, {"recs": [], "tax": None})["recs"].append(r)
            norm_index.setdefault(nn, own)

        master_rows, mapping = [], []
        for key, m in masters.items():
            mid = f"CUST-{_sid(key)[:8]}"
            names = [str(x.get(name_col)) for x in m["recs"] if x.get(name_col)]
            # Canonical name: most common normalized form, shown with its most common raw spelling
            by_norm: dict[str, list[str]] = {}
            for n in names:
                by_norm.setdefault(normalize_company_name(n), []).append(n)
            best_norm = max(by_norm, key=lambda k: len(by_norm[k])) if by_norm else None
            legal = max(set(by_norm[best_norm]), key=by_norm[best_norm].count) if best_norm else None
            types = {str(x.get(type_col)) for x in m["recs"] if type_col and x.get(type_col)}
            master_rows.append(
                {
                    "master_customer_id": mid,
                    "legal_name": legal,
                    "legal_name_norm": best_norm,
                    "has_tax_id": bool(m["tax"]),
                    "account_types": ",".join(sorted(types)) or None,
                    "crm_record_count": len(m["recs"]),
                }
            )
            for x in m["recs"]:
                mapping.append(
                    {
                        "crm_record_id": str(x.get(id_col)),
                        "master_customer_id": mid,
                        "account_name": str(x.get(name_col)) if x.get(name_col) else None,
                        "account_name_norm": normalize_company_name(x.get(name_col)),
                    }
                )

        mdf = self.spark.createDataFrame(
            master_rows,
            "master_customer_id string, legal_name string, legal_name_norm string, has_tax_id boolean, "
            "account_types string, crm_record_count int",
        )
        mapdf = self.spark.createDataFrame(
            mapping, "crm_record_id string, master_customer_id string, account_name string, account_name_norm string"
        )
        self.gateway.lakehouse.write_delta(mdf, "silver.master_customer", agent="entity_resolution")
        self.gateway.lakehouse.write_delta(mapdf, "silver.customer_record_map", agent="entity_resolution")
        return {"count": len(master_rows), "mapped": len(mapping)}


def port_resolver_for(spark) -> PortResolver:
    return build_resolver(spark)
