"""Canonical vocabularies and resolvers shared by Quality, ER, and Transform agents.

Everything here is *reference knowledge* (UN/LOCODE, World Port Index, event code
standards). Human guidance captured in HITL is layered on top via ``overrides``.
"""
from __future__ import annotations

import csv
import re
from functools import lru_cache
from typing import Any

from rapidfuzz import fuzz, process

from maritimeiq.utils.paths import SOURCES

LOCODE_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}$")

PORT_EVENTS: dict[str, str] = {
    "ETA": "ETA notice / update",
    "ANC": "Anchored, awaiting berth (end of sea passage)",
    "BRT": "Berthed / all fast",
    "COM": "Cargo operations commenced",
    "CMP": "Cargo operations completed",
    "SLD": "Sailed / departed berth",
    "OMIT": "Call omitted or cancelled",
}

CONTAINER_EVENTS: dict[str, str] = {
    "GTIN": "Gate in to terminal",
    "GTOT": "Gate out of terminal",
    "LOAD": "Loaded on vessel",
    "DISC": "Discharged from vessel",
    "STUF": "Container stuffed",
    "STRP": "Container stripped",
    "ROLL": "Rolled to a later sailing",
    "HOLD": "Customs hold",
    "RELS": "Customs released",
    "CANC": "Booking cancelled",
    "ASGN": "Equipment reassigned",
    "ONHR": "Equipment on-hire",
    "OFHR": "Equipment off-hire",
    "RPRD": "Equipment repaired",
}

CODE_DOMAINS: dict[str, dict[str, str]] = {
    "port_event": PORT_EVENTS,
    "container_event": CONTAINER_EVENTS,
}

_PORT_EVENT_RAW = {
    "10": "ETA", "20": "ANC", "30": "BRT", "40": "COM", "50": "CMP", "60": "SLD", "99": "OMIT",
    "ETA UPDATE": "ETA", "ETA NOTICE": "ETA", "EOSP": "ANC", "ANCHORED - AWAITING BERTH": "ANC",
    "ANCHORED": "ANC", "ALL FAST": "BRT", "COMMENCED CARGO": "COM", "COMPLETED CARGO": "CMP",
    "COMPLETED": "CMP", "SAILED": "SLD", "CALL CANCELLED": "OMIT", "CALL OMITTED": "OMIT",
}

# Substring rules for carrier free-text (EDI) container statuses, checked in order.
_CONTAINER_TEXT_RULES = [
    ("EQUIPMENT REASSIGNED", "ASGN"), ("ROLLED", "ROLL"), ("CUSTOMS HOLD", "HOLD"),
    ("RELEASE", "RELS"), ("GATE OUT", "GTOT"), ("GATE IN", "GTIN"), ("LOADED", "LOAD"),
    ("DISCHARG", "DISC"), ("EMPTY TO SHIPPER", "GTOT"), ("CANCEL", "CANC"),
]

_CONTAINER_PIPE = {
    "O": "GTOT", "I": "GTIN", "L": "LOAD", "D": "DISC", "S": "STUF", "U": "STRP", "R": "ROLL",
    "H": "HOLD", "H2": "RELS", "X": "CANC", "A": "ASGN", "N": "ONHR", "F": "OFHR", "P": "RPRD",
}

# Colloquial port names / abbreviations not present in the reference files.
_PORT_ALIAS_SEED = {
    "POLB": "USLGB", "POLA": "USLAX", "SAN PEDRO": "USLAX", "LA/LB": "USLAX",
    "HONGKONG": "HKHKG", "PUSAN": "KRPUS", "TSINGTAO": "CNTAO", "DUBAI": "AEJEA",
}


def map_port_event(raw: Any, overrides: dict[str, str] | None = None) -> str | None:
    if raw is None or str(raw).strip() == "":
        return None
    c = str(raw).strip().upper()
    if overrides and c in overrides:
        return overrides[c] or None
    if c in PORT_EVENTS:
        return c
    if c in _PORT_EVENT_RAW:
        return _PORT_EVENT_RAW[c]
    for k, v in _PORT_EVENT_RAW.items():
        if not k.isdigit() and k in c:
            return v
    return None


def map_container_event(
    feed: str | None, raw: Any, overrides: dict[str, str] | None = None
) -> str | None:
    if raw is None or str(raw).strip() == "":
        return None
    c = str(raw).strip().upper()
    if overrides and c in overrides:
        return overrides[c] or None
    f = (feed or "").lower()
    if f == "pipe_legacy":
        return _CONTAINER_PIPE.get(c)
    if c in CONTAINER_EVENTS:
        return c
    for k, v in _CONTAINER_TEXT_RULES:
        if k in c:
            return v
    return None


def empty_indicator(*vals: Any) -> str | None:
    for v in vals:
        if v is None:
            continue
        u = str(v).strip().upper()
        if u in {"EMPTY", "E", "MT"}:
            return "EMPTY"
        if u in {"LADEN", "L", "F", "FL", "FULL"}:
            return "LADEN"
    return None


def normalize_vessel_name(name: str | None) -> str:
    if not name:
        return ""
    s = str(name).upper().strip()
    s = re.sub(r"^(M/?V|MT|SS)\s+", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_imo(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    s = re.sub(r"[^0-9]", "", s)
    return s if len(s) == 7 else None


def _read_csv(name: str) -> list[dict[str, str]]:
    path = SOURCES / "reference" / f"{name}.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8", errors="ignore", newline="") as fh:
        return list(csv.DictReader(fh))


@lru_cache(maxsize=1)
def reference_ports() -> dict[str, dict[str, Any]]:
    """LOCODE -> {name, names[]} from UN/LOCODE + World Port Index reference files."""
    ports: dict[str, dict[str, Any]] = {}
    for r in _read_csv("unlocode_code_list"):
        code = f"{(r.get('Country') or '').strip()}{(r.get('Location') or '').strip()}".upper()
        if LOCODE_RE.match(code):
            p = ports.setdefault(code, {"name": r.get("Name") or code, "names": set()})
            for k in ("Name", "NameWoDiacritics"):
                if r.get(k):
                    p["names"].add(r[k].strip().upper())
    for r in _read_csv("world_port_index"):
        code = re.sub(r"[^A-Z0-9]", "", (r.get("UN/LOCODE") or "").upper())
        if not LOCODE_RE.match(code):
            continue
        p = ports.setdefault(code, {"name": r.get("Main Port Name") or code, "names": set()})
        for k in ("Main Port Name", "Alternate Port Name"):
            for part in re.split(r"[;/]", r.get(k) or ""):
                if part.strip():
                    p["names"].add(part.strip().upper())
    return ports


def _clean_port_text(s: str) -> str:
    t = re.sub(r",\s*[A-Z]{2}$", "", s)
    t = re.sub(r"^(PORT OF|PSA)\s+", "", t)
    t = re.sub(r"\s+(FREE ZONE|PORT|TERMINAL)$", "", t)
    return re.sub(r"\s+", " ", t).strip()


class PortResolver:
    """Resolve free-text port references to UN/LOCODE using reference data + HITL overrides."""

    def __init__(self, overrides: dict[str, str] | None = None) -> None:
        self.ports = reference_ports()
        self.overrides = {k.upper(): v for k, v in (overrides or {}).items()}
        self.name_index: dict[str, str] = {}
        for code, meta in self.ports.items():
            for n in meta["names"]:
                self.name_index.setdefault(n, code)
        for alias, code in _PORT_ALIAS_SEED.items():
            self.name_index.setdefault(alias, code)
        self._cache: dict[str, tuple[str | None, str]] = {}

    def resolve(self, text: Any) -> tuple[str | None, str]:
        """Return (locode | None, method)."""
        if text is None or str(text).strip() == "":
            return None, "empty"
        s = str(text).strip().upper()
        if s in self._cache:
            return self._cache[s]
        out = self._resolve(s)
        self._cache[s] = out
        return out

    def _resolve(self, s: str) -> tuple[str | None, str]:
        if s in self.overrides:
            return (self.overrides[s] or None), "human"
        compact = re.sub(r"[^A-Z0-9]", "", s)
        if compact in self.ports:
            return compact, "locode"
        if LOCODE_RE.match(s):
            return s, "locode_format"
        head = re.split(r"[-_/ ]", s)[0]
        if head in self.ports:
            return head, "terminal_suffix"
        if len(compact) == 3:
            hits = [c for c in self.ports if c[2:] == compact]
            if len(hits) == 1:
                return hits[0], "location_code"
        cleaned = _clean_port_text(s)
        for cand in (s, cleaned):
            if cand in self.name_index:
                return self.name_index[cand], "reference_name"
        for name, code in self.name_index.items():
            if len(name) >= 5 and re.search(rf"\b{re.escape(name)}\b", cleaned):
                return code, "reference_name"
        best = process.extractOne(cleaned, list(self.name_index.keys()), scorer=fuzz.WRatio)
        if best and best[1] >= 92:
            return self.name_index[best[0]], "fuzzy"
        return None, "unresolved"

    def suggest(self, text: str) -> tuple[str | None, float]:
        """Best-effort guess for an unresolved value (used as the HITL suggestion)."""
        cleaned = _clean_port_text(str(text).strip().upper())
        best = process.extractOne(cleaned, list(self.name_index.keys()), scorer=fuzz.WRatio)
        if not best:
            return None, 0.0
        return self.name_index[best[0]], round(best[1] / 100.0, 2)

    def port_names(self) -> dict[str, str]:
        return {c: m["name"] for c, m in self.ports.items()}


# Which Bronze columns hold ports / event codes / vessel ids. Semantic join hints
# (right_entity=port) are unioned with this at runtime by the Quality agent.
FIELD_SPEC: dict[str, dict[str, Any]] = {
    "port_agent_calls": {
        "port": ["port", "port_code", "port_locode", "location"],
        "codes": {"port_event": ["evt", "event_code", "status", "status_code"]},
        "imo": ["imo", "imo_no"],
    },
    "container_events": {
        "port": ["UNLocationCode", "LOC", "loc"],
        "codes": {"container_event": ["equipmentEventTypeCode", "STAT", "evt"]},
        "imo": ["vesselIMONumber"],
    },
    "bills_of_lading": {"port": ["PortOfLoading", "PortOfDischarge"]},
    "carrier_schedules": {"port": ["port_locode"], "imo": ["imo"]},
    "ais_positions": {"port": ["Destination"], "imo": ["IMO"]},
    "crew_manifests": {"port": ["port_meta"], "imo": ["imo_meta"]},
}

# Descriptions for columns the Transform agent derives in Silver.
SILVER_COLUMN_DOCS: dict[str, str] = {
    "master_vessel_id": "Resolved vessel key (join to master_vessel / gold.dim_vessel)",
    "master_port": "Resolved UN/LOCODE port (join to master_port / gold.dim_port)",
    "pol": "Port of loading, resolved UN/LOCODE",
    "pod": "Port of discharge, resolved UN/LOCODE",
    "destination_port": "AIS destination, resolved UN/LOCODE",
    "master_customer_id": "Resolved customer key (join to master_customer / gold.dim_customer)",
    "event_code": "Canonical event code (see code_map)",
    "event_raw": "Original event value before canonical mapping",
    "port_raw": "Original port text before resolution",
    "event_ts": "Event timestamp (UTC where available)",
    "imo": "IMO ship number (7 digits, cleaned)",
    "mmsi": "Maritime Mobile Service Identity",
    "container_id": "Container number (ISO 6346, cleaned)",
    "booking_ref": "Carrier booking reference",
    "empty_indicator": "EMPTY or LADEN",
    "carrier": "Carrier SCAC code",
    "eta_ts": "Published ETA timestamp",
    "sog_clean": "Speed over ground in knots (sentinel 102.3 removed)",
    "nav_status": "AIS navigational status (0 under way, 1 at anchor, 5 moored)",
    "teu": "Twenty-foot equivalent units for the container line",
}
