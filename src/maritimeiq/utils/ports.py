"""Reference-driven port resolution (UN/LOCODE + World Port Index + human-approved aliases)."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from rapidfuzz import fuzz, process

from maritimeiq.utils.paths import BRONZE, CONTEXT

PORT_ALIASES_PATH = CONTEXT / "port_aliases.json"

# Industry shorthand that no reference file carries. Everything else comes from reference data.
SEED_ALIASES = {
    "POLA": "USLAX",
    "POLB": "USLGB",
    "LA/LB": "USLAX",
    "SAN PEDRO": "USLAX",
}

_LOCODE = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}$")
_TERMINAL_SUFFIX = re.compile(r"^([A-Z]{2}[A-Z0-9]{3})-[A-Z0-9]{1,6}$")


def clean_port_text(text: Any) -> str:
    if text is None:
        return ""
    s = str(text).upper().strip()
    s = re.sub(r"\s+", " ", s)
    m = _TERMINAL_SUFFIX.match(s)
    if m:
        return m.group(1)
    s = re.sub(r"^PORT OF\s+", "", s)
    s = re.sub(r"\s+(PORT|PT|HARBOU?R|TERMINALS?)$", "", s)
    s = re.sub(r",\s*[A-Z]{2}$", "", s)  # "Jebel Ali, AE"
    s = re.sub(r"[^A-Z0-9/ ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


@dataclass
class PortResolver:
    """Picklable resolver (plain dicts) so it can run inside Spark UDFs."""

    locodes: set[str] = field(default_factory=set)
    names: dict[str, str] = field(default_factory=dict)
    location_codes: dict[str, str] = field(default_factory=dict)
    regions: dict[str, str] = field(default_factory=dict)
    port_names: dict[str, str] = field(default_factory=dict)
    human_aliases: dict[str, str | None] = field(default_factory=dict)

    def resolve(self, text: Any) -> tuple[str | None, str]:
        """Return (UN/LOCODE or None, method)."""
        s = clean_port_text(text)
        if not s or s in {"FOR ORDERS", "TBA", "NULL", "NONE", "N A"}:
            return None, "empty"
        if s in self.human_aliases:
            return self.human_aliases[s], "human"
        compact = s.replace(" ", "")
        if _LOCODE.match(compact) and compact in self.locodes:
            return compact, "locode"
        if s in SEED_ALIASES:
            return SEED_ALIASES[s], "seed_alias"
        if s in self.names:
            return self.names[s], "reference_name"
        if len(compact) == 3 and compact in self.location_codes:
            return self.location_codes[compact], "location_code"
        if _LOCODE.match(compact) and not self.locodes:
            return compact, "locode_unverified"
        tokens = s.replace("/", " ").split()
        # "LOS ANGELES CA" — trailing state / country token
        if len(tokens) > 1 and len(tokens[-1]) == 2 and " ".join(tokens[:-1]) in self.names:
            return self.names[" ".join(tokens[:-1])], "reference_name"
        # "BUSAN NEW PORT", "PSA SINGAPORE" — a full reference name inside the text (unique hit)
        padded = f" {' '.join(tokens)} "
        hits = {code for name, code in self.names.items() if len(name) >= 5 and f" {name} " in padded}
        if len(hits) == 1:
            return hits.pop(), "contains_name"
        if self.names and len(s) >= 4:
            hit = process.extractOne(s, list(self.names), scorer=fuzz.token_sort_ratio)
            if hit and hit[1] >= 92:
                return self.names[hit[0]], "fuzzy"
        return None, "unresolved"

    def code(self, text: Any) -> str | None:
        return self.resolve(text)[0]

    def candidates(self, text: Any, n: int = 3) -> list[dict[str, Any]]:
        s = clean_port_text(text)
        if not s or not self.names:
            return []
        hits = process.extract(s, list(self.names), scorer=fuzz.token_sort_ratio, limit=n * 3)
        out, seen = [], set()
        for name, score, _ in hits:
            code = self.names[name]
            if code in seen:
                continue
            seen.add(code)
            out.append({"locode": code, "name": self.port_names.get(code, name.title()), "score": round(score)})
            if len(out) >= n:
                break
        return out


def load_human_aliases() -> dict[str, str | None]:
    if not PORT_ALIASES_PATH.exists():
        return {}
    try:
        return json.loads(PORT_ALIASES_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def save_human_alias(raw: str, locode: str | None) -> None:
    aliases = load_human_aliases()
    aliases[clean_port_text(raw)] = locode
    PORT_ALIASES_PATH.parent.mkdir(parents=True, exist_ok=True)
    PORT_ALIASES_PATH.write_text(json.dumps(aliases, indent=2, sort_keys=True), encoding="utf-8")


def build_resolver(spark) -> PortResolver:
    """Build from Bronze reference tables (ref_unlocode_code_list, ref_world_port_index)."""
    r = PortResolver(human_aliases=load_human_aliases())
    loc_counts: dict[str, set[str]] = {}

    def add_name(name: Any, code: str) -> None:
        key = clean_port_text(name)
        if key and key not in r.names:
            r.names[key] = code

    un_path = BRONZE / "ref_unlocode_code_list"
    if un_path.exists():
        for row in spark.read.format("delta").load(str(un_path)).collect():
            d = row.asDict()
            country, loc = (d.get("Country") or "").strip().upper(), (d.get("Location") or "").strip().upper()
            if not country or not loc:
                continue
            code = f"{country}{loc}"
            r.locodes.add(code)
            loc_counts.setdefault(loc, set()).add(code)
            r.port_names.setdefault(code, d.get("Name") or code)
            add_name(d.get("Name"), code)
            add_name(d.get("NameWoDiacritics"), code)

    wpi_path = BRONZE / "ref_world_port_index"
    if wpi_path.exists():
        for row in spark.read.format("delta").load(str(wpi_path)).collect():
            d = row.asDict()
            code = (d.get("UN_LOCODE") or "").replace(" ", "").upper()
            if not _LOCODE.match(code):
                continue
            r.locodes.add(code)
            loc_counts.setdefault(code[2:], set()).add(code)
            if d.get("Region_Name"):
                r.regions[code] = d["Region_Name"]
            r.port_names.setdefault(code, d.get("Main_Port_Name") or code)
            add_name(d.get("Main_Port_Name"), code)
            for alt in re.split(r"[;/,]", d.get("Alternate_Port_Name") or ""):
                add_name(alt, code)

    r.location_codes = {loc: next(iter(codes)) for loc, codes in loc_counts.items() if len(codes) == 1}
    return r
