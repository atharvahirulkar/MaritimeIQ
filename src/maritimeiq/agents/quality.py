"""Quality agent: scan Bronze for issues, auto-fix what it can, raise HITL data gaps for the rest."""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any

from pyspark.sql import functions as F
from rapidfuzz import fuzz, process

from maritimeiq.agents.llm import LLM
from maritimeiq.agents.vocab import (
    CODE_DOMAINS,
    FIELD_SPEC,
    LOCODE_RE,
    PortResolver,
    clean_imo,
    map_container_event,
    map_port_event,
)
from maritimeiq.context_store.store import ContextStore
from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.utils.logging_feed import FEED

DATA_GAPS = "data_gaps.json"
QUALITY_REPORT = "quality_report.json"


class QualityAgent:
    MAX_PORT_GAPS = 30

    def __init__(self, gateway: MCPGateway) -> None:
        self.gateway = gateway
        self.store = ContextStore()

    # --- scanning ---------------------------------------------------------------
    def _port_columns(self, table: str, cols: list[str], resolver: PortResolver, df) -> list[str]:
        spec = [c for c in FIELD_SPEC.get(table, {}).get("port", []) if c in cols]
        hints = [
            j.get("left") for j in (self.store.load_proposals().get(table) or {}).get("joins") or []
            if isinstance(j, dict) and j.get("right_entity") == "port" and j.get("left") in cols
        ]
        for c in hints:
            if c in spec or c.startswith("_"):
                continue
            vals = [r[0] for r in df.select(c).where(F.col(c).isNotNull()).distinct().limit(200).collect()]
            hit = sum(1 for v in vals if resolver.resolve(v)[0])
            if vals and hit / len(vals) >= 0.3:
                spec.append(c)
        return spec

    def scan(self) -> dict[str, Any]:
        mappings = self.store.load_mappings()
        resolver = PortResolver(mappings["port"])
        tables = [t for t in self.gateway.lakehouse.list_tables("bronze")["tables"] if not t.startswith("ref_")]
        report: dict[str, Any] = {"tables": {}}
        port_counts: Counter = Counter()
        port_where: dict[str, set[str]] = defaultdict(set)
        unmapped: dict[str, Counter] = {d: Counter() for d in CODE_DOMAINS}
        code_where: dict[tuple[str, str], set[str]] = defaultdict(set)

        for table in tables:
            df = self.gateway.lakehouse.load(f"bronze.{table}")
            cols = [c for c in df.columns if not c.startswith("_")]
            aggs = [F.count(F.lit(1)).alias("__n")] + [F.count(F.col(f"`{c}`")).alias(f"c{i}") for i, c in enumerate(cols)]
            if "_row_hash" in df.columns:
                aggs.append(F.countDistinct("_row_hash").alias("__d"))
            st = df.agg(*aggs).collect()[0].asDict()
            n = st["__n"] or 0
            sparse = sorted(
                ({"column": c, "null_pct": round(1 - st[f"c{i}"] / n, 3)} for i, c in enumerate(cols)
                 if n and 1 - st[f"c{i}"] / n >= 0.5),
                key=lambda x: -x["null_pct"],
            )
            entry: dict[str, Any] = {
                "rows": n,
                "duplicates": n - st.get("__d", n),
                "sparse_columns": sparse,
                "schema_variants": len(sparse) > len(cols) * 0.3,
            }
            spec = FIELD_SPEC.get(table, {})

            pcols = self._port_columns(table, cols, resolver, df)
            resolved = unresolved = 0
            for c in pcols:
                for raw, cnt in df.groupBy(c).count().collect():
                    if raw is None or str(raw).strip() == "":
                        continue
                    code, _ = resolver.resolve(raw)
                    if code:
                        resolved += cnt
                    else:
                        unresolved += cnt
                        key = str(raw).strip().upper()
                        port_counts[key] += cnt
                        port_where[key].add(f"{table}.{c}")
            entry["port_columns"] = pcols
            entry["port_values"] = {"resolved": resolved, "unresolved": unresolved}

            code_issues = []
            for domain, ccols in (spec.get("codes") or {}).items():
                for c in [x for x in ccols if x in cols]:
                    feed = "feed_format" if "feed_format" in df.columns else None
                    grp = df.groupBy(*( [feed] if feed else []), c).count().collect()
                    for r in grp:
                        raw = r[c]
                        if raw is None:
                            continue
                        f = r[feed] if feed else None
                        code = (map_container_event(f, raw, mappings[domain]) if domain == "container_event"
                                else map_port_event(raw, mappings[domain]))
                        key = str(raw).strip().upper()
                        if code is None and mappings[domain].get(key) != "":
                            unmapped[domain][key] += r["count"]
                            code_where[(domain, key)].add(f"{table}.{c}")
                            code_issues.append(key)
            entry["unmapped_codes"] = sorted(set(code_issues))

            bad_imo = 0
            for c in [x for x in spec.get("imo", []) if x in cols]:
                for raw, cnt in df.groupBy(c).count().collect():
                    if raw is not None and str(raw).strip() and clean_imo(raw) is None:
                        bad_imo += cnt
            entry["invalid_imo"] = bad_imo
            report["tables"][table] = entry
            FEED.emit("quality", f"{table}: dupes={entry['duplicates']} unresolved_ports={unresolved} "
                      f"unmapped_codes={len(entry['unmapped_codes'])} invalid_imo={bad_imo}")

        gaps = self._port_gaps(port_counts, port_where, resolver) + self._code_gaps(unmapped, code_where)
        residual = sum(c for raw, c in port_counts.items() if f"port:{raw}" not in {g["id"] for g in gaps})
        report["summary"] = {
            "tables": len(tables),
            "duplicates": sum(t["duplicates"] for t in report["tables"].values()),
            "unresolved_port_values": len(port_counts),
            "unresolved_port_rows_beyond_queue": residual,
            "unmapped_codes": sum(len(c) for c in unmapped.values()),
            "gaps_raised": len(gaps),
        }
        return {"report": report, "gaps": gaps}

    # --- gap suggestions --------------------------------------------------------
    def _port_gaps(self, counts: Counter, where: dict[str, set[str]], resolver: PortResolver) -> list[dict[str, Any]]:
        top = [raw for raw, _ in counts.most_common(self.MAX_PORT_GAPS)]
        known = resolver.port_names()
        llm = {}
        if top and LLM.available:
            raw = LLM.complete_json(
                "You resolve messy port references from shipping documents. For each value, first write `place`: "
                "what real-world place, terminal, or code this is and which city/port complex it belongs to (a "
                "container terminal belongs to its port; an IATA city code belongs to that city's port). Then pick "
                "the matching UN/LOCODE from known_ports (code -> name), or null if the place is not one of them. "
                "Return JSON {\"mappings\": [{\"raw\", \"place\", \"locode\", \"confidence\": 0-1}]}.",
                json.dumps({"known_ports": known, "values": top}),
            )
            for m in (raw or {}).get("mappings") or []:
                if isinstance(m, dict) and m.get("raw"):
                    llm[str(m["raw"]).upper()] = m
        gaps = []
        for raw in top:
            m = llm.get(raw)
            if m is not None:
                sug = m.get("locode") if m.get("locode") in known else None
                score = float(m.get("confidence") or 0) if sug else 0.0
                why = f"LLM: {m.get('place') or 'unknown place'}"
            else:
                sug, score = resolver.suggest(raw)
                why = "closest reference name (no LLM)"
            gaps.append({
                "id": f"port:{raw}", "kind": "data", "domain": "port", "status": "open",
                "table": ", ".join(sorted(where[raw]))[:120], "field": "port", "raw": raw, "count": counts[raw],
                "detail": f"'{raw}' ({counts[raw]:,} rows) does not resolve to a UN/LOCODE",
                "suggestion": sug, "suggestion_label": f"{sug} · {known.get(sug, '')}" if sug else None,
                "score": round(score, 2), "rationale": why,
            })
        return gaps

    def _code_gaps(self, unmapped: dict[str, Counter], where: dict[tuple[str, str], set[str]]) -> list[dict[str, Any]]:
        gaps = []
        for domain, counts in unmapped.items():
            if not counts:
                continue
            vocab = CODE_DOMAINS[domain]
            llm = {}
            if LLM.available:
                raw = LLM.complete_json(
                    "Map raw shipping event labels from carrier feeds to a canonical code list (code -> description). "
                    "For each value, first write `meaning`: what physically happens to the vessel/container. Then pick "
                    "the code with that meaning, or null if none fits. "
                    "Return JSON {\"mappings\": [{\"raw\", \"meaning\", \"code\", \"confidence\": 0-1}]}.",
                    json.dumps({"codes": vocab, "values": list(counts.keys())}),
                )
                for m in (raw or {}).get("mappings") or []:
                    if isinstance(m, dict) and m.get("raw"):
                        llm[str(m["raw"]).upper()] = m
            labels = {desc.upper(): code for code, desc in vocab.items()}
            for raw, cnt in counts.most_common():
                m = llm.get(raw)
                if m is not None:
                    sug = m.get("code") if m.get("code") in vocab else None
                    score = float(m.get("confidence") or 0) if sug else 0.0
                    why = f"LLM: {m.get('meaning') or ''}"
                else:
                    best = process.extractOne(raw, list(labels.keys()), scorer=fuzz.WRatio)
                    sug, score, why = ((labels[best[0]], best[1] / 100, "closest code description (no LLM)")
                                       if best else (None, 0.0, ""))
                gaps.append({
                    "id": f"{domain}:{raw}", "kind": "data", "domain": domain, "status": "open",
                    "table": ", ".join(sorted(where[(domain, raw)])), "field": domain.replace("_", " "),
                    "raw": raw, "count": cnt,
                    "detail": f"'{raw}' ({cnt:,} rows) is not in the {domain.replace('_', ' ')} vocabulary",
                    "suggestion": sug, "suggestion_label": f"{sug} · {vocab.get(sug, '')}" if sug else None,
                    "score": round(score, 2), "rationale": why,
                })
        return gaps

    # --- lifecycle --------------------------------------------------------------
    def run(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(state or {})
        FEED.emit("quality", "Scanning Bronze for duplicates, nulls, unresolved keys, and unmapped codes")
        out = self.scan()
        prior = {g["id"]: g for g in self.store.load_gaps(DATA_GAPS) if g.get("status") == "resolved"}
        gaps = list(prior.values()) + [g for g in out["gaps"] if g["id"] not in prior]
        self.store.save_gaps(DATA_GAPS, gaps)
        self.store.save_json(QUALITY_REPORT, out["report"])
        state["quality"] = out["report"]["summary"]
        FEED.emit("quality", f"Quality scan done: {out['report']['summary']}", event_type="quality_complete")
        return state

    def apply_gap_decision(self, gap_id: str, decision: str, value: str | None = None) -> dict[str, Any]:
        gap = next((g for g in self.store.load_gaps(DATA_GAPS) if g["id"] == gap_id), None)
        if gap is None:
            return {"ok": False, "error": "gap not found"}
        domain = gap["domain"]
        if decision == "approve":
            mapped = gap.get("suggestion")
        elif decision == "edit":
            mapped = (value or "").strip().upper()
            valid = LOCODE_RE.match(mapped) if domain == "port" else mapped in CODE_DOMAINS[domain]
            if not valid:
                return {"ok": False, "error": f"'{value}' is not a valid {'UN/LOCODE' if domain == 'port' else 'code'}"}
        else:
            mapped = ""
        self.store.set_mapping(domain, gap["raw"], mapped)
        self.store.resolve_gap(DATA_GAPS, gap_id, decision, mapped)
        self.store.append_correction({"kind": "data_gap", "domain": domain, "raw": gap["raw"],
                                      "action": decision, "value": mapped})
        return {"ok": True}
