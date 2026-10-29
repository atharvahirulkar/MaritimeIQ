"""Persistent context: semantic layer, HITL gaps, human mappings, and agent artifacts."""
from __future__ import annotations

import json
from typing import Any

import yaml

from maritimeiq.utils.paths import CONTEXT, ensure_lakehouse_dirs


class ContextStore:
    def __init__(self) -> None:
        ensure_lakehouse_dirs()
        self.semantic_path = CONTEXT / "semantic_layer.yaml"
        self.corrections_path = CONTEXT / "corrections.jsonl"
        self.proposals_path = CONTEXT / "proposals.json"

    # --- generic JSON artifacts -------------------------------------------------
    def load_json(self, name: str, default: Any = None) -> Any:
        path = CONTEXT / name
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default

    def save_json(self, name: str, data: Any) -> None:
        (CONTEXT / name).write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

    def exists(self, name: str) -> bool:
        return (CONTEXT / name).exists()

    # --- semantic layer ---------------------------------------------------------
    def load_semantics(self) -> dict[str, Any]:
        if not self.semantic_path.exists():
            return {"tables": {}}
        with self.semantic_path.open(encoding="utf-8") as f:
            return yaml.safe_load(f) or {"tables": {}}

    def save_semantics(self, data: dict[str, Any]) -> None:
        with self.semantic_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)

    def approve_table(self, table: str, columns: list[dict[str, Any]], auto: bool = False) -> None:
        sem = self.load_semantics()
        tables = sem.setdefault("tables", {})
        tables[table] = {"columns": columns, "approved": True, "auto": auto}
        self.save_semantics(sem)

    def pii_columns(self, table: str) -> list[str]:
        cols = (self.load_semantics().get("tables", {}).get(table) or {}).get("columns") or []
        return [c["name"] for c in cols if c.get("pii") and c.get("status") != "rejected"]

    # --- proposals / corrections ------------------------------------------------
    def save_proposals(self, proposals: dict[str, Any]) -> None:
        self.save_json("proposals.json", proposals)

    def load_proposals(self) -> dict[str, Any]:
        return self.load_json("proposals.json", {}) or {}

    def append_correction(self, correction: dict[str, Any]) -> None:
        with self.corrections_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(correction) + "\n")

    def read_corrections(self, limit: int = 100) -> list[dict[str, Any]]:
        if not self.corrections_path.exists():
            return []
        lines = self.corrections_path.read_text(encoding="utf-8").strip().splitlines()[-limit:]
        return [json.loads(x) for x in lines if x.strip()]

    # --- human-provided mappings (HITL data gaps) -------------------------------
    def load_mappings(self) -> dict[str, dict[str, str]]:
        data = self.load_json("mappings.json", {}) or {}
        data.setdefault("port", {})
        data.setdefault("port_event", {})
        data.setdefault("container_event", {})
        return data

    def set_mapping(self, domain: str, raw: str, value: str | None) -> None:
        data = self.load_mappings()
        data.setdefault(domain, {})[str(raw).strip().upper()] = value or ""
        self.save_json("mappings.json", data)

    # --- gap queues -------------------------------------------------------------
    def load_gaps(self, name: str) -> list[dict[str, Any]]:
        return self.load_json(name, []) or []

    def save_gaps(self, name: str, gaps: list[dict[str, Any]]) -> None:
        self.save_json(name, gaps)

    def resolve_gap(self, name: str, gap_id: str, decision: str, value: Any = None) -> dict[str, Any] | None:
        gaps = self.load_gaps(name)
        hit = None
        for g in gaps:
            if g.get("id") == gap_id:
                g["status"] = "resolved"
                g["decision"] = decision
                g["resolved_value"] = value
                hit = g
        self.save_gaps(name, gaps)
        return hit
