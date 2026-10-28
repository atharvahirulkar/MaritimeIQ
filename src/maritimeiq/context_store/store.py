"""Semantic layer + corrections persistence."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from maritimeiq.utils.paths import CONTEXT, ensure_lakehouse_dirs


class ContextStore:
    def __init__(self) -> None:
        ensure_lakehouse_dirs()
        self.semantic_path = CONTEXT / "semantic_layer.yaml"
        self.corrections_path = CONTEXT / "corrections.jsonl"
        self.proposals_path = CONTEXT / "proposals.json"

    def load_semantics(self) -> dict[str, Any]:
        if not self.semantic_path.exists():
            return {"tables": {}}
        with self.semantic_path.open(encoding="utf-8") as f:
            return yaml.safe_load(f) or {"tables": {}}

    def save_semantics(self, data: dict[str, Any]) -> None:
        with self.semantic_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)

    def save_proposals(self, proposals: dict[str, Any]) -> None:
        with self.proposals_path.open("w", encoding="utf-8") as f:
            json.dump(proposals, f, indent=2)

    def load_proposals(self) -> dict[str, Any]:
        if not self.proposals_path.exists():
            return {}
        return json.loads(self.proposals_path.read_text(encoding="utf-8"))

    def append_correction(self, correction: dict[str, Any]) -> None:
        with self.corrections_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(correction) + "\n")

    def read_corrections(self, limit: int = 100) -> list[dict[str, Any]]:
        if not self.corrections_path.exists():
            return []
        lines = self.corrections_path.read_text(encoding="utf-8").strip().splitlines()[-limit:]
        return [json.loads(x) for x in lines if x.strip()]

    def approve_table(self, table: str, columns: list[dict[str, Any]], auto: bool = False) -> None:
        sem = self.load_semantics()
        tables = sem.setdefault("tables", {})
        tables[table] = {"columns": columns, "approved": True, "auto": auto}
        self.save_semantics(sem)
