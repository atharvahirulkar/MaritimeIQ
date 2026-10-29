"""Deterministic governance guard for every MCP tool call."""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from maritimeiq.utils.paths import CONTEXT, POLICY_PATH, ensure_lakehouse_dirs
from maritimeiq.utils.logging_feed import FEED


@dataclass
class GuardDecision:
    allowed: bool
    reason: str
    masked: bool = False
    tool: str = ""
    agent: str = ""
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: dict[str, Any] = field(default_factory=dict)


class GovernanceGuard:
    def __init__(self, policy_path: Path = POLICY_PATH) -> None:
        with policy_path.open(encoding="utf-8") as f:
            self.policy = yaml.safe_load(f)
        self.salt = os.environ.get(
            self.policy["pii_masking"].get("salt_env", "MARITIMEIQ_PII_SALT"),
            self.policy["pii_masking"].get("default_salt", "maritimeiq-hackathon-salt"),
        )
        ensure_lakehouse_dirs()
        self.audit_path = Path(self.policy["audit"]["path"])
        if not self.audit_path.is_absolute():
            from maritimeiq.utils.paths import ROOT

            self.audit_path = ROOT / self.audit_path
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        # Runtime semantic PII tags: table -> set(columns)
        self.pii_tags: dict[str, set[str]] = {}
        self.approvals: set[str] = set()

    def register_pii(self, table: str, columns: list[str]) -> None:
        self.pii_tags.setdefault(table, set()).update(columns)

    def register_approval(self, approval_id: str) -> None:
        if not approval_id.startswith(self.policy.get("approval_id_prefix", "APPR-")):
            raise ValueError("Invalid approval id prefix")
        self.approvals.add(approval_id)

    def hash_pii(self, value: str) -> str:
        if value is None:
            return value
        digest = hashlib.sha256(f"{self.salt}:{value}".encode()).hexdigest()
        return f"tok_{digest[:16]}"

    def mask_value(self, value: Any) -> Any:
        if value is None:
            return None
        s = str(value)
        patterns = self.policy["pii_masking"]["patterns"]
        for _, pat in patterns.items():
            s = re.sub(pat, lambda m: self.hash_pii(m.group(0)), s)
        return s

    def _audit(self, decision: GuardDecision, payload: dict[str, Any] | None = None) -> None:
        record = {**asdict(decision), "payload": payload or {}}
        with self.audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        # Also mirror under context for UI
        mirror = CONTEXT / "audit_log.jsonl"
        with mirror.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        level = "info" if decision.allowed else "warn"
        FEED.emit(
            "guard",
            f"{'ALLOW' if decision.allowed else 'BLOCK'} {decision.tool}: {decision.reason}",
            level=level,
            acting_agent=decision.agent,
        )

    def check_sql(self, sql: str, agent: str = "unknown") -> GuardDecision:
        upper = sql.upper()
        for kw in self.policy.get("blocked_sql_keywords", []):
            # word-boundary-ish check
            if re.search(rf"\b{re.escape(kw.upper())}\b", upper):
                d = GuardDecision(
                    False,
                    f"Blocked destructive SQL keyword: {kw}",
                    tool="run_spark_sql",
                    agent=agent,
                    details={"sql": sql[:500]},
                )
                self._audit(d)
                return d
        # Insight cannot touch bronze
        if agent.lower() == "insight" and re.search(r"\bBRONZE\b", upper):
            d = GuardDecision(
                False,
                "Insight agent cannot read Bronze layer",
                tool="run_spark_sql",
                agent=agent,
            )
            self._audit(d)
            return d
        d = GuardDecision(True, "SQL allowed", tool="run_spark_sql", agent=agent)
        self._audit(d, {"sql": sql[:500]})
        return d

    def check_gold_write(self, approval_id: str | None, agent: str = "insight") -> GuardDecision:
        if not self.policy.get("gold_requires_approval", True):
            d = GuardDecision(True, "Gold approval not required", tool="write_delta", agent=agent)
            self._audit(d)
            return d
        if not approval_id or approval_id not in self.approvals:
            d = GuardDecision(
                False,
                "Gold write requires a registered human approval_id",
                tool="write_delta",
                agent=agent,
                details={"approval_id": approval_id},
            )
            self._audit(d)
            return d
        d = GuardDecision(True, "Gold write approved", tool="write_delta", agent=agent)
        self._audit(d, {"approval_id": approval_id})
        return d

    def check_adversarial_prompt(self, prompt: str, agent: str = "insight") -> GuardDecision:
        text = prompt.lower()
        for pat in self.policy.get("adversarial_patterns", []):
            if re.search(pat, text, flags=re.I):
                # Allow benign analytical questions that mention ports etc.; only block
                # when clearly requesting PII dump / destructive ops.
                pii_request = bool(
                    re.search(
                        r"(show|dump|list|reveal|unmask|export).{0,40}(passport|pii|email|phone|tax)",
                        text,
                        flags=re.I,
                    )
                ) or bool(re.search(r"(drop|delete|truncate)\s+(table|from)", text, flags=re.I))
                if pii_request or re.search(r"(drop|delete|truncate)\s+(table|from)", text, flags=re.I):
                    d = GuardDecision(
                        False,
                        f"Adversarial prompt blocked (matched /{pat}/)",
                        tool="insight_ask",
                        agent=agent,
                        details={"prompt": prompt[:300]},
                    )
                    self._audit(d)
                    return d
        d = GuardDecision(True, "Prompt allowed", tool="insight_ask", agent=agent)
        self._audit(d, {"prompt": prompt[:300]})
        return d

    def check_bronze_pii_read(self, table: str, columns: list[str] | None, agent: str) -> GuardDecision:
        if agent.lower() == "insight" and table.startswith("bronze"):
            tagged = self.pii_tags.get(table, set())
            req = set(columns or tagged)
            overlap = tagged & req if columns else tagged
            if overlap or (columns is None and tagged):
                d = GuardDecision(
                    False,
                    f"Insight blocked from Bronze PII columns: {sorted(overlap or tagged)}",
                    tool="read_sample",
                    agent=agent,
                )
                self._audit(d)
                return d
            # Even without tags, insight should not read bronze
            if "bronze" in self.policy.get("insight_forbidden_layers", []):
                d = GuardDecision(
                    False,
                    "Insight agent cannot read Bronze tables",
                    tool="read_sample",
                    agent=agent,
                )
                self._audit(d)
                return d
        d = GuardDecision(True, "Read allowed", tool="read_sample", agent=agent)
        self._audit(d, {"table": table, "columns": columns})
        return d

    def mask_row(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        tagged = self.pii_tags.get(table, set())
        out = {}
        for k, v in row.items():
            if k in tagged or k.lower() in {c.lower() for c in tagged}:
                out[k] = self.hash_pii(str(v)) if v is not None else None
            else:
                out[k] = self.mask_value(v) if isinstance(v, str) else v
        return out


# Process-wide singleton used by MCP tools and agents
GUARD = GovernanceGuard()
