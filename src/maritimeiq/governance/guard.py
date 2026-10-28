"""Deterministic governance guard for every MCP tool call."""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from maritimeiq.governance.sql_guard import validate_select
from maritimeiq.utils.paths import CONTEXT, LAKEHOUSE, POLICY_PATH, ROOT, ensure_lakehouse_dirs
from maritimeiq.utils.logging_feed import FEED

SALT_FILE = LAKEHOUSE / ".pii_salt"

# Agents whose SQL must be a single read-only query (Text-to-SQL / investigation)
READ_ONLY_AGENTS = {"insight", "gold_builder", "eval_baseline"}


@dataclass
class GuardDecision:
    allowed: bool
    reason: str
    masked: bool = False
    tool: str = ""
    agent: str = ""
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: dict[str, Any] = field(default_factory=dict)


def _load_salt(env_name: str) -> str:
    """Env salt wins; else a random per-install salt persisted outside source control."""
    env = os.environ.get(env_name)
    if env:
        return env
    if SALT_FILE.exists():
        return SALT_FILE.read_text(encoding="utf-8").strip()
    SALT_FILE.parent.mkdir(parents=True, exist_ok=True)
    salt = secrets.token_hex(16)
    SALT_FILE.write_text(salt, encoding="utf-8")
    return salt


class GovernanceGuard:
    def __init__(self, policy_path: Path = POLICY_PATH) -> None:
        with policy_path.open(encoding="utf-8") as f:
            self.policy = yaml.safe_load(f)
        ensure_lakehouse_dirs()
        self.salt = _load_salt(self.policy["pii_masking"].get("salt_env", "MARITIMEIQ_PII_SALT"))
        self.audit_path = Path(self.policy["audit"]["path"])
        if not self.audit_path.is_absolute():
            self.audit_path = ROOT / self.audit_path
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        # Runtime semantic PII tags: table -> set(columns)
        self.pii_tags: dict[str, set[str]] = {}
        self.approvals: set[str] = set()
        self._load_approvals()

    # ---- approvals -------------------------------------------------------
    @property
    def approvals_path(self) -> Path:
        return CONTEXT / "approvals.jsonl"

    def _load_approvals(self) -> None:
        """Approvals survive process restarts (UI reload, eval CLI)."""
        if not self.approvals_path.exists():
            return
        for line in self.approvals_path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            aid = rec.get("approval_id")
            if aid and aid.startswith(self.policy.get("approval_id_prefix", "APPR-")):
                self.approvals.add(aid)

    def register_approval(self, approval_id: str) -> None:
        if not approval_id.startswith(self.policy.get("approval_id_prefix", "APPR-")):
            raise ValueError("Invalid approval id prefix")
        self.approvals.add(approval_id)

    # ---- PII -------------------------------------------------------------
    def register_pii(self, table: str, columns: list[str]) -> None:
        self.pii_tags.setdefault(table, set()).update(columns)

    def sync_semantic_pii(self) -> set[str]:
        """Reload PII tags from the approved semantic layer; return all PII column names."""
        path = CONTEXT / "semantic_layer.yaml"
        names: set[str] = set()
        if not path.exists():
            return names
        try:
            sem = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            return names
        for table, meta in (sem.get("tables") or {}).items():
            cols = [c["name"] for c in (meta.get("columns") or []) if c.get("pii") and c.get("name")]
            if cols:
                self.register_pii(f"bronze.{table}", cols)
                self.register_pii(f"silver.{table}", cols)
                names.update(c.lower() for c in cols)
        return names

    def all_pii_columns(self) -> set[str]:
        return {c.lower() for cols in self.pii_tags.values() for c in cols}

    def hash_pii(self, value: str) -> str:
        if value is None:
            return value
        digest = hashlib.sha256(f"{self.salt}:{value}".encode()).hexdigest()
        return f"tok_{digest[:16]}"

    def mask_value(self, value: Any) -> Any:
        if value is None:
            return None
        s = str(value)
        if s.startswith("tok_"):
            return s
        patterns = self.policy["pii_masking"]["patterns"]
        for _, pat in patterns.items():
            s = re.sub(pat, lambda m: self.hash_pii(m.group(0)), s)
        return s

    def mask_row(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        tagged = {c.lower() for c in self.pii_tags.get(table, set())}
        out = {}
        for k, v in row.items():
            if k.lower() in tagged and not str(k).lower().endswith("_masked"):
                out[k] = self.hash_pii(str(v)) if v is not None else None
            else:
                out[k] = self.mask_value(v) if isinstance(v, str) else v
        return out

    def mask_result_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Mask query output: PII-tagged column names (any table) + regex PII in strings."""
        pii = self.all_pii_columns() | self.sync_semantic_pii()
        out = []
        for row in rows:
            masked = {}
            for k, v in row.items():
                kl = str(k).lower()
                if kl in pii and not kl.endswith("_masked") and v is not None:
                    masked[k] = self.hash_pii(str(v))
                elif isinstance(v, str):
                    masked[k] = self.mask_value(v)
                else:
                    masked[k] = v
            out.append(masked)
        return out

    # ---- audit -----------------------------------------------------------
    def _audit(self, decision: GuardDecision, payload: dict[str, Any] | None = None) -> None:
        record = {**asdict(decision), "payload": payload or {}}
        line = json.dumps(record, default=str) + "\n"
        with self.audit_path.open("a", encoding="utf-8") as f:
            f.write(line)
        # UI reads CONTEXT/audit_log.jsonl — mirror only when policy points elsewhere
        mirror = CONTEXT / "audit_log.jsonl"
        if mirror.resolve() != self.audit_path.resolve():
            with mirror.open("a", encoding="utf-8") as f:
                f.write(line)
        level = "info" if decision.allowed else "warn"
        FEED.emit(
            "guard",
            f"{'ALLOW' if decision.allowed else 'BLOCK'} {decision.tool}: {decision.reason}",
            level=level,
            acting_agent=decision.agent,
            event_type=None if decision.allowed else "guard_block",
        )

    # ---- checks ----------------------------------------------------------
    def check_sql(self, sql: str, agent: str = "unknown", allowed_tables: set[str] | None = None) -> GuardDecision:
        upper = sql.upper()
        for kw in self.policy.get("blocked_sql_keywords", []):
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
        if agent.lower() in READ_ONLY_AGENTS:
            check = validate_select(sql, allowed_tables)
            if agent.lower() != "eval_baseline" and not check.ok:
                d = GuardDecision(False, check.reason, tool="run_spark_sql", agent=agent, details={"sql": sql[:500]})
                self._audit(d)
                return d
        if agent.lower() == "insight" and re.search(r"\bBRONZE", upper):
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
        reason = None
        # Destructive intent
        if re.search(r"\b(drop|delete|truncate|alter|insert|update)\s+(table|from|into|database|schema)\b", text):
            reason = "destructive operation requested"
        # Prompt-injection attempts against the agent
        elif re.search(
            r"ignore (all |any )?(previous|prior|above|your) (instructions|rules)|system prompt|"
            r"you are now|developer mode|bypass (the )?(guard|policy|rules)",
            text,
        ):
            reason = "prompt injection attempt"
        else:
            pii_term = None
            for pat in self.policy.get("adversarial_patterns", []):
                if re.search(pat, text, flags=re.I):
                    pii_term = pat
                    break
            if pii_term is None:
                pii_term = next(
                    (
                        p
                        for p in self.policy.get("pii_terms", [])
                        if re.search(p, text, flags=re.I)
                    ),
                    None,
                )
            # Aggregates over PII ("how many crew have passports on file") are fine;
            # requests for the values themselves are not.
            aggregate = re.search(r"\b(how many|count of|number of (crew|records|rows|people)|share of|percentage|ratio)\b", text)
            if pii_term and not aggregate:
                reason = f"PII value request (matched /{pii_term}/)"
        if reason:
            d = GuardDecision(
                False,
                f"Adversarial prompt blocked: {reason}",
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


# Process-wide singleton used by MCP tools and agents
GUARD = GovernanceGuard()
