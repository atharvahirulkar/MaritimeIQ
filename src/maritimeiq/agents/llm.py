"""Optional OpenAI client with deterministic heuristic fallback (no API key required)."""
from __future__ import annotations

import json
import os
import re
from typing import Any

from pydantic import BaseModel, Field

from maritimeiq.utils.env import load_env

load_env()


class ColumnProposal(BaseModel):
    name: str
    meaning: str
    data_type_hint: str = "string"
    pii: bool = False
    pii_class: str | None = None
    code_map: dict[str, str] = Field(default_factory=dict)
    confidence: float = 0.5
    rationale: str = ""


class SemanticProposal(BaseModel):
    columns: list[ColumnProposal]
    joins: list[dict[str, Any]] = Field(default_factory=list)


def _heuristic_column(name: str, samples: list[Any]) -> ColumnProposal:
    n = name.lower()
    pii = False
    pii_class = None
    meaning = f"Field `{name}` from source profile"
    confidence = 0.55
    code_map: dict[str, str] = {}

    if any(x in n for x in ["passport", "email", "phone", "tax", "dob", "birth", "contact", "seaman", "cdc"]):
        pii = True
        pii_class = "direct"
        confidence = 0.95
        meaning = f"PII field: {name}"
    elif n in {"mmsi"} or "mmsi" in n:
        meaning = "Maritime Mobile Service Identity"
        confidence = 0.9
    elif n in {"imo", "imo_number", "vesselimonumber"} or n.endswith("imo"):
        meaning = "IMO ship number"
        confidence = 0.9
    elif "vessel" in n or n in {"ship", "vslname", "ship_name"}:
        meaning = "Vessel name (free text / AIS static)"
        confidence = 0.85
    elif "locode" in n or n in {"port", "port_code", "unlocationcode", "location", "pol", "pod"}:
        meaning = "Port identifier (LOCODE or alias)"
        confidence = 0.85
    elif n in {"navstatus", "status", "status_code", "evt", "event_code", "equipmenteventtypecode"}:
        meaning = "Status / event code (needs dictionary)"
        confidence = 0.7
        if n == "navstatus" or n == "status":
            code_map = {"0": "under way", "1": "at anchor", "5": "moored", "15": "undefined"}
        if n == "status_code":
            code_map = {
                "10": "ETA",
                "20": "arrived anchorage",
                "30": "berthed",
                "40": "ops commenced",
                "50": "ops completed",
                "60": "departed",
                "99": "cancelled",
            }
        if n == "evt":
            code_map = {
                "ETA": "ETA notice",
                "ANC": "anchored",
                "BRT": "berthed",
                "COM": "cargo commenced",
                "CMP": "completed",
                "SLD": "sailed",
                "OMIT": "omitted",
            }
    elif "lat" in n:
        meaning = "Latitude decimal degrees"
        confidence = 0.9
    elif "lon" in n:
        meaning = "Longitude decimal degrees"
        confidence = 0.9

    # sample-based boosts
    sample_str = " ".join(str(s) for s in samples[:5])
    if re.search(r"[A-Z]{2}[A-Z0-9]{3}", sample_str) and "port" in meaning.lower():
        confidence = min(1.0, confidence + 0.05)

    return ColumnProposal(
        name=name,
        meaning=meaning,
        pii=pii,
        pii_class=pii_class,
        code_map=code_map,
        confidence=confidence,
        rationale="Heuristic proposal from column name + samples",
    )


class LLMClient:
    def __init__(self) -> None:
        load_env()
        self.provider = (os.environ.get("MARITIMEIQ_LLM_PROVIDER") or "openai").lower()
        self.model = os.environ.get("MARITIMEIQ_MODEL", "gpt-4o-mini")
        self.api_key = os.environ.get("OPENAI_API_KEY")
        self._client = None
        self._vertex = None
        self.last_source: str = "none"
        self._init_client()

    def _init_client(self) -> None:
        if self.provider == "vertex":
            # Vertex via OpenAI-compatible endpoint or google-genai when configured
            project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
            location = os.environ.get("VERTEX_LOCATION", "us-central1")
            # Prefer OpenAI-compatible Vertex endpoint if key/base set
            base = os.environ.get("OPENAI_BASE_URL") or os.environ.get("VERTEX_OPENAI_BASE_URL")
            key = os.environ.get("OPENAI_API_KEY") or os.environ.get("VERTEX_API_KEY") or "unused"
            if base:
                try:
                    from openai import OpenAI

                    self._client = OpenAI(api_key=key, base_url=base)
                    self.model = os.environ.get("MARITIMEIQ_MODEL", "google/gemini-2.0-flash")
                    return
                except Exception:  # noqa: BLE001
                    self._client = None
            self._vertex = {"project": project, "location": location}
            return
        if self.api_key:
            try:
                from openai import OpenAI

                self._client = OpenAI(api_key=self.api_key)
            except Exception:  # noqa: BLE001
                self._client = None

    @property
    def available(self) -> bool:
        return self._client is not None

    def complete_json(self, system: str, user: str) -> dict[str, Any] | None:
        if not self._client:
            return None
        for _attempt in range(2):
            try:
                resp = self._client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                )
                content = resp.choices[0].message.content or "{}"
                return json.loads(content)
            except Exception:  # noqa: BLE001
                continue
        return None

    def _heuristic_proposal(self, columns: list[str], profiles: dict[str, dict[str, Any]]) -> SemanticProposal:
        cols = [_heuristic_column(c, profiles.get(c, {}).get("samples", [])) for c in columns]
        joins = []
        for c in columns:
            cl = c.lower()
            if "imo" in cl or "mmsi" in cl or "vessel" in cl or cl in {"ship", "vslname"}:
                joins.append({"left": c, "right_entity": "vessel", "confidence": 0.8})
            if "port" in cl or "locode" in cl or cl in {"location", "pol", "pod"}:
                joins.append({"left": c, "right_entity": "port", "confidence": 0.8})
        return SemanticProposal(columns=cols, joins=joins)

    def propose_semantics(
        self,
        table: str,
        columns: list[str],
        profiles: dict[str, dict[str, Any]],
        corrections: list[dict[str, Any]],
        system_prompt: str,
    ) -> SemanticProposal:
        payload = {
            "table": table,
            "columns": columns,
            "profiles": profiles,
            "corrections": corrections[-20:],
        }
        # LLM-first when available; heuristics only after failed parse/retry
        if self.available:
            raw = self.complete_json(system_prompt, json.dumps(payload))
            if raw:
                try:
                    proposal = SemanticProposal.model_validate(raw)
                    self.last_source = "llm"
                    return proposal
                except Exception:  # noqa: BLE001
                    raw2 = self.complete_json(
                        system_prompt + "\nReturn ONLY valid JSON matching the schema.",
                        json.dumps(payload),
                    )
                    if raw2:
                        try:
                            proposal = SemanticProposal.model_validate(raw2)
                            self.last_source = "llm"
                            return proposal
                        except Exception:  # noqa: BLE001
                            pass
        self.last_source = "heuristic"
        return self._heuristic_proposal(columns, profiles)


LLM = LLMClient()
