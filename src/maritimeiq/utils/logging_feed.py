"""Agent event feed: in-memory for live streaming, appended to agent_feed.jsonl for history."""
from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from maritimeiq.utils.paths import CONTEXT, ensure_lakehouse_dirs


@dataclass
class AgentEvent:
    agent: str
    message: str
    level: str = "info"
    data: dict[str, Any] = field(default_factory=dict)
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class AgentFeed:
    def __init__(self) -> None:
        self._events: list[AgentEvent] = []
        self._lock = threading.Lock()

    def emit(self, agent: str, message: str, level: str = "info", **data: Any) -> AgentEvent:
        ev = AgentEvent(agent=agent, message=message, level=level, data=data)
        with self._lock:
            self._events.append(ev)
        ensure_lakehouse_dirs()
        path = CONTEXT / "agent_feed.jsonl"
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(ev)) + "\n")
        return ev

    def recent(self, n: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            return [asdict(e) for e in self._events[-n:]]

    def since(self, cursor: int) -> tuple[int, list[dict[str, Any]]]:
        """Events after position `cursor`, plus the new cursor (for streaming)."""
        with self._lock:
            return len(self._events), [asdict(e) for e in self._events[cursor:]]

    def clear(self) -> None:
        with self._lock:
            self._events.clear()


FEED = AgentFeed()
