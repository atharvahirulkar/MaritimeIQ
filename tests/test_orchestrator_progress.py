"""Stage checkpointing: resume instead of restart; human decisions invalidate downstream."""
from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def orch_mod(tmp_path: Path, monkeypatch):
    import maritimeiq.agents.orchestrator as o

    monkeypatch.setattr(o, "PROGRESS_PATH", tmp_path / "progress.json")
    return o


def test_mark_and_invalidate(orch_mod) -> None:
    for s in ["bronze", "context", "er", "silver", "gold"]:
        orch_mod.mark_done(s)
    orch_mod.invalidate_from("silver")
    assert set(orch_mod.load_progress()) == {"bronze", "context", "er"}
    orch_mod.reset_progress()
    assert orch_mod.load_progress() == {}
