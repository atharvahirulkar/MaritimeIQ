"""API tests against an isolated temporary lakehouse (the real lakehouse and data/ are never touched)."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi.testclient import TestClient

import maritimeiq.agents.gold_builder as gold_builder
import maritimeiq.context_store.store as store_mod
import maritimeiq.utils.logging_feed as feed_mod
from maritimeiq.agents.quality import DATA_GAPS, QualityAgent
from maritimeiq.api import app as app_mod
from maritimeiq.walkthrough import state


@pytest.fixture()
def lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "proj"
    lh = root / "lakehouse"
    dirs = {name: lh / name for name in ("bronze", "silver", "gold", "context")}
    for p in dirs.values():
        p.mkdir(parents=True)
    (root / "data" / "hackathon" / "sources").mkdir(parents=True)
    ctx = dirs["context"]
    for mod, attr in ((store_mod, "CONTEXT"), (feed_mod, "CONTEXT"), (state, "CONTEXT")):
        monkeypatch.setattr(mod, attr, ctx)
    monkeypatch.setattr(state, "WALK_SESSION_PATH", ctx / "walk_session.json")
    monkeypatch.setattr(gold_builder, "GOLD_CATALOG", ctx / "gold_catalog.yaml")
    for attr in ("BRONZE", "SILVER", "GOLD"):
        monkeypatch.setattr(state, attr, dirs[attr.lower()])
    monkeypatch.setattr(state, "LAKEHOUSE", lh)
    monkeypatch.setattr(state, "ROOT", root)
    monkeypatch.setattr(state, "SOURCES", root / "data" / "hackathon" / "sources")
    monkeypatch.setattr(app_mod, "orch", lambda: SimpleNamespace(quality=QualityAgent(None), context=None))
    return root


@pytest.fixture()
def client(lake: Path) -> TestClient:
    return TestClient(app_mod.app)


def test_status_on_empty_lakehouse(client: TestClient) -> None:
    s = client.get("/api/status").json()
    assert [x["id"] for x in s["steps"]][:3] == ["sources", "bronze", "semantics"]
    assert s["resume_step"] == "sources"
    steps = {x["id"]: x for x in s["steps"]}
    assert steps["sources"]["unlocked"] and not steps["bronze"]["unlocked"]
    assert steps["sources"]["continue"]["will_run"] and steps["sources"]["continue"]["stage"] == "ingest"
    assert s["progress"]["pct"] == 0


def test_data_gap_decision_round_trip(client: TestClient, lake: Path) -> None:
    ctx = lake / "lakehouse" / "context"
    gap = {"id": "port:PASIR PANJANG", "kind": "data", "domain": "port", "status": "open", "table": "ais_positions.Destination",
           "field": "port", "raw": "PASIR PANJANG", "count": 42, "detail": "does not resolve", "suggestion": None,
           "suggestion_label": None, "score": 0.0, "rationale": "LLM: Singapore"}
    (ctx / DATA_GAPS).write_text(json.dumps([gap]), encoding="utf-8")

    items = client.get("/api/gaps?kinds=data").json()
    assert [i["id"] for i in items] == ["port:PASIR PANJANG"]

    res = client.post("/api/gaps/decisions", json={"decisions": [{"id": gap["id"], "action": "approve"}]}).json()
    assert res["applied"] == 0 and "suggestion" in res["results"][0]["error"]

    res = client.post("/api/gaps/decisions", json={"decisions": [{"id": gap["id"], "action": "edit", "value": "nope"}]}).json()
    assert res["applied"] == 0 and "UN/LOCODE" in res["results"][0]["error"]

    res = client.post("/api/gaps/decisions", json={"decisions": [{"id": gap["id"], "action": "edit", "value": "sgsin"}]}).json()
    assert res["applied"] == 1
    assert json.loads((ctx / "mappings.json").read_text())["port"]["PASIR PANJANG"] == "SGSIN"
    assert client.get("/api/gaps?kinds=data").json() == []
    assert client.get("/api/status").json()["gaps"]["data"] == 0


def test_hard_reset_keeps_bronze_and_data(client: TestClient, lake: Path) -> None:
    lh = lake / "lakehouse"
    (lh / "bronze" / "ais_positions").mkdir()
    (lh / "silver" / "master_vessel").mkdir()
    (lh / "context" / "proposals.json").write_text("{}", encoding="utf-8")
    source_file = lake / "data" / "hackathon" / "sources" / "keep.csv"
    source_file.write_text("a,b\n1,2\n", encoding="utf-8")

    r = client.post("/api/reset").json()
    assert r["walk_step"] == "bronze" and r["bronze_tables"] == 1
    assert (lh / "bronze" / "ais_positions").exists()
    assert source_file.read_text() == "a,b\n1,2\n"
    assert not any((lh / "silver").iterdir())
    assert not (lh / "context" / "proposals.json").exists()
    backup = lake / r["backup"]
    assert (backup / "silver" / "master_vessel").exists() and (backup / "context" / "proposals.json").exists()
