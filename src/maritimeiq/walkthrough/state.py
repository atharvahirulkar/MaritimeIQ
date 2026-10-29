"""Walkthrough state: steps, gating from lakehouse artifacts, staleness, reset, summaries."""
from __future__ import annotations

import json
import shutil
import time
from typing import Any

from maritimeiq.agents.discovery import INSIGHTS
from maritimeiq.agents.gold_builder import load_gold_catalog
from maritimeiq.agents.orchestrator import STAGE_RUNS, open_gap_counts
from maritimeiq.agents.quality import DATA_GAPS, QUALITY_REPORT
from maritimeiq.agents.transform import CLEANUP_REPORT
from maritimeiq.context_store.store import ContextStore
from maritimeiq.utils.paths import BRONZE, CONTEXT, GOLD, LAKEHOUSE, ROOT, SILVER, SOURCES

STEPS: list[dict[str, str]] = [
    {"id": "sources", "label": "Sources", "act": "0", "blurb": "Pre-Bronze source pack"},
    {"id": "bronze", "label": "Bronze", "act": "1", "blurb": "Raw landed tables"},
    {"id": "semantics", "label": "Semantics", "act": "1", "blurb": "LLM meanings + Judge"},
    {"id": "quality", "label": "Quality", "act": "1", "blurb": "What is broken in Bronze"},
    {"id": "hitl", "label": "HITL Gaps", "act": "1", "blurb": "Human guidance on gaps"},
    {"id": "connect", "label": "Connect", "act": "1", "blurb": "Link entities across sources"},
    {"id": "silver", "label": "Silver", "act": "1", "blurb": "Clean tables + semantics"},
    {"id": "insights", "label": "Insights", "act": "2", "blurb": "AI investigates Silver"},
    {"id": "gold", "label": "Gold", "act": "2", "blurb": "AI-ready relational mart"},
    {"id": "ask", "label": "Ask", "act": "2", "blurb": "Guarded Text-to-SQL"},
    {"id": "trust", "label": "Trust", "act": "side", "blurb": "Judge · eval · guards"},
]
STEP_IDS = [s["id"] for s in STEPS]
ACTIVATION_STEPS = [s for s in STEP_IDS if s != "trust"]
STAGE_LABELS = {"ingest": "Ingestion", "semantics": "Context + Judge", "quality": "Quality scan",
                "connect": "Entity resolution", "silver": "Silver transform", "discover": "Discovery",
                "gold": "Gold build"}

# Continue on a step runs `stage` (if any) and moves to `next`; `gate` must be clear first.
CONTINUE: dict[str, dict[str, Any]] = {
    "sources": {"stage": "ingest", "next": "bronze"},
    "bronze": {"stage": "semantics", "next": "semantics"},
    "semantics": {"stage": "quality", "next": "quality"},
    "quality": {"stage": None, "next": "hitl"},
    "hitl": {"stage": "connect", "next": "connect", "gate": ("semantic", "data")},
    "connect": {"stage": "silver", "next": "silver", "gate": ("entity",)},
    "silver": {"stage": "discover", "next": "insights"},
    "insights": {"stage": "gold", "next": "gold"},
    "gold": {"stage": None, "next": "ask"},
    "ask": {"stage": None, "next": "trust"},
    "trust": {"stage": None, "next": None},
}
# Stage whose output each step displays.
STEP_STAGE = {"bronze": "ingest", "semantics": "semantics", "quality": "quality", "connect": "connect",
              "silver": "silver", "insights": "discover", "gold": "gold"}
DECISION_FILES = ("corrections.jsonl", DATA_GAPS, "entity_gaps.json", "mappings.json")
UPSTREAM = ["ingest", "semantics", "quality", "connect", "silver", "discover", "gold"]
WALK_SESSION_PATH = CONTEXT / "walk_session.json"


def _dirs(root) -> list[str]:
    return sorted(p.name for p in root.iterdir() if p.is_dir()) if root.exists() else []


def bronze_tables() -> list[str]:
    return [t for t in _dirs(BRONZE) if not t.startswith("ref_")]


def silver_tables() -> list[str]:
    return _dirs(SILVER)


def gold_tables() -> list[str]:
    return _dirs(GOLD)


def stage_runs() -> dict[str, float]:
    return ContextStore().load_json(STAGE_RUNS, {}) or {}


def progress() -> dict[str, Any]:
    store = ContextStore()
    done = {
        "ingest": bool(bronze_tables()),
        "semantics": bool(store.load_proposals()),
        "quality": store.exists(QUALITY_REPORT),
        "connect": (SILVER / "master_vessel").exists(),
        "silver": store.exists(CLEANUP_REPORT),
        "discover": store.exists(INSIGHTS),
        "gold": bool(load_gold_catalog().get("materialized")),
    }
    return {"done": done, "gaps": open_gap_counts()}


def step_unlocked(step: str, prog: dict[str, Any]) -> bool:
    if step in {"sources", "trust"}:
        return True
    d = prog["done"]
    return {
        "bronze": d["ingest"], "semantics": d["semantics"], "quality": d["quality"], "hitl": d["quality"],
        "connect": d["connect"], "silver": d["silver"], "insights": d["discover"], "gold": d["gold"],
        "ask": d["gold"] or d["silver"],
    }.get(step, False)


def gate_open(step: str, prog: dict[str, Any]) -> tuple[bool, str]:
    kinds = CONTINUE.get(step, {}).get("gate") or ()
    open_n = {k: prog["gaps"][k] for k in kinds if prog["gaps"][k]}
    if not open_n:
        return True, ""
    return False, " · ".join(f"{n} {k} gap(s)" for k, n in open_n.items()) + " still open"


def _decisions_mtime() -> float:
    times = [(CONTEXT / f).stat().st_mtime for f in DECISION_FILES if (CONTEXT / f).exists()]
    return max(times) if times else 0.0


def stale(stage: str) -> bool:
    """A stage is stale if an upstream stage (or a HITL decision it consumes) is newer."""
    runs = stage_runs()
    t = runs.get(stage)
    if t is None:
        return False
    if any(runs.get(s, 0) > t for s in UPSTREAM[:UPSTREAM.index(stage)]):
        return True
    if stage in {"connect", "silver"} and _decisions_mtime() > t:
        return True
    ins = CONTEXT / INSIGHTS
    return stage == "gold" and ins.exists() and ins.stat().st_mtime > t


def continue_plan(step: str, prog: dict[str, Any]) -> dict[str, Any]:
    """What Continue does from `step`: run the next stage only when missing or stale."""
    action = dict(CONTINUE.get(step, {"stage": None, "next": None}))
    stage = action.get("stage")
    action["will_run"] = bool(stage) and (not step_unlocked(action["next"], prog) or stale(stage))
    action["can_rerun"] = bool(stage) and step_unlocked(action["next"], prog)
    action["stage_label"] = STAGE_LABELS.get(stage) if stage else None
    ok, why = gate_open(step, prog)
    action.update(gate_ok=ok, gate_reason=why)
    action.pop("gate", None)
    return action


def infer_resume_step(prog: dict[str, Any]) -> str:
    for step in reversed(ACTIVATION_STEPS):
        if step != "ask" and step_unlocked(step, prog):
            if step == "quality" and (prog["gaps"]["semantic"] or prog["gaps"]["data"]):
                return "hitl"
            return step
    return "sources"


def activation_progress(prog: dict[str, Any]) -> dict[str, Any]:
    d, g = prog["done"], prog["gaps"]
    milestones = [
        d["ingest"], d["semantics"], d["quality"],
        d["quality"] and not g["semantic"] and not g["data"],
        d["connect"] and not g["entity"], d["silver"], d["discover"], d["gold"],
    ]
    n = sum(bool(m) for m in milestones)
    return {"pct": int(round(100 * n / len(milestones))), "done": n, "total": len(milestones)}


def status() -> dict[str, Any]:
    prog = progress()
    steps = []
    for s in STEPS:
        stage = STEP_STAGE.get(s["id"])
        steps.append({**s, "unlocked": step_unlocked(s["id"], prog),
                      "stale": bool(stage and prog["done"].get(stage) and stale(stage)),
                      "continue": continue_plan(s["id"], prog)})
    return {"steps": steps, "progress": activation_progress(prog), "gaps": prog["gaps"], "done": prog["done"],
            "resume_step": load_walk_step() or infer_resume_step(prog)}


def save_walk_session(walk_step: str) -> None:
    CONTEXT.mkdir(parents=True, exist_ok=True)
    WALK_SESSION_PATH.write_text(json.dumps({"walk_step": walk_step}), encoding="utf-8")


def load_walk_step() -> str | None:
    if not WALK_SESSION_PATH.exists():
        return None
    try:
        step = json.loads(WALK_SESSION_PATH.read_text(encoding="utf-8")).get("walk_step")
    except Exception:  # noqa: BLE001
        return None
    return step if step in STEP_IDS else None


def hard_reset() -> dict[str, Any]:
    """Reset activation after Bronze. Keeps data/ and Bronze; Silver/Gold/context move to a backup."""
    data_root = (ROOT / "data").resolve()
    backup = LAKEHOUSE / f"_reset_backup_{time.strftime('%Y%m%d-%H%M%S')}"
    backup.mkdir(parents=True, exist_ok=True)
    for root in (SILVER, GOLD, CONTEXT):
        assert not str(root.resolve()).startswith(str(data_root)), root
        if root.exists():
            shutil.move(str(root), str(backup / root.name))
        root.mkdir(parents=True, exist_ok=True)
    step = "bronze" if bronze_tables() else "sources"
    save_walk_session(step)
    return {"walk_step": step, "bronze_tables": len(bronze_tables()), "backup": str(backup.relative_to(ROOT))}


def source_inventory() -> list[dict[str, Any]]:
    landed = bronze_tables()
    rows = []
    for p in _dirs(SOURCES):
        if p.startswith("."):
            continue
        files = [f for f in (SOURCES / p).rglob("*") if f.is_file() and not f.name.startswith(".")]
        rows.append({
            "source": p,
            "files": len(files),
            "formats": ", ".join(sorted({f.suffix.lstrip(".") or "—" for f in files})),
            "size_mb": round(sum(f.stat().st_size for f in files) / 1e6, 1),
            "bronze": [t for t in landed if t.startswith(p.rstrip("s")) or p in t],
        })
    return rows


def cleanup_rows() -> list[dict[str, Any]]:
    report = ContextStore().load_json(CLEANUP_REPORT, {}) or {}
    rows = []
    for table, r in report.items():
        fixes = {k: v for k, v in r.items()
                 if k not in {"source", "bronze_rows", "silver_rows", "issues_remaining"}
                 and isinstance(v, (int, float)) and v}
        rows.append({
            "table": table, "source": r.get("source"), "bronze_rows": r.get("bronze_rows"),
            "silver_rows": r.get("silver_rows"), "fixes": fixes,
            "remaining": {k: v for k, v in (r.get("issues_remaining") or {}).items() if v},
        })
    return rows


def journey_summary() -> list[dict[str, Any]]:
    store = ContextStore()
    q = (store.load_json(QUALITY_REPORT, {}) or {}).get("summary") or {}
    sem = store.load_semantics()
    tables = (sem.get("tables") or {}).values()
    fields = sum(len(t.get("columns") or []) for t in tables)
    pii = sum(1 for t in tables for c in t.get("columns") or [] if c.get("pii"))
    human = sum(1 for name in (DATA_GAPS, "entity_gaps.json") for g in store.load_gaps(name)
                if g.get("status") == "resolved")
    catalog = load_gold_catalog()
    return [
        {"aspect": "Raw tables", "bronze": len(bronze_tables()), "activated": f"{len(silver_tables())} Silver tables"},
        {"aspect": "Duplicate rows", "bronze": f"{q.get('duplicates', 0):,}", "activated": "removed in Silver"},
        {"aspect": "Field meanings", "bronze": "undocumented", "activated": f"{fields} fields in semantic layer"},
        {"aspect": "PII", "bronze": "untracked", "activated": f"{pii} columns tagged and masked"},
        {"aspect": "Unresolved ports / codes",
         "bronze": f"{q.get('unresolved_port_values', 0)} ports · {q.get('unmapped_codes', 0)} codes",
         "activated": f"{human} gaps resolved with human guidance"},
        {"aspect": "Cross-source links", "bronze": "siloed keys",
         "activated": f"{len(sem.get('relationships') or [])} relationships"},
        {"aspect": "AI-ready Gold", "bronze": "—", "activated": f"{len(catalog.get('materialized') or [])} tables"},
    ]


def silver_model() -> dict[str, Any]:
    """Silver relationship graph as nodes/edges."""
    rels = ContextStore().load_semantics().get("relationships") or []
    nodes: dict[str, dict[str, Any]] = {}
    edges = []
    for i, r in enumerate(rels):
        a_t, a_c = r["from"].rsplit(".", 1)
        b_t, b_c = r["to"].rsplit(".", 1)
        for t in (a_t, b_t):
            nodes.setdefault(t, {"id": t, "kind": "master" if t.startswith("master_") else "table"})
        edges.append({"id": f"e{i}", "source": a_t, "target": b_t, "label": a_c if a_c == b_c else f"{a_c} → {b_c}",
                      "type": r.get("type")})
    return {"nodes": list(nodes.values()), "edges": edges}


def gold_model() -> dict[str, Any]:
    """Gold dims → facts/aggregates graph as nodes/edges."""
    catalog = load_gold_catalog()
    nodes = [{"id": t["table_name"].replace("gold.", ""), "kind": t.get("kind")} for t in catalog.get("tables") or []]
    edges = []
    for i, r in enumerate(catalog.get("relationships") or []):
        src, key = r["from"].replace("gold.", "").rsplit(".", 1)
        dim = r["to"].replace("gold.", "").rsplit(".", 1)[0]
        edges.append({"id": f"e{i}", "source": dim, "target": src, "label": key})
    return {"nodes": nodes, "edges": edges}
