"""Gated pre-Bronze → Gold walkthrough step definitions and readiness checks."""
from __future__ import annotations

import json
from typing import Any

from maritimeiq.context_store.store import ContextStore
from maritimeiq.ui.components.hitl_queue import open_review_items
from maritimeiq.utils.paths import BRONZE, CONTEXT, GOLD, ROOT, SILVER, SOURCES

STEPS: list[dict[str, str]] = [
    {"id": "sources", "label": "Sources", "act": "0", "blurb": "Pre-Bronze source pack"},
    {"id": "bronze", "label": "Bronze", "act": "1", "blurb": "Ingest messy raw tables"},
    {"id": "semantics", "label": "Semantics", "act": "1", "blurb": "LLM meanings + Judge"},
    {"id": "hitl", "label": "HITL Gaps", "act": "1", "blurb": "Human guidance on gaps"},
    {"id": "connect", "label": "Connect", "act": "1", "blurb": "Entity resolution masters"},
    {"id": "silver", "label": "Silver", "act": "1", "blurb": "Clean governed tables"},
    {"id": "gold", "label": "Gold Build", "act": "2", "blurb": "Relational mart from Silver"},
    {"id": "ask", "label": "Ask", "act": "2", "blurb": "Text-to-SQL cascade"},
    {"id": "trust", "label": "Trust", "act": "side", "blurb": "Judge · eval · guards"},
]

STEP_IDS = [s["id"] for s in STEPS]

WALK_SESSION_PATH = CONTEXT / "walk_session.json"


def _bronze_tables() -> list[str]:
    if not BRONZE.exists():
        return []
    return sorted(p.name for p in BRONZE.iterdir() if p.is_dir())


def _silver_tables() -> list[str]:
    if not SILVER.exists():
        return []
    return sorted(p.name for p in SILVER.iterdir() if p.is_dir())


def _gold_tables() -> list[str]:
    if not GOLD.exists():
        return []
    return sorted(p.name for p in GOLD.iterdir() if p.is_dir())


def load_gold_catalog() -> dict[str, Any]:
    path = CONTEXT / "gold_catalog.yaml"
    if not path.exists():
        return {"tables": [], "primary_view": None}
    try:
        import yaml

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {"tables": []}
    except Exception:  # noqa: BLE001
        return {"tables": []}


def source_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    manifest_path = ROOT / "data" / "hackathon" / "manifest.json"
    meta: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            meta = (json.loads(manifest_path.read_text(encoding="utf-8")).get("sources") or {})
        except Exception:  # noqa: BLE001
            meta = {}
    if SOURCES.exists():
        for p in sorted(SOURCES.iterdir()):
            if not p.is_dir() or p.name.startswith("."):
                continue
            files = [f for f in p.rglob("*") if f.is_file()]
            m = meta.get(p.name) or {}
            rows.append(
                {
                    "Source": p.name,
                    "Files": len(files),
                    "Manifest Rows": m.get("rows", "—") if isinstance(m, dict) else "—",
                    "In Bronze": "Yes" if p.name in _bronze_tables() else "No",
                }
            )
    return rows


def silver_ready_checklist() -> dict[str, Any]:
    store = ContextStore()
    sem_tables = store.load_semantics().get("tables", {}) or {}
    bronze = _bronze_tables()
    silver = _silver_tables()
    hitl_open = len(open_review_items())
    has_masters = any(
        t.startswith("master_") or t.endswith("_aliases") or "record_map" in t for t in silver
    )
    checks = [
        {"id": "bronze", "label": "Bronze tables ingested", "ok": len(bronze) >= 3},
        {"id": "semantics", "label": "Semantic layer approved for key tables", "ok": len(sem_tables) >= 1},
        {"id": "hitl", "label": "No open HITL semantic gaps", "ok": hitl_open == 0},
        {"id": "er", "label": "Entity masters present", "ok": has_masters},
        {
            "id": "silver",
            "label": "Silver fact tables present",
            "ok": any(t in silver for t in ("port_agent_events", "ais_positions", "container_events")),
        },
    ]
    return {
        "ok": all(c["ok"] for c in checks),
        "checks": checks,
        "bronze_n": len(bronze),
        "silver_n": len(silver),
        "gold_n": len(_gold_tables()),
        "sem_tables": len(sem_tables),
        "hitl_open": hitl_open,
    }


def step_unlocked(step_id: str, phase: str | None) -> bool:
    """Whether the user may navigate to this step (Trust always; others by progress)."""
    if step_id == "trust":
        return True
    if step_id == "sources":
        return True
    phase = phase or "idle"
    order = {
        "idle": 0,
        "bronze_done": 1,
        "context_done": 2,
        "awaiting_hitl": 2,
        "er_done": 4,
        "silver_done": 5,
        "gold_done": 6,
        "insight_done": 7,
    }
    rank = order.get(phase, 0)
    need = {
        "bronze": 0,
        "semantics": 1,
        "hitl": 2,
        "connect": 3,
        "silver": 4,
        "gold": 5,
        "ask": 6,
    }
    # Allow current + completed steps; also allow hitl whenever context ran
    idx = STEP_IDS.index(step_id) if step_id in STEP_IDS else 0
    if step_id == "hitl" and rank >= 2:
        return True
    if step_id == "ask":
        return silver_ready_checklist()["ok"] and (
            rank >= 5 or bool(load_gold_catalog().get("tables")) or len(_gold_tables()) > 0 or rank >= 6
        )
    if step_id == "gold":
        return silver_ready_checklist()["ok"] or rank >= 5
    # Map step to minimum phase rank
    min_rank = need.get(step_id, 0)
    if step_id == "bronze":
        return True
    if step_id == "semantics":
        return rank >= 1 or len(_bronze_tables()) > 0
    if step_id == "connect":
        return rank >= 3 or (rank >= 2 and len(open_review_items()) == 0)
    if step_id == "silver":
        return rank >= 4 or (SILVER.exists() and any(SILVER.iterdir()))
    return rank >= min_rank


def continue_action(step_id: str) -> dict[str, Any]:
    """What Continue should do from this step: pipeline through, or navigate only."""
    mapping = {
        "sources": {"next": "bronze", "run": None},
        "bronze": {"next": "semantics", "run": "bronze"},
        "semantics": {"next": "hitl", "run": "context"},
        "hitl": {"next": "connect", "run": None, "require_hitl_clear": True},
        "connect": {"next": "silver", "run": "er"},
        "silver": {"next": "gold", "run": "silver"},
        "gold": {"next": "ask", "run": "gold"},
        "ask": {"next": "trust", "run": None},
        "trust": {"next": "trust", "run": None},
    }
    return mapping.get(step_id, {"next": "sources", "run": None})


def save_walk_session(walk_step: str, pipeline_state: dict[str, Any] | None = None, **extra: Any) -> None:
    CONTEXT.mkdir(parents=True, exist_ok=True)
    payload = {
        "walk_step": walk_step,
        "pipeline_state": pipeline_state or {},
        "hitl_resolved_count": extra.get("hitl_resolved_count", 0),
    }
    WALK_SESSION_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def load_walk_session() -> dict[str, Any] | None:
    if not WALK_SESSION_PATH.exists():
        return None
    try:
        return json.loads(WALK_SESSION_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def infer_resume_step(phase: str | None = None) -> tuple[str, str]:
    """Return (walk_step, phase) from disk progress when session is missing."""
    ready = silver_ready_checklist()
    open_hitl = len(open_review_items())
    gold_n = len(_gold_tables())
    silver_n = len(_silver_tables())
    bronze_n = len(_bronze_tables())
    sem_n = ready["sem_tables"]

    if phase in {"insight_done", "gold_done"} or gold_n:
        return "ask" if gold_n else "gold", phase or ("gold_done" if gold_n else "silver_done")
    if phase == "silver_done" or (silver_n and ready["ok"]):
        return "gold" if ready["ok"] else "silver", phase or "silver_done"
    if phase == "er_done" or (silver_n and any(t.startswith("master_") for t in _silver_tables())):
        return "silver", phase or "er_done"
    if phase == "awaiting_hitl" or (sem_n and open_hitl):
        return "hitl", "awaiting_hitl"
    if phase == "context_done" or (sem_n and not open_hitl):
        return "connect" if sem_n else "semantics", phase or ("context_done" if sem_n else "bronze_done")
    if phase == "bronze_done" or bronze_n:
        return "semantics" if bronze_n else "bronze", phase or ("bronze_done" if bronze_n else "idle")
    return "sources", phase or "idle"


def hard_reset_walkthrough() -> dict[str, Any]:
    """Reset activation *after* Bronze.

    Never touches ``data/`` (source pack). Keeps Bronze Delta tables.
    Clears Silver / Gold / context artifacts so Semantics → Gold can re-run.
    Resumes at the Semantics step when Bronze is present, else Sources.
    """
    import shutil

    # Safety: never delete project source data
    data_root = ROOT / "data"
    assert not str(SILVER).startswith(str(data_root))
    assert not str(GOLD).startswith(str(data_root))
    assert not str(CONTEXT).startswith(str(data_root))

    if WALK_SESSION_PATH.exists():
        WALK_SESSION_PATH.unlink()

    # Wipe only post-Bronze activation layers — keep BRONZE intact.
    for root in (SILVER, GOLD, CONTEXT):
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)

    bronze_n = len(_bronze_tables())
    if bronze_n:
        step, phase = "semantics", "bronze_done"
    else:
        step, phase = "sources", "idle"
    save_walk_session(step, {"phase": phase}, hitl_resolved_count=0)
    return {"walk_step": step, "phase": phase, "bronze_tables": bronze_n}


# Core activation steps (Trust is side-path, not in %).
ACTIVATION_STEPS = [s["id"] for s in STEPS if s["id"] != "trust"]


def activation_progress(walk_step: str, hitl_resolved: int = 0) -> dict[str, Any]:
    """Overall walkthrough progress + HITL sub-progress."""
    step = walk_step if walk_step in ACTIVATION_STEPS else "sources"
    idx = ACTIVATION_STEPS.index(step)
    # Count current step as in-progress: completed = idx (sources=0% done of journey start)
    completed = idx
    total = len(ACTIVATION_STEPS)
    pct = int(round(100 * completed / total)) if total else 0
    # Nudge: on ask (last activation step) show 100% if gold exists
    if step == "ask" and (_gold_tables() or load_gold_catalog().get("materialized")):
        pct = 100
        completed = total
    open_hitl = len(open_review_items())
    hitl_total = open_hitl + max(0, int(hitl_resolved))
    hitl_pct = (
        int(round(100 * hitl_resolved / hitl_total)) if hitl_total else (100 if step not in {"semantics", "hitl"} else 0)
    )
    return {
        "pct": min(100, pct),
        "completed": completed,
        "total": total,
        "step": step,
        "hitl_open": open_hitl,
        "hitl_resolved": hitl_resolved,
        "hitl_pct": hitl_pct,
        "caption": (
            f"Activation {min(100, pct)}% ({completed}/{total}) · "
            f"HITL {open_hitl} open"
            + (f" · {hitl_resolved} resolved this session" if hitl_resolved else "")
        ),
    }


def cleanup_summary() -> dict[str, Any]:
    """Messy pre-Bronze/Bronze vs activated Silver/Gold snapshot."""
    store = ContextStore()
    sem = store.load_semantics().get("tables", {}) or {}
    fields = 0
    pii = 0
    for meta in sem.values():
        for col in meta.get("columns") or []:
            fields += 1
            if col.get("pii"):
                pii += 1
    sources = source_inventory()
    bronze = _bronze_tables()
    silver = _silver_tables()
    gold = _gold_tables()
    masters = [t for t in silver if t.startswith("master_") or "alias" in t or "record_map" in t]
    return {
        "rows": [
            {"Aspect": "Source folders", "Before (messy)": len(sources) or "—", "After (activated)": len(bronze) or "—"},
            {"Aspect": "Raw Bronze tables", "Before (messy)": len(bronze) or "—", "After (activated)": "Governed via Silver"},
            {"Aspect": "Field semantics", "Before (messy)": "Unknown / undocumented", "After (activated)": f"{fields} fields approved"},
            {"Aspect": "PII tags", "Before (messy)": "Untracked", "After (activated)": f"{pii} columns tagged + masked"},
            {"Aspect": "HITL gaps", "Before (messy)": "Unresolved meanings", "After (activated)": f"{len(open_review_items())} open"},
            {"Aspect": "Entity masters", "Before (messy)": "Siloed keys", "After (activated)": f"{len(masters)} link tables"},
            {"Aspect": "Silver tables", "Before (messy)": "—", "After (activated)": len(silver) or "—"},
            {"Aspect": "Gold mart tables", "Before (messy)": "—", "After (activated)": len(gold) or "—"},
        ],
        "narrative": (
            f"Started from {len(sources) or 'raw'} source folder(s) landed as {len(bronze)} messy Bronze tables. "
            f"Semantics cover {fields} fields ({pii} PII). Silver has {len(silver)} tables "
            f"({len(masters)} masters). Gold mart: {len(gold)} tables."
        ),
    }


def gold_dim_model_mermaid(proposals: list[dict[str, Any]] | None = None) -> str:
    """Mermaid ER-style sketch of Gold dims → facts → overview."""
    catalog = load_gold_catalog()
    props = proposals or catalog.get("tables") or []
    dims = [p for p in props if p.get("kind") == "dimension"]
    facts = [p for p in props if p.get("kind") in {"fact", "aggregate"}]
    views = [p for p in props if p.get("kind") == "view"]
    lines = ["flowchart LR"]
    for d in dims:
        nid = d["table_name"].replace("gold.", "").replace("-", "_")
        lines.append(f'  {nid}["{d["table_name"]}"]')
    for f in facts:
        nid = f["table_name"].replace("gold.", "").replace("-", "_")
        lines.append(f'  {nid}["{f["table_name"]}"]')
        for d in dims:
            dn = d["table_name"].replace("gold.", "").replace("-", "_")
            lines.append(f"  {dn} --> {nid}")
    for v in views:
        nid = v["table_name"].replace("gold.", "").replace("-", "_")
        lines.append(f'  {nid}["{v["table_name"]}"]')
        for f in facts[:3]:
            fn = f["table_name"].replace("gold.", "").replace("-", "_")
            lines.append(f"  {fn} --> {nid}")
    if len(lines) == 1:
        lines.append('  empty["No Gold catalog yet"]')
    return "```mermaid\n" + "\n".join(lines) + "\n```"
