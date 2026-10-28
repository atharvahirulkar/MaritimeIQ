"""MaritimeIQ API: walkthrough state, stage jobs, HITL decisions, Ask, Trust, live agent feed."""
from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from maritimeiq.utils.env import load_env

load_env()

from maritimeiq.agents.discovery import INSIGHTS
from maritimeiq.agents.gold_builder import load_gold_catalog
from maritimeiq.agents.llm import LLM
from maritimeiq.agents.orchestrator import STAGES, Orchestrator
from maritimeiq.agents.quality import QUALITY_REPORT
from maritimeiq.api.jobs import JobRunner
from maritimeiq.context_store.store import ContextStore
from maritimeiq.governance.guard import GUARD
from maritimeiq.utils.logging_feed import FEED
from maritimeiq.utils.paths import BRONZE, CONTEXT, GOLD, ROOT, SILVER
from maritimeiq.walkthrough import hitl, state

WEB_DIST = ROOT / "web" / "dist"
LAYERS = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD}

app = FastAPI(title="MaritimeIQ")
jobs = JobRunner()
_orch: Orchestrator | None = None
_orch_lock = threading.Lock()
_count_cache: dict[tuple[str, str, float], int] = {}


def orch() -> Orchestrator:
    global _orch
    with _orch_lock:
        if _orch is None:
            _orch = Orchestrator()
        return _orch


def _safe(obj: Any) -> Any:
    return json.loads(json.dumps(obj, default=str))


# --- walkthrough ------------------------------------------------------------------
@app.get("/api/status")
def get_status() -> dict[str, Any]:
    return {**state.status(), "job": jobs.active(), "llm": LLM.available}


class Session(BaseModel):
    walk_step: str


@app.put("/api/session")
def put_session(body: Session) -> dict[str, Any]:
    if body.walk_step not in state.STEP_IDS:
        raise HTTPException(400, f"unknown step {body.walk_step}")
    state.save_walk_session(body.walk_step)
    return {"ok": True}


@app.post("/api/stages/{stage}/run")
def run_stage(stage: str) -> dict[str, Any]:
    if stage not in STAGES:
        raise HTTPException(404, f"unknown stage {stage}")

    def work() -> dict[str, Any]:
        out = orch().run_stage(stage)
        return {"open_gaps": out.get("open_gaps")}

    try:
        return jobs.submit("stage", state.STAGE_LABELS[stage], work)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return _safe(job)


@app.get("/api/feed")
async def feed(request: Request, history: int = 60) -> StreamingResponse:
    async def events():
        cursor, backlog = FEED.since(0)
        for ev in backlog[-history:]:
            yield f"data: {json.dumps(ev, default=str)}\n\n"
        while not await request.is_disconnected():
            cursor, new = FEED.since(cursor)
            for ev in new:
                yield f"data: {json.dumps(ev, default=str)}\n\n"
            if not new:
                yield ": keepalive\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/reset")
def reset() -> dict[str, Any]:
    if jobs.active():
        raise HTTPException(409, "a stage is running")
    return state.hard_reset()


# --- data ---------------------------------------------------------------------------
@app.get("/api/sources")
def sources() -> list[dict[str, Any]]:
    return state.source_inventory()


@app.get("/api/tables/{layer}")
def tables(layer: str, counts: bool = False) -> list[dict[str, Any]]:
    if layer not in LAYERS:
        raise HTTPException(404, f"unknown layer {layer}")
    root = LAYERS[layer]
    names = state.bronze_tables() if layer == "bronze" else state._dirs(root)
    out = []
    for n in names:
        row: dict[str, Any] = {"name": n}
        if counts:
            stamp = (root / n / "_delta_log").stat().st_mtime if (root / n / "_delta_log").exists() else 0
            key = (layer, n, stamp)
            if key not in _count_cache:
                _count_cache[key] = orch().spark.read.format("delta").load(str(root / n)).count()
            row["rows"] = _count_cache[key]
        out.append(row)
    return out


@app.get("/api/tables/{layer}/{name}/sample")
def sample(layer: str, name: str, n: int = Query(15, le=200)) -> dict[str, Any]:
    if layer not in LAYERS:
        raise HTTPException(404, f"unknown layer {layer}")
    res = orch().gateway.lakehouse.read_sample(f"{layer}.{name}", n=n, agent="ui")
    return _safe(res)


@app.get("/api/semantics")
def semantics() -> dict[str, Any]:
    store = ContextStore()
    proposals = store.load_proposals()
    for res in proposals.values():
        for c in (res or {}).get("columns") or []:
            c.pop("profile", None)
    return _safe({"proposals": proposals, "learning": store.load_json("context_learning.json", {}) or {}})


@app.get("/api/quality")
def quality() -> dict[str, Any]:
    return ContextStore().load_json(QUALITY_REPORT, {}) or {}


@app.get("/api/cleanup")
def cleanup() -> dict[str, Any]:
    return {"rows": state.cleanup_rows(), "journey": state.journey_summary()}


@app.get("/api/silver/model")
def silver_model() -> dict[str, Any]:
    sem = ContextStore().load_semantics()
    return _safe({"graph": state.silver_model(), "relationships": sem.get("relationships") or [],
                  "tables": sem.get("silver_tables") or {}})


@app.get("/api/gold/catalog")
def gold_catalog() -> dict[str, Any]:
    return _safe({**load_gold_catalog(), "graph": state.gold_model()})


# --- HITL ---------------------------------------------------------------------------
@app.get("/api/gaps")
def gaps(kinds: str = "semantic,data,entity") -> list[dict[str, Any]]:
    wanted = tuple(k for k in kinds.split(",") if k in hitl.KINDS)
    return hitl.open_items(wanted)


class Decision(BaseModel):
    id: str
    action: str
    value: str | None = None


class Decisions(BaseModel):
    decisions: list[Decision]


@app.post("/api/gaps/decisions")
def decide(body: Decisions) -> dict[str, Any]:
    items = {i["id"]: i for i in hitl.open_items()}
    results = []
    for d in body.decisions:
        item = items.get(d.id)
        res = hitl.apply_decision(orch(), item, d.action, d.value) if item else {"ok": False, "error": "gap is no longer open"}
        if res.get("ok"):
            FEED.emit("review", f"HITL {d.action} {item['kind']} gap {item['subject']}", event_type="hitl_decision",
                      gap=d.id, decision=d.action, value=d.value)
        results.append({"id": d.id, "ok": bool(res.get("ok")), "error": res.get("error")})
    return {"applied": sum(r["ok"] for r in results), "results": results}


# --- Act 2 --------------------------------------------------------------------------
@app.get("/api/insights")
def insights() -> dict[str, Any]:
    return _safe(ContextStore().load_json(INSIGHTS, {}) or {})


class Selection(BaseModel):
    ids: list[str]


@app.put("/api/insights/selection")
def set_selection(body: Selection) -> dict[str, Any]:
    orch().discovery.set_selection(body.ids)
    return {"ok": True}


class Question(BaseModel):
    question: str


@app.post("/api/ask")
def ask(body: Question) -> dict[str, Any]:
    if jobs.active():
        raise HTTPException(409, "a stage is running; ask again when it finishes")
    return _safe(orch().insight.ask(body.question))


# --- Trust --------------------------------------------------------------------------
@app.post("/api/guard/probe")
def probe(body: Question) -> dict[str, Any]:
    d = GUARD.check_adversarial_prompt(body.question, agent="insight")
    return {"allowed": d.allowed, "reason": d.reason}


@app.get("/api/judge")
def judge_summary() -> list[dict[str, Any]]:
    rows = []
    for table, res in ContextStore().load_proposals().items():
        cols = (res or {}).get("columns") or []
        if cols:
            rows.append({
                "table": table, "columns": len(cols),
                "llm_judged": sum(1 for c in cols if c.get("judge_source") == "llm"),
                "auto_approved": sum(1 for c in cols if c.get("status") == "auto_approved"),
                "human_resolved": sum(1 for c in cols if c.get("status") in {"human_approved", "rejected"}),
                "open": sum(1 for c in cols if c.get("status") == "needs_review"),
                "graded_wrong": sum(1 for c in cols if c.get("grade") == "wrong"),
            })
    return rows


@app.post("/api/eval")
def run_eval_job() -> dict[str, Any]:
    from maritimeiq.eval.harness import run_eval

    try:
        return jobs.submit("eval", "Eval harness", lambda: {"ok": bool(run_eval())})
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/eval/latest")
def eval_latest() -> dict[str, Any]:
    p = ROOT / ".eval_results" / "latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


@app.get("/api/audit")
def audit(limit: int = 60) -> list[dict[str, Any]]:
    p = CONTEXT / "audit_log.jsonl"
    if not p.exists():
        return []
    out = []
    for line in reversed(p.read_text(encoding="utf-8").strip().splitlines()[-limit:]):
        try:
            out.append(json.loads(line))
        except Exception:  # noqa: BLE001
            continue
    return out


# --- SPA ----------------------------------------------------------------------------
if WEB_DIST.exists():
    app.mount("/assets", StaticFiles(directory=WEB_DIST / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str) -> FileResponse:
        target = WEB_DIST / path
        return FileResponse(target if path and target.is_file() else WEB_DIST / "index.html")
