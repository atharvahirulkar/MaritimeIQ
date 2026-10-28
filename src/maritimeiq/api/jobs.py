"""Single-worker job runner: one Spark stage at a time, state queryable while it runs."""
from __future__ import annotations

import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from maritimeiq.utils.logging_feed import FEED


class JobRunner:
    def __init__(self) -> None:
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mq-job")
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._active: str | None = None

    def active(self) -> dict[str, Any] | None:
        with self._lock:
            return dict(self._jobs[self._active]) if self._active else None

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def submit(self, kind: str, label: str, fn: Callable[[], Any]) -> dict[str, Any]:
        with self._lock:
            if self._active:
                raise RuntimeError(f"'{self._jobs[self._active]['label']}' is still running")
            job_id = uuid.uuid4().hex[:10]
            job = {"id": job_id, "kind": kind, "label": label, "status": "running",
                   "started": time.time(), "finished": None, "error": None, "result": None}
            self._jobs[job_id] = job
            self._active = job_id
        self._pool.submit(self._run, job_id, fn)
        return dict(job)

    def _run(self, job_id: str, fn: Callable[[], Any]) -> None:
        try:
            result = fn()
            update = {"status": "done", "result": result}
        except Exception as exc:  # noqa: BLE001
            FEED.emit("orchestrator", f"Job failed: {exc}", level="error", event_type="job_failed")
            update = {"status": "failed", "error": f"{exc}", "trace": traceback.format_exc()[-2000:]}
        with self._lock:
            self._jobs[job_id].update(update, finished=time.time())
            self._active = None
