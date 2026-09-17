"""
core/jobs.py
────────────
Background job dispatch logic.
Keeps main.py clean — routers just call dispatch_job().

Two execution modes:
  async  → _async_job:    uses asyncio.gather for concurrent steps
  sync   → _blocking_job: plain blocking fn, wrapped in asyncio.to_thread
"""

import asyncio
import time
from datetime import datetime
from typing import Any

from .store import broadcast_ws, job_status_store

from fastapi_app.logger_engine import logger


# ── Async variant ─────────────────────────────────────────────────────────────

async def _step_fetch_data(payload: dict) -> str:
    """Simulates async I/O — DB read, HTTP call, etc."""
    await asyncio.sleep(1)
    return f"fetched:{payload}"


async def _step_compute(payload: dict) -> int:
    """Simulates async computation."""
    await asyncio.sleep(0.5)
    return len(str(payload))


async def _async_job(job_id: str, payload: dict) -> None:
    """
    Runs two async steps concurrently via asyncio.gather.
    Both steps run at the same time — total time = max(step times), not sum.
    """
    job_status_store[job_id]["status"]     = "running"
    job_status_store[job_id]["started_at"] = datetime.utcnow()
    try:
        result_a, result_b = await asyncio.gather(
            _step_fetch_data(payload),
            _step_compute(payload),
        )
        job_status_store[job_id].update(
            status="done",
            result={"fetch": result_a, "compute": result_b},
            finished_at=datetime.utcnow(),
        )
        await broadcast_ws(f"job:{job_id}:done")
        logger.info(f"✅ Async job {job_id} done")
    except Exception as exc:
        logger.error(f"💥 Async job {job_id} failed: {exc}")
        job_status_store[job_id].update(
            status="failed",
            result=str(exc),
            finished_at=datetime.utcnow(),
        )


# ── Sync variant ──────────────────────────────────────────────────────────────

def _blocking_job(job_id: str, payload: dict) -> None:
    """
    Purely synchronous — safe to use blocking libs (pandas, PIL, boto3 sync, etc.).
    NEVER call this directly from an async route — use asyncio.to_thread().
    """
    job_status_store[job_id]["status"]     = "running"
    job_status_store[job_id]["started_at"] = datetime.utcnow()
    try:
        time.sleep(2)    # simulates a blocking operation
        job_status_store[job_id].update(
            status="done",
            result=f"sync-processed:{payload}",
            finished_at=datetime.utcnow(),
        )
        logger.info(f"✅ Sync job {job_id} done")
    except Exception as exc:
        logger.error(f"💥 Sync job {job_id} failed: {exc}")
        job_status_store[job_id].update(
            status="failed",
            result=str(exc),
            finished_at=datetime.utcnow(),
        )


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def dispatch_job(job_id: str, payload: dict[str, Any], sync: bool) -> None:
    """
    Single entry point called by BackgroundTasks.

    sync=False  →  async path  (asyncio.gather)
    sync=True   →  blocking path wrapped in asyncio.to_thread
                   (runs in thread pool, event loop stays free)
    """
    if sync:
        await asyncio.to_thread(_blocking_job, job_id, payload)
    else:
        await _async_job(job_id, payload)