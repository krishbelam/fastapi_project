"""
routers/jobs.py
───────────────
Background job submission and status polling.
"""

import uuid

from fastapi import APIRouter, BackgroundTasks, HTTPException

from fastapi_app.core import dispatch_job, job_status_store
from fastapi_app.models import JobRequest, JobStatusResponse

from fastapi_app.logger_engine import logger

router = APIRouter(prefix="/jobs", tags=["Background Jobs"])


@router.post("/", response_model=JobStatusResponse, status_code=202)
async def submit_job(body: JobRequest, background_tasks: BackgroundTasks):
    """
    Returns 202 immediately. Job runs after response is sent.
    Poll GET /jobs/{job_id} for status.
    Connect to WS /ws to get pushed notification on completion.
    """
    job_id = str(uuid.uuid4())
    job_status_store[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "result": None,
        "started_at": None,
        "finished_at": None,
    }
    background_tasks.add_task(dispatch_job, job_id, body.payload, body.sync)
    logger.info(f"📋 Job {job_id} queued  sync={body.sync}")
    return job_status_store[job_id]


@router.get("/", response_model=list[JobStatusResponse])
async def list_jobs():
    return list(job_status_store.values())


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    job = job_status_store.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job
