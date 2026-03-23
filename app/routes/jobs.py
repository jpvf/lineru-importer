from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
from fastapi.params import Body
from app.state import repository as repo
from app.sync import worker

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@router.get("")
def list_jobs(limit: int = 20, offset: int = 0):
    return {"jobs": repo.get_jobs(limit, offset)}


@router.get("/current")
def current_job():
    job = repo.get_current_job()
    if not job:
        return {"job": None}
    logs = repo.get_job_table_logs(job["id"])
    return {"job": job, "tables": logs}


@router.post("")
def start_job(body: dict = Body(default={})):
    if worker.is_running():
        raise HTTPException(409, "A job is already running")

    job_id = repo.create_job(trigger_reason=body.get("trigger_reason", "manual"))
    worker.start(job_id)
    return {"job_id": job_id, "status": "started"}


@router.get("/{job_id}")
def get_job(job_id: int):
    job = repo.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    logs = repo.get_job_table_logs(job_id)
    return {"job": job, "tables": logs}


@router.post("/{job_id}/pause")
def pause_job(job_id: int):
    _assert_active(job_id)
    worker.pause()
    repo.update_job(job_id, status="paused", paused_at=datetime.now(timezone.utc).isoformat())
    return {"status": "paused"}


@router.post("/{job_id}/resume")
def resume_job(job_id: int):
    job = repo.get_job(job_id)
    if not job or job["status"] != "paused":
        raise HTTPException(400, "Job is not paused")
    worker.resume()
    repo.update_job(job_id, status="running", resumed_at=datetime.now(timezone.utc).isoformat())
    return {"status": "running"}


@router.post("/{job_id}/cancel")
def cancel_job(job_id: int):
    _assert_active(job_id)
    worker.cancel()
    return {"status": "cancelling"}


def _assert_active(job_id: int):
    if not worker.is_running() or worker.current_job_id() != job_id:
        raise HTTPException(400, "Job is not active")
