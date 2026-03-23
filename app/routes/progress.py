from fastapi import APIRouter
from app.state import repository as repo
from app.sync import worker

router = APIRouter(prefix="/api/progress", tags=["progress"])


@router.get("")
def get_progress():
    job = repo.get_current_job()
    if not job:
        return {"job": None, "tables": [], "twingate": repo.get_last_twingate_check()}

    logs = repo.get_job_table_logs(job["id"])
    rows_done  = job.get("rows_done") or 0
    rows_total = job.get("rows_total") or 1
    pct = round(rows_done / rows_total * 100, 1) if rows_total else 0

    return {
        "job": {
            **job,
            "pct": pct,
            "is_paused": worker.is_paused(),
        },
        "tables": logs,
        "twingate": repo.get_last_twingate_check(),
    }
