"""Background sync worker — single thread per job."""
import threading
from app.sync.engine import SyncEngine

_pause_event  = threading.Event()
_cancel_event = threading.Event()
_worker_thread: threading.Thread | None = None
_current_job_id: int | None = None


def start(job_id: int):
    global _worker_thread, _current_job_id
    _pause_event.clear()
    _cancel_event.clear()
    _current_job_id = job_id

    engine = SyncEngine(job_id, _pause_event, _cancel_event)
    _worker_thread = threading.Thread(target=engine.run, daemon=True, name=f"sync-job-{job_id}")
    _worker_thread.start()


def pause():
    _pause_event.set()


def resume():
    _pause_event.clear()


def cancel():
    _cancel_event.set()
    _pause_event.clear()  # unblock if paused so it can exit


def is_running() -> bool:
    return _worker_thread is not None and _worker_thread.is_alive()


def is_paused() -> bool:
    return _pause_event.is_set()


def current_job_id() -> int | None:
    return _current_job_id if is_running() else None
