"""TCP connectivity check for Twingate-routed Aurora connection."""
import socket
import time
import threading
from app.state import repository as repo
from app.notifications.telegram import notify

_monitor_thread: threading.Thread | None = None
_stop_event = threading.Event()


def check_connectivity() -> tuple[bool, int | None, str | None]:
    """Try TCP connect to Aurora host:port. Returns (success, latency_ms, error)."""
    s = repo.effective_settings()
    try:
        start = time.monotonic()
        with socket.create_connection((s["aurora_host"], s["aurora_port"]), timeout=10):
            pass
        latency_ms = int((time.monotonic() - start) * 1000)
        return True, latency_ms, None
    except Exception as e:
        return False, None, str(e)


def _monitor_loop(on_failure=None):
    # Run immediately on start, then on interval
    while not _stop_event.is_set():
        ok, latency, error = check_connectivity()
        repo.log_twingate_check(ok, latency, error)

        if not ok:
            notify(f"⚠️ *Twingate / Aurora unreachable*\n`{error}`\nSync paused if running.")
            if on_failure:
                on_failure()

        s = repo.effective_settings()
        _stop_event.wait(s["twingate_check_interval"])


def start_monitor(on_failure=None):
    global _monitor_thread, _stop_event
    _stop_event.clear()
    _monitor_thread = threading.Thread(
        target=_monitor_loop,
        args=(on_failure,),
        daemon=True,
        name="twingate-monitor"
    )
    _monitor_thread.start()


def stop_monitor():
    _stop_event.set()
