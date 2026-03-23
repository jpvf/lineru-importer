"""Telegram bot command listener (long-polling)."""
import threading
import time
import logging
import httpx
from app.state import repository as repo

logger = logging.getLogger(__name__)

_stop_event = threading.Event()
_thread: threading.Thread | None = None


def _get_status_message() -> str:
    job = repo.get_current_job()
    twingate = repo.get_last_twingate_check()

    tg = "✅ Aurora OK"
    if twingate:
        if not twingate["success"]:
            tg = f"⚠️ Aurora unreachable: `{twingate['error']}`"
        elif twingate["latency_ms"]:
            tg = f"✅ Aurora OK ({twingate['latency_ms']}ms)"

    if not job:
        return f"💤 *No active sync job*\n{tg}"

    status_icons = {
        "running":   "🔄",
        "paused":    "⏸",
        "done":      "✅",
        "error":     "❌",
        "cancelled": "🛑",
    }
    icon = status_icons.get(job["status"], "❓")
    rows_done  = job.get("rows_done") or 0
    rows_total = job.get("rows_total") or 0
    pct        = job.get("pct") or 0
    tables_done  = job.get("tables_done") or 0
    tables_total = job.get("tables_total") or 0

    lines = [
        f"{icon} *Job #{job['id']}* — {job['status'].upper()}",
        f"📊 {rows_done:,} / {rows_total:,} rows ({pct}%)",
        f"🗂 {tables_done} / {tables_total} tables",
        tg,
    ]

    if job.get("error_message"):
        lines.append(f"⚠️ `{job['error_message'][:200]}`")

    # Current table
    logs = repo.get_job_table_logs(job["id"])
    running_table = next((t for t in logs if t["status"] == "running"), None)
    if running_table:
        t_rows = running_table.get("rows_synced") or 0
        t_total = running_table.get("rows_total") or 0
        t_pct = round(t_rows / t_total * 100, 1) if t_total else 0
        lines.append(f"⚙️ `{running_table['table_name']}`: {t_rows:,}/{t_total:,} ({t_pct}%)")

    return "\n".join(lines)


def _poll_loop():
    s = repo.effective_settings()
    token    = s["telegram_bot_token"]
    chat_id  = str(s["telegram_chat_id"])
    if not token or not chat_id:
        return

    offset = 0
    base   = f"https://api.telegram.org/bot{token}"

    while not _stop_event.is_set():
        try:
            r = httpx.get(f"{base}/getUpdates", params={"offset": offset, "timeout": 20}, timeout=25)
            if r.status_code != 200:
                time.sleep(5)
                continue

            updates = r.json().get("result", [])
            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text    = msg.get("text", "").strip().lower()
                from_id = str(msg.get("chat", {}).get("id", ""))

                if from_id != chat_id:
                    continue

                if text in ("/status", "/s"):
                    reply = _get_status_message()
                    httpx.post(f"{base}/sendMessage", json={
                        "chat_id": chat_id,
                        "text": reply,
                        "parse_mode": "Markdown",
                    }, timeout=10)

        except Exception as e:
            logger.warning("Telegram bot poll error: %s", e)
            time.sleep(10)


def start_bot():
    global _thread, _stop_event
    _stop_event.clear()
    _thread = threading.Thread(target=_poll_loop, daemon=True, name="telegram-bot")
    _thread.start()


def stop_bot():
    _stop_event.set()
