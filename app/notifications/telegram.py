import httpx
from app.state import repository as repo


def notify(message: str) -> bool:
    """Send a Telegram message. Returns True on success."""
    s = repo.effective_settings()
    token = s["telegram_bot_token"]
    chat_id = s["telegram_chat_id"]
    if not token or not chat_id:
        return False
    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False
