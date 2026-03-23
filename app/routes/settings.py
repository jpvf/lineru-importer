import httpx
from fastapi import APIRouter
import pymysql
from app.state import repository as repo

router = APIRouter(prefix="/api/settings", tags=["settings"])

SENSITIVE = {"aurora_password", "local_password", "local_root_password", "telegram_bot_token"}


@router.get("")
def get_settings():
    data = repo.get_settings()
    # Redact sensitive keys
    return {k: ("***" if k in SENSITIVE and v else v) for k, v in data.items()}


@router.put("")
def update_settings(body: dict):
    # Never overwrite a sensitive field with the redacted placeholder "***"
    filtered = {k: v for k, v in body.items() if not (k in SENSITIVE and v == "***")}
    repo.set_settings(filtered)
    return {"status": "ok"}


@router.post("/test-aurora")
def test_aurora():
    s = repo.effective_settings()
    try:
        conn = pymysql.connect(
            host=s["aurora_host"], port=s["aurora_port"],
            user=s["aurora_user"], password=s["aurora_password"],
            database=s["aurora_schema"], connect_timeout=10,
        )
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/test-local")
def test_local():
    s = repo.effective_settings()
    try:
        conn = pymysql.connect(
            host=s["local_host"], port=s["local_port"],
            user=s["local_user"], password=s["local_password"],
            connect_timeout=10,
        )
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/test-telegram")
def test_telegram():
    s = repo.effective_settings()
    token = s["telegram_bot_token"]
    chat_id = s["telegram_chat_id"]
    if not token or not chat_id:
        return {"ok": False, "error": "Token or chat_id not configured"}
    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": "✅ aurora-sync test message", "parse_mode": "Markdown"},
            timeout=10,
        )
        return {"ok": r.status_code == 200, "response": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/twingate")
def twingate_status():
    return repo.get_last_twingate_check() or {"success": None, "checked_at": None}
