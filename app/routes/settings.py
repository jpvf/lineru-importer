import socket
from fastapi import APIRouter
import pymysql
import httpx
from app.state import repository as repo
from app.config import settings

router = APIRouter(prefix="/api/settings", tags=["settings"])

SENSITIVE = {"aurora_password", "local_password", "local_root_password", "telegram_bot_token"}


@router.get("")
def get_settings():
    data = repo.get_settings()
    # Redact sensitive keys
    return {k: ("***" if k in SENSITIVE and v else v) for k, v in data.items()}


@router.put("")
def update_settings(body: dict):
    repo.set_settings(body)
    return {"status": "ok"}


@router.post("/test-aurora")
def test_aurora():
    try:
        conn = pymysql.connect(
            host=settings.aurora_host, port=settings.aurora_port,
            user=settings.aurora_user, password=settings.aurora_password,
            database=settings.aurora_schema, connect_timeout=10,
        )
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/test-local")
def test_local():
    try:
        conn = pymysql.connect(
            host=settings.local_host, port=settings.local_port,
            user=settings.local_user, password=settings.local_password,
            connect_timeout=10,
        )
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/test-telegram")
def test_telegram():
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id
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
