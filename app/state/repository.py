"""All SQLite read/write operations. Thread-safe via WAL mode."""
import json
import threading
from datetime import datetime, timezone
from app.database import get_db, dict_row, dict_rows

_lock = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── Tables ──────────────────────────────────────────────────────────────────

def upsert_table(row: dict):
    db = get_db()
    with _lock:
        db.execute("""
            INSERT INTO aurora_tables
                (schema_name, table_name, data_length_bytes, index_length_bytes,
                 row_count_estimate, has_auto_increment, auto_increment_col,
                 has_updated_at, updated_at_col, has_generated_cols,
                 generated_col_names, cursor_strategy, discovered_at)
            VALUES
                (:schema_name,:table_name,:data_length_bytes,:index_length_bytes,
                 :row_count_estimate,:has_auto_increment,:auto_increment_col,
                 :has_updated_at,:updated_at_col,:has_generated_cols,
                 :generated_col_names,:cursor_strategy,:discovered_at)
            ON CONFLICT(schema_name, table_name) DO UPDATE SET
                data_length_bytes   = excluded.data_length_bytes,
                index_length_bytes  = excluded.index_length_bytes,
                row_count_estimate  = excluded.row_count_estimate,
                has_auto_increment  = excluded.has_auto_increment,
                auto_increment_col  = excluded.auto_increment_col,
                has_updated_at      = excluded.has_updated_at,
                updated_at_col      = excluded.updated_at_col,
                has_generated_cols  = excluded.has_generated_cols,
                generated_col_names = excluded.generated_col_names,
                cursor_strategy     = excluded.cursor_strategy,
                discovered_at       = excluded.discovered_at
        """, row)
        db.commit()


def get_tables(search: str = "", schema: str = "", strategy: str = "", only_selected: bool = False) -> list[dict]:
    db = get_db()
    query = "SELECT t.*, s.status as sync_status, s.rows_synced, s.last_synced_at, s.last_synced_id FROM aurora_tables t LEFT JOIN sync_state s ON t.schema_name=s.schema_name AND t.table_name=s.table_name WHERE 1=1"
    params = []
    if search:
        query += " AND t.table_name LIKE ?"
        params.append(f"%{search}%")
    if schema:
        query += " AND t.schema_name = ?"
        params.append(schema)
    if strategy:
        query += " AND t.cursor_strategy = ?"
        params.append(strategy)
    if only_selected:
        query += " AND t.sync_data = 1"
    query += " ORDER BY t.data_length_bytes DESC"
    return dict_rows(db.execute(query, params).fetchall())


def set_table_selection(schema_name: str, table_name: str, sync_data: bool):
    db = get_db()
    with _lock:
        db.execute(
            "UPDATE aurora_tables SET sync_data=? WHERE schema_name=? AND table_name=?",
            (1 if sync_data else 0, schema_name, table_name)
        )
        db.commit()


def bulk_set_selection(selections: list[dict]):
    db = get_db()
    with _lock:
        for s in selections:
            db.execute(
                "UPDATE aurora_tables SET sync_data=? WHERE schema_name=? AND table_name=?",
                (1 if s["sync_data"] else 0, s["schema"], s["table"])
            )
        db.commit()


def get_selected_tables() -> list[dict]:
    return get_tables(only_selected=True)


def get_table(schema_name: str, table_name: str) -> dict | None:
    db = get_db()
    row = db.execute(
        "SELECT * FROM aurora_tables WHERE schema_name=? AND table_name=?",
        (schema_name, table_name)
    ).fetchone()
    return dict_row(row)


# ─── Sync State ───────────────────────────────────────────────────────────────

def get_sync_state(schema_name: str, table_name: str) -> dict | None:
    db = get_db()
    row = db.execute(
        "SELECT * FROM sync_state WHERE schema_name=? AND table_name=?",
        (schema_name, table_name)
    ).fetchone()
    return dict_row(row)


def upsert_sync_state(schema_name: str, table_name: str, **kwargs):
    db = get_db()
    kwargs["updated_at"] = now_iso()
    state = get_sync_state(schema_name, table_name)
    with _lock:
        if state:
            sets = ", ".join(f"{k}=?" for k in kwargs)
            db.execute(
                f"UPDATE sync_state SET {sets} WHERE schema_name=? AND table_name=?",
                [*kwargs.values(), schema_name, table_name]
            )
        else:
            db.execute(
                "INSERT INTO sync_state (schema_name, table_name, updated_at) VALUES (?,?,?)",
                (schema_name, table_name, kwargs["updated_at"])
            )
            if len(kwargs) > 1:
                sets = ", ".join(f"{k}=?" for k in kwargs if k != "updated_at")
                vals = [v for k, v in kwargs.items() if k != "updated_at"]
                db.execute(
                    f"UPDATE sync_state SET {sets} WHERE schema_name=? AND table_name=?",
                    [*vals, schema_name, table_name]
                )
        db.commit()


# ─── Jobs ─────────────────────────────────────────────────────────────────────

def create_job(trigger_reason: str = "manual") -> int:
    db = get_db()
    with _lock:
        cur = db.execute(
            "INSERT INTO jobs (status, trigger_reason, created_at) VALUES ('pending',?,?)",
            (trigger_reason, now_iso())
        )
        db.commit()
        return cur.lastrowid


def update_job(job_id: int, **kwargs):
    db = get_db()
    with _lock:
        sets = ", ".join(f"{k}=?" for k in kwargs)
        db.execute(f"UPDATE jobs SET {sets} WHERE id=?", [*kwargs.values(), job_id])
        db.commit()


def get_job(job_id: int) -> dict | None:
    db = get_db()
    return dict_row(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())


def get_current_job() -> dict | None:
    db = get_db()
    row = db.execute(
        "SELECT * FROM jobs WHERE status IN ('running','paused') ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict_row(row)


def get_jobs(limit: int = 20, offset: int = 0) -> list[dict]:
    db = get_db()
    return dict_rows(
        db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    )


# ─── Job Table Log ────────────────────────────────────────────────────────────

def create_job_table_log(job_id: int, schema_name: str, table_name: str, rows_total: int) -> int:
    db = get_db()
    with _lock:
        cur = db.execute(
            "INSERT INTO job_table_log (job_id, schema_name, table_name, rows_total) VALUES (?,?,?,?)",
            (job_id, schema_name, table_name, rows_total)
        )
        db.commit()
        return cur.lastrowid


def update_job_table_log(log_id: int, **kwargs):
    db = get_db()
    with _lock:
        sets = ", ".join(f"{k}=?" for k in kwargs)
        db.execute(f"UPDATE job_table_log SET {sets} WHERE id=?", [*kwargs.values(), log_id])
        db.commit()


def get_job_table_logs(job_id: int) -> list[dict]:
    db = get_db()
    return dict_rows(
        db.execute("SELECT * FROM job_table_log WHERE job_id=? ORDER BY id", (job_id,)).fetchall()
    )


# ─── App Settings ─────────────────────────────────────────────────────────────

def get_settings() -> dict:
    db = get_db()
    rows = db.execute("SELECT key, value FROM app_settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


def set_setting(key: str, value: str):
    db = get_db()
    with _lock:
        db.execute(
            "INSERT INTO app_settings (key,value,updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, now_iso())
        )
        db.commit()


def set_settings(data: dict):
    for k, v in data.items():
        set_setting(k, str(v))


def effective_settings() -> dict:
    """Runtime settings: SQLite values override .env defaults."""
    from app.config import settings as s
    db = get_settings()

    def val(key, default):
        v = db.get(key)
        return v if v else default

    return {
        "aurora_host":              val("aurora_host", s.aurora_host),
        "aurora_port":              int(val("aurora_port", s.aurora_port)),
        "aurora_user":              val("aurora_user", s.aurora_user),
        "aurora_password":          val("aurora_password", s.aurora_password),
        "aurora_schema":            val("aurora_schema", s.aurora_schema),
        "local_host":               val("local_host", s.local_host),
        "local_port":               int(val("local_port", s.local_port)),
        "local_user":               val("local_user", s.local_user),
        "local_password":           val("local_password", s.local_password),
        "local_root_password":      val("local_root_password", s.local_root_password),
        "batch_size":               int(val("batch_size", s.batch_size)),
        "twingate_check_interval":  int(val("twingate_check_interval", s.twingate_check_interval)),
        "telegram_bot_token":       val("telegram_bot_token", s.telegram_bot_token),
        "telegram_chat_id":         val("telegram_chat_id", s.telegram_chat_id),
    }


# ─── Twingate ─────────────────────────────────────────────────────────────────

def log_twingate_check(success: bool, latency_ms: int | None, error: str | None):
    db = get_db()
    with _lock:
        db.execute(
            "INSERT INTO twingate_checks (checked_at, success, latency_ms, error) VALUES (?,?,?,?)",
            (now_iso(), 1 if success else 0, latency_ms, error)
        )
        db.commit()


def get_last_twingate_check() -> dict | None:
    db = get_db()
    return dict_row(
        db.execute("SELECT * FROM twingate_checks ORDER BY id DESC LIMIT 1").fetchone()
    )
