import sqlite3
import threading
from app.config import settings

_local = threading.local()

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS aurora_tables (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    data_length_bytes   INTEGER DEFAULT 0,
    index_length_bytes  INTEGER DEFAULT 0,
    row_count_estimate  INTEGER DEFAULT 0,
    has_auto_increment  INTEGER DEFAULT 0,
    auto_increment_col  TEXT,
    has_updated_at      INTEGER DEFAULT 0,
    updated_at_col      TEXT,
    has_generated_cols  INTEGER DEFAULT 0,
    generated_col_names TEXT,
    cursor_strategy     TEXT DEFAULT 'full',
    sync_data           INTEGER DEFAULT 0,
    sync_schema         INTEGER DEFAULT 1,
    discovered_at       TEXT NOT NULL,
    UNIQUE(schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS sync_state (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    last_synced_id      INTEGER,
    last_synced_at      TEXT,
    last_offset         INTEGER DEFAULT 0,
    rows_synced         INTEGER DEFAULT 0,
    total_rows_at_start INTEGER DEFAULT 0,
    status              TEXT DEFAULT 'pending',
    error_message       TEXT,
    updated_at          TEXT NOT NULL,
    UNIQUE(schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    status          TEXT NOT NULL DEFAULT 'pending',
    started_at      TEXT,
    paused_at       TEXT,
    resumed_at      TEXT,
    completed_at    TEXT,
    cancelled_at    TEXT,
    error_message   TEXT,
    tables_total    INTEGER DEFAULT 0,
    tables_done     INTEGER DEFAULT 0,
    rows_total      INTEGER DEFAULT 0,
    rows_done       INTEGER DEFAULT 0,
    trigger_reason  TEXT DEFAULT 'manual',
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_table_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL,
    schema_name     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    status          TEXT DEFAULT 'pending',
    rows_synced     INTEGER DEFAULT 0,
    rows_total      INTEGER DEFAULT 0,
    bytes_synced    INTEGER DEFAULT 0,
    started_at      TEXT,
    completed_at    TEXT,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS twingate_checks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at  TEXT NOT NULL,
    success     INTEGER NOT NULL,
    latency_ms  INTEGER,
    error       TEXT
);
"""


def get_db() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(settings.database_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        _local.conn = conn
    return _local.conn


def dict_row(row: sqlite3.Row) -> dict:
    return dict(row) if row else None


def dict_rows(rows) -> list[dict]:
    return [dict(r) for r in rows]
