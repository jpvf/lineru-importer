"""Discover tables, columns, sizes and strategies from Aurora."""
import json
from datetime import datetime, timezone
from app.aurora.connection import get_aurora_conn
from app.config import settings

DATETIME_COL_NAMES = {"updated_at", "modified_at", "last_modified", "updated", "modified", "changed_at"}
DATETIME_TYPES = {"datetime", "timestamp"}


def discover_all() -> list[dict]:
    conn = get_aurora_conn()
    try:
        with conn.cursor() as cur:
            # Table sizes from information_schema
            cur.execute("""
                SELECT
                    TABLE_NAME,
                    COALESCE(DATA_LENGTH, 0)  AS data_length_bytes,
                    COALESCE(INDEX_LENGTH, 0) AS index_length_bytes,
                    COALESCE(TABLE_ROWS, 0)   AS row_count_estimate
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY DATA_LENGTH DESC
            """, (settings.aurora_schema,))
            tables = cur.fetchall()

            # All columns with extra info for each table
            cur.execute("""
                SELECT
                    TABLE_NAME,
                    COLUMN_NAME,
                    COLUMN_KEY,
                    EXTRA,
                    DATA_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """, (settings.aurora_schema,))
            all_cols = cur.fetchall()

        # Group columns by table
        cols_by_table: dict[str, list] = {}
        for col in all_cols:
            cols_by_table.setdefault(col["TABLE_NAME"], []).append(col)

        now = datetime.now(timezone.utc).isoformat()
        result = []

        for t in tables:
            name = t["TABLE_NAME"]
            cols = cols_by_table.get(name, [])

            # Auto-increment primary key
            auto_inc_col = next(
                (c["COLUMN_NAME"] for c in cols
                 if "auto_increment" in c["EXTRA"].lower() and c["COLUMN_KEY"] == "PRI"),
                None
            )

            # Datetime/timestamp update column
            updated_at_col = next(
                (c["COLUMN_NAME"] for c in cols
                 if c["COLUMN_NAME"].lower() in DATETIME_COL_NAMES
                 and c["DATA_TYPE"].lower() in DATETIME_TYPES),
                None
            )

            # Generated columns
            generated = [
                c["COLUMN_NAME"] for c in cols
                if "generated" in c["EXTRA"].lower()
            ]

            # Cursor strategy
            if auto_inc_col:
                strategy = "auto_increment"
            elif updated_at_col:
                strategy = "datetime"
            else:
                strategy = "full"

            result.append({
                "schema_name":         settings.aurora_schema,
                "table_name":          name,
                "data_length_bytes":   t["data_length_bytes"],
                "index_length_bytes":  t["index_length_bytes"],
                "row_count_estimate":  t["row_count_estimate"],
                "has_auto_increment":  1 if auto_inc_col else 0,
                "auto_increment_col":  auto_inc_col,
                "has_updated_at":      1 if updated_at_col else 0,
                "updated_at_col":      updated_at_col,
                "has_generated_cols":  1 if generated else 0,
                "generated_col_names": json.dumps(generated),
                "cursor_strategy":     strategy,
                "discovered_at":       now,
            })

        return result
    finally:
        conn.close()


def get_table_columns(conn, table_name: str) -> tuple[list[str], list[str]]:
    """Returns (insertable_cols, generated_cols) for a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (settings.aurora_schema, table_name))
        cols = cur.fetchall()

    insertable = [c["COLUMN_NAME"] for c in cols if "generated" not in c["EXTRA"].lower()]
    generated  = [c["COLUMN_NAME"] for c in cols if "generated" in c["EXTRA"].lower()]
    return insertable, generated


def get_views(conn) -> list[dict]:
    """Returns list of {name, definition} for all views."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT TABLE_NAME as name
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = %s
        """, (settings.aurora_schema,))
        views = cur.fetchall()

    result = []
    for v in views:
        with conn.cursor() as cur:
            cur.execute(f"SHOW CREATE VIEW `{v['name']}`")
            row = cur.fetchone()
            result.append({"name": v["name"], "definition": row.get("Create View", "")})
    return result


def get_routines(conn) -> list[dict]:
    """Returns list of {type, name, definition} for all SPs and functions."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ROUTINE_TYPE as type, ROUTINE_NAME as name
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = %s
        """, (settings.aurora_schema,))
        routines = cur.fetchall()

    result = []
    for r in routines:
        with conn.cursor() as cur:
            if r["type"] == "PROCEDURE":
                cur.execute(f"SHOW CREATE PROCEDURE `{r['name']}`")
                row = cur.fetchone()
                defn = row.get("Create Procedure", "")
            else:
                cur.execute(f"SHOW CREATE FUNCTION `{r['name']}`")
                row = cur.fetchone()
                defn = row.get("Create Function", "")
            result.append({"type": r["type"], "name": r["name"], "definition": defn})
    return result


def get_row_count(conn, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
        return cur.fetchone()["cnt"]
