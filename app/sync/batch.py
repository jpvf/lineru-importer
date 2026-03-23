"""Bulk INSERT helpers for local MySQL."""
import pymysql


def bulk_replace(conn: pymysql.Connection, table: str, cols: list[str], rows: list[dict]):
    """REPLACE INTO — used for full-strategy tables (no incremental)."""
    if not rows:
        return
    col_list = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"REPLACE INTO `{table}` ({col_list}) VALUES ({placeholders})"
    values = [[row.get(c) for c in cols] for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, values)
    conn.commit()


def bulk_upsert(conn: pymysql.Connection, table: str, cols: list[str], rows: list[dict]):
    """INSERT ... ON DUPLICATE KEY UPDATE — used for incremental strategies."""
    if not rows:
        return
    col_list = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols)
    sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    values = [[row.get(c) for c in cols] for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, values)
    conn.commit()
