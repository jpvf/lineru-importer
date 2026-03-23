import pymysql
import pymysql.cursors
from app.config import settings


def get_local_conn(autocommit: bool = False) -> pymysql.Connection:
    return pymysql.connect(
        host=settings.local_host,
        port=settings.local_port,
        user=settings.local_user,
        password=settings.local_password,
        database=settings.aurora_schema,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=autocommit,
        connect_timeout=10,
        read_timeout=600,
        write_timeout=600,
    )


def get_local_root_conn() -> pymysql.Connection:
    """Root connection for initial DB creation."""
    return pymysql.connect(
        host=settings.local_host,
        port=settings.local_port,
        user="root",
        password=settings.local_root_password,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def ensure_database_exists():
    conn = get_local_root_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{settings.aurora_schema}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            cur.execute(
                f"GRANT ALL PRIVILEGES ON `{settings.aurora_schema}`.* TO %s@'%%'",
                (settings.local_user,)
            )
            cur.execute("FLUSH PRIVILEGES")
        conn.commit()
    finally:
        conn.close()
