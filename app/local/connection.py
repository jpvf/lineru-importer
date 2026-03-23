import pymysql
import pymysql.cursors
from app.state import repository as repo


def get_local_conn(autocommit: bool = False) -> pymysql.Connection:
    s = repo.effective_settings()
    return pymysql.connect(
        host=s["local_host"],
        port=s["local_port"],
        user=s["local_user"],
        password=s["local_password"],
        database=s["aurora_schema"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=autocommit,
        connect_timeout=10,
        read_timeout=600,
        write_timeout=600,
    )


def get_local_root_conn() -> pymysql.Connection:
    """Root connection for initial DB creation."""
    s = repo.effective_settings()
    return pymysql.connect(
        host=s["local_host"],
        port=s["local_port"],
        user="root",
        password=s["local_root_password"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def ensure_database_exists():
    s = repo.effective_settings()
    conn = get_local_root_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{s['aurora_schema']}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            cur.execute(
                f"GRANT ALL PRIVILEGES ON `{s['aurora_schema']}`.* TO %s@'%%'",
                (s["local_user"],)
            )
            cur.execute("FLUSH PRIVILEGES")
        conn.commit()
    finally:
        conn.close()
