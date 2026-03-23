import pymysql
import pymysql.cursors
from app.state import repository as repo


def get_aurora_conn() -> pymysql.Connection:
    s = repo.effective_settings()
    return pymysql.connect(
        host=s["aurora_host"],
        port=s["aurora_port"],
        user=s["aurora_user"],
        password=s["aurora_password"],
        database=s["aurora_schema"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=300,
        write_timeout=300,
    )
