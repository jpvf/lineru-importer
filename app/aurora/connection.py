import pymysql
import pymysql.cursors
from app.config import settings


def get_aurora_conn() -> pymysql.Connection:
    return pymysql.connect(
        host=settings.aurora_host,
        port=settings.aurora_port,
        user=settings.aurora_user,
        password=settings.aurora_password,
        database=settings.aurora_schema,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=300,
        write_timeout=300,
    )
