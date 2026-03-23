from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Aurora
    aurora_host: str = ""
    aurora_port: int = 3306
    aurora_user: str = ""
    aurora_password: str = ""
    aurora_schema: str = ""

    # Local MySQL
    local_host: str = "mysql"
    local_port: int = 3306
    local_user: str = "syncuser"
    local_password: str = ""
    local_root_password: str = ""

    # App
    database_path: str = "/data/aurora_sync.db"
    app_port: int = 8090
    batch_size: int = 5000

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Twingate
    twingate_check_interval: int = 300  # seconds


settings = Settings()
