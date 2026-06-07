from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore")

    port: int = 3000
    host: str = "0.0.0.0"
    debug: bool = False

    postgres_host:     str = "localhost"
    postgres_port:     int = 5432
    postgres_user:     str = "aptuser"
    postgres_password: str = "aptpassword"
    postgres_db:       str = "aptdb"

    replication_slot_name:        str   = "orders_slot"
    publication_name:             str   = "orders_pub"
    replication_reconnect_delay_s: float = 3.0

    sse_heartbeat_interval_s: float = 20.0

    @property
    def asyncpg_dsn(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
