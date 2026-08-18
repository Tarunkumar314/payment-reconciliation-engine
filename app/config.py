from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_ENV: str = "development"
    DEBUG: bool = True
    PORT: int = 8000

    POSTGRES_USER: str = "reconcile_user"
    POSTGRES_PASSWORD: str = "reconcile_pass"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "reconcile_db"
    DATABASE_URL: str = "postgresql+asyncpg://reconcile_user:reconcile_pass@localhost:5432/reconcile_db"

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_URL: str = "redis://localhost:6379/0"

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_SETTLEMENT_TOPIC: str = "settlement-events"
    KAFKA_DLQ_TOPIC: str = "settlement-events-dlq"

    # Mock bank settings
    MOCK_BANK_URL: str = "http://localhost:8000"  # points at self in dev; overridden in Docker
    MOCK_BANK_FAILURE_RATE: float = 0.2           # 0.0 = never fail, 1.0 = always fail
    MOCK_BANK_DELAY_MS: int = 100                 # artificial latency in milliseconds

    # Settlement worker retry config
    SETTLEMENT_MAX_RETRIES: int = 3
    SETTLEMENT_BASE_BACKOFF_SECONDS: float = 1.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

@lru_cache()
def get_settings() -> Settings:
    return Settings()
