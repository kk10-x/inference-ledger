from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    provider_base_url: str = "https://api.openai.com/v1"
    provider_api_key: str = ""

    kafka_bootstrap: str = "localhost:19092"
    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql://ledger:ledger@localhost:5432/ledger"

    # How long the reconciler waits for provider-reported usage before the
    # sweeper force-settles on the gateway's own count alone.
    settlement_window_seconds: int = 300

    # Tokens of disagreement tolerated before a settlement is flagged as drift.
    # Zero is the honest default: any disagreement is a real disagreement.
    drift_tolerance_tokens: int = 0

    idempotency_ttl_seconds: int = 86_400


settings = Settings()
