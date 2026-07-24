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

    # Per-tenant token bucket. Capacity is the burst a tenant may spend at once;
    # refill is their sustained rate.
    tenant_budget_tokens: int = 200_000
    tenant_refill_tokens_per_second: float = 50.0

    # Assumed completion size when a request declares no `max_tokens`. Only used
    # for the admission guess; the mid-stream draw is what actually enforces.
    admission_estimate_tokens: int = 512

    # Must stay below the container/pod termination grace period, and the
    # reserve must be large enough for the producer to flush. See
    # docs/shutdown.md for the table these two have to agree with.
    shutdown_grace_seconds: float = 40.0
    shutdown_flush_reserve_seconds: float = 10.0

    # Flush `requests.started` to the broker before returning the first byte.
    # Costs one round-trip on TTFT and closes the window where a SIGKILL loses
    # a buffered start event, leaving a request that no component knows existed.
    # Default on: an unbillable request is worse than a slightly slower one.
    durable_request_start: bool = True


settings = Settings()
