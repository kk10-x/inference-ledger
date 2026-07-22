-- Settlement ledger.
--
-- The primary key on request_id is what turns an at-least-once Kafka pipeline
-- into an effectively-once ledger: the reconciler commits offsets only after
-- this insert, and a redelivered event hits ON CONFLICT DO NOTHING.

CREATE TABLE IF NOT EXISTS settlements (
    request_id      TEXT PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    model           TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    metered_total   INTEGER     NOT NULL,
    provider_total  INTEGER,
    drift_tokens    INTEGER     NOT NULL DEFAULT 0,
    drift_reason    TEXT,
    settled_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A unique settlement per idempotency key is the second line of defence against
-- double billing. Violations here are surfaced as RETRY_DOUBLE_COUNT drift
-- rather than swallowed, because a silent dedup failure is the expensive kind.
CREATE TABLE IF NOT EXISTS idempotency_index (
    idempotency_key TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    request_id      TEXT NOT NULL REFERENCES settlements(request_id)
);

-- Requests awaiting the second ledger. The sweeper scans this by started_at.
CREATE TABLE IF NOT EXISTS pending_settlements (
    request_id  TEXT PRIMARY KEY,
    tenant_id   TEXT        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS pending_settlements_started_at_idx
    ON pending_settlements (started_at);

CREATE INDEX IF NOT EXISTS settlements_drift_idx
    ON settlements (drift_reason, settled_at)
    WHERE drift_reason IS NOT NULL;

CREATE INDEX IF NOT EXISTS settlements_tenant_idx
    ON settlements (tenant_id, settled_at);
