"""Settlement persistence.

The gateway and reconciler both depend on this ``LedgerRepo`` protocol rather
than on psycopg directly, so the join logic and sweeper can be tested against
``InMemoryLedger`` with no database.

The single invariant every implementation must uphold: **writing a settlement
for a ``request_id`` that is already settled is a no-op that reports the fact,
never a second row.** That is what makes the whole pipeline effectively-once —
the reconciler commits Kafka offsets only after a successful write, so a
redelivered event re-runs the write, and the write must absorb it.
"""

from __future__ import annotations

from typing import Protocol

from inference_ledger.events import Settlement


class LedgerRepo(Protocol):
    def record_pending(self, request_id: str, tenant_id: str, started_at: float) -> None:
        """Note that a request is awaiting its second ledger. Idempotent."""
        ...

    def settle(self, settlement: Settlement) -> bool:
        """Persist a settlement and clear any pending row.

        Returns True if this call created the settlement, False if one already
        existed. A False is not an error — it is duplicate delivery being
        absorbed exactly as designed — but the caller records it as a metric,
        because a *rising* rate of duplicates means something upstream is wrong.
        """
        ...

    def expired_pending(self, older_than: float, limit: int = 500) -> list[tuple[str, str]]:
        """Return ``(request_id, tenant_id)`` pending longer than ``older_than``."""
        ...


class InMemoryLedger:
    """Test double with the same effectively-once guarantee as Postgres."""

    def __init__(self) -> None:
        self.settlements: dict[str, Settlement] = {}
        self.pending: dict[str, tuple[str, float]] = {}
        self.duplicate_settle_attempts = 0

    def record_pending(self, request_id: str, tenant_id: str, started_at: float) -> None:
        self.pending.setdefault(request_id, (tenant_id, started_at))

    def settle(self, settlement: Settlement) -> bool:
        self.pending.pop(settlement.request_id, None)
        if settlement.request_id in self.settlements:
            self.duplicate_settle_attempts += 1
            return False
        self.settlements[settlement.request_id] = settlement
        return True

    def expired_pending(self, older_than: float, limit: int = 500) -> list[tuple[str, str]]:
        return [
            (request_id, tenant_id)
            for request_id, (tenant_id, started_at) in self.pending.items()
            if started_at <= older_than
        ][:limit]


class PostgresLedger:
    """Production repo. psycopg is imported lazily so the test suite and CI do
    not require a database driver present."""

    def __init__(self, dsn: str, apply_schema: bool = True) -> None:
        from psycopg_pool import ConnectionPool

        self._pool = ConnectionPool(dsn, min_size=1, max_size=8, open=True)
        if apply_schema:
            self._apply_schema()

    def _apply_schema(self) -> None:
        """Create the ledger tables if they do not exist.

        The schema is authored once in ``schema.sql`` and applied by the
        application itself, not by a Postgres-specific init mount. That is the
        only form that also works against a managed database (RDS has no
        ``docker-entrypoint-initdb.d``), so the same code path covers local
        compose, Kubernetes, and cloud. Every statement is ``IF NOT EXISTS``, so
        applying it on every reconciler start is idempotent and safe.
        """
        from pathlib import Path

        schema = (Path(__file__).parent / "schema.sql").read_text()
        with self._pool.connection() as conn:
            conn.execute(schema)

    def record_pending(self, request_id: str, tenant_id: str, started_at: float) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO pending_settlements (request_id, tenant_id, started_at)
                VALUES (%s, %s, to_timestamp(%s))
                ON CONFLICT (request_id) DO NOTHING
                """,
                (request_id, tenant_id, started_at),
            )

    def settle(self, s: Settlement) -> bool:
        # A single transaction: the INSERT and the pending-delete must not be
        # separable, or a crash between them could resurrect a settled request.
        with self._pool.connection() as conn, conn.transaction():
            cur = conn.execute(
                """
                INSERT INTO settlements (
                    request_id, tenant_id, model, status, metered_total,
                    provider_total, drift_tokens, drift_reason, settled_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
                ON CONFLICT (request_id) DO NOTHING
                """,
                (
                    s.request_id,
                    s.tenant_id,
                    s.model,
                    s.status.value,
                    s.metered_total,
                    s.provider_total,
                    s.drift_tokens,
                    s.drift_reason.value if s.drift_reason else None,
                    s.settled_at,
                ),
            )
            conn.execute("DELETE FROM pending_settlements WHERE request_id = %s", (s.request_id,))
            # rowcount is 0 when ON CONFLICT suppressed the insert.
            return cur.rowcount == 1

    def expired_pending(self, older_than: float, limit: int = 500) -> list[tuple[str, str]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT request_id, tenant_id FROM pending_settlements
                WHERE started_at <= to_timestamp(%s)
                ORDER BY started_at
                LIMIT %s
                """,
                (older_than, limit),
            ).fetchall()
        return [(r[0], r[1]) for r in rows]
