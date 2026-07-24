"""The stateful half of the join: hold one ledger until its partner arrives.

``attribute()`` in ``join.py`` is pure — given both ledgers it decides what
happened. This module owns the *waiting*: the two ledgers for a request arrive
on different topics, in either order, possibly more than once. ``PendingJoiner``
buffers whichever side comes first and settles the moment the pair is complete.

It is deliberately free of Kafka and Postgres so the ordering matrix — A-then-B,
B-then-A, duplicate A, duplicate B, A-with-no-B — is exhaustively unit-testable.
The consumer feeds it events and acts on what it returns.

Per-partition state, not global: because every event is keyed by ``request_id``,
one partition's consumer sees both sides of every join it is responsible for, so
this map never needs to be shared across consumers. On rebalance the state is
simply rebuilt by replaying the partition from the last committed offset.
"""

from __future__ import annotations

from inference_ledger.events import ProviderUsage, RequestMetered, Settlement
from inference_ledger.reconciler.join import attribute


class PendingJoiner:
    def __init__(self, *, tolerance: int = 0) -> None:
        self._tolerance = tolerance
        self._metered: dict[str, RequestMetered] = {}
        self._usage: dict[str, ProviderUsage] = {}
        # request_ids already settled, so a late duplicate of either side is
        # recognised as a double-count rather than joined a second time.
        self._settled: set[str] = set()

    def on_metered(self, metered: RequestMetered) -> Settlement | None:
        """Feed Ledger A. Returns a settlement if Ledger B was already waiting."""
        rid = metered.request_id
        if rid in self._settled:
            # Ledger A arriving after we already settled means the gateway
            # emitted it twice — a dedup failure, flagged rather than swallowed.
            return attribute(metered, self._usage.get(rid), duplicate_key=True)

        usage = self._usage.pop(rid, None)
        if usage is not None:
            return self._settle(metered, usage)

        self._metered[rid] = metered
        return None

    def on_usage(self, usage: ProviderUsage) -> Settlement | None:
        """Feed Ledger B. Returns a settlement if Ledger A was already waiting."""
        rid = usage.request_id
        metered = self._metered.pop(rid, None)
        if metered is not None:
            return self._settle(metered, usage)

        # B with no A yet. Buffer it; A drives settlement because A is the record
        # that carries tenant, model and terminal state. A usage event that never
        # finds its A is a provider reporting a request we have no record of —
        # left buffered and surfaced by the consumer as an operational metric.
        self._usage[rid] = usage
        return None

    def _settle(self, metered: RequestMetered, usage: ProviderUsage | None) -> Settlement:
        self._settled.add(metered.request_id)
        return attribute(metered, usage, tolerance=self._tolerance)

    def force_settle(self, metered: RequestMetered) -> Settlement:
        """Settle on Ledger A alone — used by the sweeper when B never arrived."""
        self._usage.pop(metered.request_id, None)
        self._metered.pop(metered.request_id, None)
        self._settled.add(metered.request_id)
        return attribute(metered, None, tolerance=self._tolerance)

    def discard(self, request_id: str) -> None:
        """Drop any buffered half-join for a request settled elsewhere.

        The sweeper writes its settlement straight to the ledger, so without
        this the joiner keeps the orphaned Ledger A forever and
        ``buffered_events`` grows without bound — a slow leak that also makes
        the gauge useless as a health signal.
        """
        self._metered.pop(request_id, None)
        self._usage.pop(request_id, None)
        self._settled.add(request_id)

    def peek_metered(self, request_id: str) -> RequestMetered | None:
        """Read a buffered Ledger A without consuming it (used by the sweeper)."""
        return self._metered.get(request_id)

    def forget(self, request_id: str) -> None:
        """Drop settled-set membership once it can no longer be contradicted.

        Bounds memory: the settled set would otherwise grow without limit. Called
        by the consumer once a request is older than the settlement window, past
        which a duplicate is impossible because the sweeper has moved on.
        """
        self._settled.discard(request_id)

    @property
    def buffered_metered(self) -> int:
        return len(self._metered)

    @property
    def buffered_usage(self) -> int:
        return len(self._usage)
