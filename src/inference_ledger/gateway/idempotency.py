"""Idempotency-key dedup, borrowed wholesale from payments-API practice.

A key moves through two stored states:

``in_flight``
    A request is running. A retry arriving now is a client giving up too early,
    not a new charge — it gets 409 rather than a second upstream call.
``completed``
    The request finished. A retry resolves to the original ``request_id`` so the
    caller can reconcile, and no new work is done.

The subtle case is a gateway crash, which leaves a key stuck ``in_flight`` with
nobody to complete it. That is why the in-flight state carries a **short** TTL
while the completed state carries a long one: a legitimate retry becomes possible
once the crashed attempt's lease lapses, and the orphaned partial is still
attributed as ``GATEWAY_CRASH_PARTIAL`` by the reconciler. A single long TTL
would deadlock the key; no TTL at all would leak it forever.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum

import redis.asyncio as redis

#: How long a crashed request holds its key before a retry may take over. Short
#: enough that clients are not blocked for long, comfortably longer than the
#: slowest expected stream plus the shutdown grace period.
IN_FLIGHT_TTL_SECONDS = 180


class ClaimOutcome(StrEnum):
    CLAIMED = "claimed"
    """First time seeing this key — proceed."""
    IN_FLIGHT = "in_flight"
    """An earlier attempt is still running — reject, do not duplicate work."""
    REPLAY = "replay"
    """Already completed — resolve to the original request."""


@dataclass(frozen=True)
class Claim:
    outcome: ClaimOutcome
    request_id: str
    """For CLAIMED this is the caller's new id; otherwise the original's."""


class IdempotencyStore:
    def __init__(self, client: redis.Redis, completed_ttl_seconds: int) -> None:
        self._redis = client
        self._completed_ttl = completed_ttl_seconds

    @staticmethod
    def _key(tenant_id: str, idempotency_key: str) -> str:
        # Namespaced by tenant: two tenants independently choosing "retry-1"
        # must not collide, and a shared namespace would leak one tenant's
        # request ids to another.
        return f"idem:{tenant_id}:{idempotency_key}"

    async def claim(self, tenant_id: str, idempotency_key: str, request_id: str) -> Claim:
        """Atomically claim a key. Never overwrites an existing claim."""
        key = self._key(tenant_id, idempotency_key)
        payload = json.dumps({"state": ClaimOutcome.IN_FLIGHT.value, "request_id": request_id})

        # SET NX is the whole concurrency story: exactly one caller wins, and
        # losers read the winner's record rather than racing to create their own.
        if await self._redis.set(key, payload, nx=True, ex=IN_FLIGHT_TTL_SECONDS):
            return Claim(ClaimOutcome.CLAIMED, request_id)

        existing = await self._redis.get(key)
        if existing is None:
            # The lease expired between our SET and GET. Treat as in-flight and
            # let the client retry; inventing a claim here could double-bill.
            return Claim(ClaimOutcome.IN_FLIGHT, request_id)

        record = json.loads(existing)
        state = record.get("state")
        original_id = record.get("request_id", request_id)
        if state == "completed":
            return Claim(ClaimOutcome.REPLAY, original_id)
        return Claim(ClaimOutcome.IN_FLIGHT, original_id)

    async def complete(self, tenant_id: str, idempotency_key: str, request_id: str) -> None:
        """Promote a key to completed and extend its TTL."""
        await self._redis.set(
            self._key(tenant_id, idempotency_key),
            json.dumps({"state": "completed", "request_id": request_id}),
            ex=self._completed_ttl,
        )

    async def release(self, tenant_id: str, idempotency_key: str) -> None:
        """Drop an in-flight claim that never became a real request.

        Used when admission fails *after* the claim — a budget rejection should
        not burn the client's key, because no work and no charge occurred.
        """
        await self._redis.delete(self._key(tenant_id, idempotency_key))
