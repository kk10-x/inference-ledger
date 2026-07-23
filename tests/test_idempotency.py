"""Dedup must hold under retry storms and survive a crashed attempt."""

import asyncio

import pytest

from inference_ledger.gateway.idempotency import ClaimOutcome, IdempotencyStore


@pytest.fixture
def store(redis_client):
    return IdempotencyStore(redis_client, completed_ttl_seconds=3600)


async def test_first_claim_wins(store):
    claim = await store.claim("acme", "key-1", "req-1")
    assert claim.outcome is ClaimOutcome.CLAIMED
    assert claim.request_id == "req-1"


async def test_retry_while_in_flight_is_rejected_with_the_original_id(store):
    await store.claim("acme", "key-1", "req-1")
    retry = await store.claim("acme", "key-1", "req-2")
    assert retry.outcome is ClaimOutcome.IN_FLIGHT
    # The caller learns which request is already running rather than a dead end.
    assert retry.request_id == "req-1"


async def test_retry_after_completion_replays(store):
    await store.claim("acme", "key-1", "req-1")
    await store.complete("acme", "key-1", "req-1")
    retry = await store.claim("acme", "key-1", "req-2")
    assert retry.outcome is ClaimOutcome.REPLAY
    assert retry.request_id == "req-1"


async def test_concurrent_claims_produce_exactly_one_winner(store):
    """The retry-storm case: twenty simultaneous claims, one charge."""
    results = await asyncio.gather(*(store.claim("acme", "key-1", f"req-{i}") for i in range(20)))
    winners = [r for r in results if r.outcome is ClaimOutcome.CLAIMED]
    assert len(winners) == 1
    assert all(r.request_id == winners[0].request_id for r in results)


async def test_tenants_do_not_collide_on_the_same_key(store):
    a = await store.claim("acme", "shared-key", "req-a")
    b = await store.claim("globex", "shared-key", "req-b")
    assert a.outcome is ClaimOutcome.CLAIMED
    assert b.outcome is ClaimOutcome.CLAIMED


async def test_released_key_can_be_claimed_again(store):
    """A budget rejection must not burn the client's idempotency key."""
    await store.claim("acme", "key-1", "req-1")
    await store.release("acme", "key-1")
    again = await store.claim("acme", "key-1", "req-2")
    assert again.outcome is ClaimOutcome.CLAIMED
    assert again.request_id == "req-2"


async def test_expired_in_flight_lease_frees_the_key(store, redis_client):
    """Simulates a gateway crash: the claim exists but nobody will complete it."""
    await store.claim("acme", "key-1", "req-1")
    # Expire the lease the way the TTL eventually would.
    await redis_client.delete("idem:acme:key-1")
    recovered = await store.claim("acme", "key-1", "req-2")
    assert recovered.outcome is ClaimOutcome.CLAIMED
