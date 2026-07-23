"""The token bucket must be atomic, must refill, and must not bank credit."""

import asyncio

import pytest

from inference_ledger.gateway.budget import TokenBudget

NOW = 1_700_000_000_000


@pytest.fixture
def budget(redis_client):
    return TokenBudget(redis_client, capacity_tokens=1_000, refill_tokens_per_second=10.0)


async def test_draws_within_capacity(budget):
    decision = await budget.consume("acme", 400, NOW)
    assert decision.granted
    assert decision.remaining == 600


async def test_draw_beyond_remaining_is_refused_whole(budget):
    await budget.consume("acme", 900, NOW)
    decision = await budget.consume("acme", 200, NOW)
    assert not decision.granted
    # Refused, not partially served — the bucket is untouched.
    assert decision.remaining == 100


async def test_refills_over_time(budget):
    await budget.consume("acme", 1_000, NOW)
    exhausted = await budget.consume("acme", 1, NOW)
    assert not exhausted.granted

    # 10 tokens/sec for 30s = 300 tokens back.
    refilled = await budget.consume("acme", 250, NOW + 30_000)
    assert refilled.granted
    assert refilled.remaining == 50


async def test_idle_tenant_cannot_bank_credit_beyond_capacity(budget):
    # An hour idle at 10/sec would be 36,000 tokens if unclamped.
    decision = await budget.consume("acme", 1, NOW + 3_600_000)
    assert decision.granted
    assert decision.remaining == 999


async def test_concurrent_draws_cannot_oversell_the_bucket(budget):
    """Twenty concurrent 100-token draws against a 1000-token bucket."""
    results = await asyncio.gather(*(budget.consume("acme", 100, NOW) for _ in range(20)))
    granted = [r for r in results if r.granted]
    assert len(granted) == 10
    assert min(r.remaining for r in results) == 0


async def test_tenants_have_independent_buckets(budget):
    await budget.consume("acme", 1_000, NOW)
    other = await budget.consume("globex", 500, NOW)
    assert other.granted


async def test_fractional_refill_accumulates(redis_client):
    """Sub-token refills over short intervals must not truncate to zero.

    A 50ms chunk interval at 10 tokens/sec is 0.5 tokens. Integer arithmetic
    would round that to nothing and the bucket would never refill under load.
    """
    budget = TokenBudget(redis_client, capacity_tokens=100, refill_tokens_per_second=10.0)
    await budget.consume("acme", 100, NOW)
    for i in range(1, 21):
        await budget.consume("acme", 0, NOW + i * 50)
    decision = await budget.consume("acme", 10, NOW + 1_000)
    assert decision.granted
