"""End-to-end request path against a fake provider, fake Redis and fake bus.

These are the tests that would otherwise need containers. Every one of them
asserts the same underlying property: **one request produces exactly one
Ledger A entry, carrying the right terminal state.**
"""

from __future__ import annotations

import pytest
from conftest import provider_stream
from httpx import ASGITransport, AsyncClient

from inference_ledger import topics
from inference_ledger.events import TerminalState

BODY = {
    "model": "gpt-4o-mini",
    "stream": True,
    "messages": [{"role": "user", "content": "hello there friend"}],
}


async def call(app, **headers):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://gateway.test") as client:
        return await client.post("/v1/chat/completions", json=BODY, headers=headers)


@pytest.fixture
async def running(make_app):
    """Yields (app_factory) with lifespan handled per-test."""

    def _run(transport, **overrides):
        return make_app(transport, **overrides)

    return _run


async def test_successful_stream_settles_once_as_completed(running):
    app, state = running(provider_stream(["Hello", " world"]))
    async with app.router.lifespan_context(app):
        response = await call(app, **{"Idempotency-Key": "k1", "X-Tenant-Id": "acme"})
        assert response.status_code == 200
        assert "Hello" in response.text
        assert "[DONE]" in response.text

    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert len(metered) == 1
    assert metered[0].terminal_state is TerminalState.COMPLETED
    assert metered[0].completion_tokens > 0
    # Started and metered are both emitted, keyed identically.
    assert len(state.bus.events_on(topics.REQUESTS_STARTED)) == 1


async def test_client_sees_tokens_unmodified(running):
    app, _ = running(provider_stream(["alpha", "beta"]))
    async with app.router.lifespan_context(app):
        response = await call(app, **{"Idempotency-Key": "k1"})
    assert "alpha" in response.text
    assert "beta" in response.text


async def test_retry_with_same_key_does_not_reach_the_provider_twice(running):
    app, state = running(provider_stream(["once"]))
    async with app.router.lifespan_context(app):
        first = await call(app, **{"Idempotency-Key": "same", "X-Tenant-Id": "acme"})
        assert first.status_code == 200
        second = await call(app, **{"Idempotency-Key": "same", "X-Tenant-Id": "acme"})

    assert second.status_code == 409
    # One charge, not two — the whole point of the key.
    assert len(state.bus.events_on(topics.REQUESTS_METERED)) == 1


async def test_admission_rejection_does_not_burn_the_key(running):
    app, state = running(provider_stream(["hi"]), tenant_budget_tokens=1)
    async with app.router.lifespan_context(app):
        rejected = await call(app, **{"Idempotency-Key": "k1", "X-Tenant-Id": "poor"})
        assert rejected.status_code == 429
        # Same key, now with headroom: must be servable, not stuck at 409.
        state.budget._capacity = 10_000
        retried = await call(app, **{"Idempotency-Key": "k1", "X-Tenant-Id": "rich"})
        assert retried.status_code == 200

    # The rejected request never ran, so it never settled.
    assert len(state.bus.events_on(topics.REQUESTS_METERED)) == 1


async def test_mid_stream_budget_cut_settles_as_budget_exceeded(running):
    # Capacity covers admission but not a long response.
    app, state = running(
        provider_stream(["x" * 400 for _ in range(20)]),
        tenant_budget_tokens=60,
        admission_estimate_tokens=1,
    )
    async with app.router.lifespan_context(app):
        response = await call(app, **{"Idempotency-Key": "k1", "X-Tenant-Id": "acme"})

    assert response.status_code == 200
    # Cut cleanly, not dropped: the client still sees a terminal frame.
    assert "[DONE]" in response.text
    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert len(metered) == 1
    assert metered[0].terminal_state is TerminalState.BUDGET_EXCEEDED


async def test_provider_error_still_settles(running):
    app, state = running(provider_stream([], status_code=500))
    async with app.router.lifespan_context(app):
        await call(app, **{"Idempotency-Key": "k1"})

    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert len(metered) == 1
    assert metered[0].terminal_state is TerminalState.PROVIDER_ERROR
    # Prompt tokens were still spent upstream; they are not written off.
    assert metered[0].prompt_tokens > 0


async def test_readiness_flips_but_liveness_holds_during_drain(running):
    app, state = running(provider_stream(["hi"]))
    transport = ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=transport, base_url="http://g.test") as client,
    ):
        assert (await client.get("/readyz")).status_code == 200
        state.ready = False
        assert (await client.get("/readyz")).status_code == 503
        # Being killed for failing liveness mid-drain would be self-inflicted.
        assert (await client.get("/healthz")).status_code == 200


async def test_requests_are_refused_once_draining(running):
    app, state = running(provider_stream(["hi"]))
    async with app.router.lifespan_context(app):
        state.ready = False
        response = await call(app, **{"Idempotency-Key": "k1"})
    assert response.status_code == 503
    assert state.bus.events_on(topics.REQUESTS_METERED) == []


async def test_shutdown_settles_a_stream_left_open(running, make_state):
    """The drain path, exercised directly: a session open at shutdown must
    settle as GATEWAY_SHUTDOWN rather than vanishing."""
    from inference_ledger.gateway.app import StreamSession, drain
    from inference_ledger.gateway.metering import StreamMeter

    state = make_state(provider_stream(["hi"]))
    meter = StreamMeter("req-open", "k-open", "acme", "gpt-4o-mini", prompt_tokens=5)
    meter.consume('data: {"choices":[{"delta":{"content":"partial"}}]}')
    state.sessions["req-open"] = StreamSession(meter, "acme", "k-open")

    await drain(state, grace_seconds=0.3, flush_reserve=0.1)

    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert len(metered) == 1
    assert metered[0].terminal_state is TerminalState.GATEWAY_SHUTDOWN
    assert metered[0].completion_tokens > 0
    assert state.sessions == {}


async def test_settle_is_idempotent_when_two_paths_race(make_state):
    """The drain and the response generator both try to settle. Exactly one wins."""
    import asyncio

    from inference_ledger.gateway.app import StreamSession
    from inference_ledger.gateway.metering import StreamMeter

    state = make_state(provider_stream(["hi"]))
    meter = StreamMeter("req-1", "k-1", "acme", "gpt-4o-mini", prompt_tokens=5)
    state.sessions["req-1"] = StreamSession(meter, "acme", "k-1")

    await asyncio.gather(
        state.settle("req-1", TerminalState.COMPLETED),
        state.settle("req-1", TerminalState.GATEWAY_SHUTDOWN),
        state.settle("req-1", TerminalState.CLIENT_DISCONNECT),
    )

    assert len(state.bus.events_on(topics.REQUESTS_METERED)) == 1
