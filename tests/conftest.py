"""Fakes for the whole gateway dependency set.

Everything the gateway touches has an in-process substitute: ``fakeredis`` for
Redis, ``httpx.MockTransport`` for the provider, and ``InMemoryBus`` for Kafka.
The request path is therefore fully testable — including budget cuts, client
disconnects and shutdown drains — with no containers running.
"""

from __future__ import annotations

import json

import fakeredis.aioredis
import httpx
import pytest

from inference_ledger.bus import InMemoryBus
from inference_ledger.config import Settings
from inference_ledger.gateway.app import GatewayState, create_app
from inference_ledger.gateway.budget import TokenBudget
from inference_ledger.gateway.idempotency import IdempotencyStore


def sse_chunk(content: str) -> str:
    return "data: " + json.dumps({"choices": [{"delta": {"content": content}}]}) + "\n\n"


def provider_stream(chunks: list[str], status_code: int = 200) -> httpx.MockTransport:
    """A provider that emits ``chunks`` as SSE, then ``[DONE]``."""

    def handler(request: httpx.Request) -> httpx.Response:
        if status_code >= 400:
            return httpx.Response(status_code, json={"error": {"message": "upstream failed"}})
        body = "".join(sse_chunk(c) for c in chunks) + "data: [DONE]\n\n"
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    return httpx.MockTransport(handler)


@pytest.fixture
def redis_client():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        provider_api_key="test-key",
        tenant_budget_tokens=1_000,
        tenant_refill_tokens_per_second=10.0,
        admission_estimate_tokens=10,
        shutdown_grace_seconds=2.0,
        shutdown_flush_reserve_seconds=0.5,
    )


@pytest.fixture
def make_state(redis_client, test_settings):
    """Builds gateway state around a caller-supplied provider transport."""

    def _make(transport: httpx.MockTransport, **overrides) -> GatewayState:
        cfg = test_settings.model_copy(update=overrides)
        return GatewayState(
            settings=cfg,
            bus=InMemoryBus(),
            idempotency=IdempotencyStore(redis_client, cfg.idempotency_ttl_seconds),
            budget=TokenBudget(
                redis_client, cfg.tenant_budget_tokens, cfg.tenant_refill_tokens_per_second
            ),
            client=httpx.AsyncClient(transport=transport, base_url="http://provider.test"),
        )

    return _make


@pytest.fixture
def make_app(make_state):
    def _make(transport: httpx.MockTransport, **overrides):
        state = make_state(transport, **overrides)
        return create_app(state), state

    return _make
