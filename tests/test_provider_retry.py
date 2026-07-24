"""Retry-before-first-token, and the boundary that keeps it safe.

An upstream deploy leaves stale keepalive connections in the pool; the next
request to reuse one fails through no fault of its own. Retrying is correct
*only* while no token has been produced — at that point nothing has reached the
client and nothing has been billed. After the first token, a retry would bill
the prefix twice, so the failure has to stand.
"""

from __future__ import annotations

import json

import httpx
import pytest

from inference_ledger import topics
from inference_ledger.events import TerminalState
from inference_ledger.gateway.app import StreamSession, _pump
from inference_ledger.gateway.metering import StreamMeter

BODY = {
    "model": "gpt-4o-mini",
    "stream": True,
    "messages": [{"role": "user", "content": "hello"}],
}


def sse_body(chunks: list[str]) -> str:
    body = "".join(
        "data: " + json.dumps({"choices": [{"delta": {"content": c}}]}) + "\n\n" for c in chunks
    )
    return body + "data: [DONE]\n\n"


def failing_then_ok(failures: int) -> tuple[httpx.MockTransport, list[int]]:
    """Transport that raises a connection error `failures` times, then succeeds."""
    calls = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        if calls[0] <= failures:
            raise httpx.ConnectError("stale pooled connection", request=request)
        return httpx.Response(
            200, text=sse_body(["ok "]), headers={"content-type": "text/event-stream"}
        )

    return httpx.MockTransport(handler), calls


async def run_pump(state) -> None:
    meter = StreamMeter("req-1", "k-1", "acme", "gpt-4o-mini", prompt_tokens=1, tokenizer=len)
    session = StreamSession(meter, "acme", "k-1")
    state.sessions["req-1"] = session
    await _pump(state, "req-1", dict(BODY), admitted=0, queue=session.queue)


async def test_stale_connection_is_retried_and_succeeds(make_state):
    transport, calls = failing_then_ok(failures=1)
    state = make_state(transport)

    await run_pump(state)

    assert calls[0] == 2, "the failed attempt was not retried"
    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert metered[0].terminal_state is TerminalState.COMPLETED
    assert metered[0].completion_tokens > 0


async def test_retry_is_bounded(make_state):
    """A provider that is genuinely down must not be hammered."""
    transport, calls = failing_then_ok(failures=99)
    state = make_state(transport)

    await run_pump(state)

    assert calls[0] == 2, "expected exactly one retry"
    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert metered[0].terminal_state is TerminalState.PROVIDER_ERROR


async def test_failure_after_first_token_is_not_retried(make_state):
    """The safety boundary: retrying here would bill the prefix twice."""
    calls = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        # Emits tokens, then dies mid-stream without a terminal frame.
        return httpx.Response(
            200,
            text="data: " + json.dumps({"choices": [{"delta": {"content": "partial "}}]}) + "\n\n",
            headers={"content-type": "text/event-stream"},
        )

    state = make_state(httpx.MockTransport(handler))
    await run_pump(state)

    assert calls[0] == 1, "a stream that already produced tokens must not be retried"
    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert len(metered) == 1
    assert metered[0].completion_tokens > 0


@pytest.mark.parametrize("failures", [0, 1, 99])
async def test_exactly_one_ledger_entry_regardless_of_retries(make_state, failures):
    """However many attempts happen upstream, the ledger sees one request."""
    transport, _calls = failing_then_ok(failures)
    state = make_state(transport)

    await run_pump(state)

    assert len(state.bus.events_on(topics.REQUESTS_METERED)) == 1
    assert state.sessions == {}
