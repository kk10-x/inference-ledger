"""The client-disconnect path — the bug the chaos suite found.

Before the pump refactor, a client hanging up cancelled the response generator,
which closed the upstream stream, which meant the provider's usage block (last
in the stream) was never read. Ledger B never existed, so every disconnect
force-settled as ``UNSETTLED_TIMEOUT`` and ``CLIENT_DISCONNECT_PARTIAL`` was
unreachable — the gateway could not tell "the client left" from "the provider
never reported".

These tests pin the fixed behaviour: the provider stream is drained to
completion regardless of the client, so both ledgers exist and the gap is
attributable.
"""

from __future__ import annotations

import asyncio
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


def provider_with_usage(chunks: list[str], completion_tokens: int) -> httpx.MockTransport:
    """A provider that ends with a usage block, the way a real one does."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = "".join(
            "data: " + json.dumps({"choices": [{"delta": {"content": c}}]}) + "\n\n" for c in chunks
        )
        body += (
            "data: "
            + json.dumps(
                {
                    "choices": [],
                    "usage": {"prompt_tokens": 1, "completion_tokens": completion_tokens},
                }
            )
            + "\n\n"
        )
        body += "data: [DONE]\n\n"
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    return httpx.MockTransport(handler)


async def run_pump(state, *, client_gone: bool) -> None:
    meter = StreamMeter("req-1", "k-1", "acme", "gpt-4o-mini", prompt_tokens=1, tokenizer=len)
    session = StreamSession(meter, "acme", "k-1")
    session.client_gone = client_gone
    state.sessions["req-1"] = session
    await _pump(state, "req-1", dict(BODY), admitted=0, queue=session.queue)


@pytest.fixture
def state(make_state):
    return make_state(provider_with_usage(["alpha ", "beta ", "gamma "], completion_tokens=99))


async def test_departed_client_still_yields_both_ledgers(state):
    """The whole point: Ledger B must arrive even though nobody was listening."""
    await run_pump(state, client_gone=True)

    metered = state.bus.events_on(topics.REQUESTS_METERED)
    usage = state.bus.events_on(topics.PROVIDER_USAGE)
    assert len(metered) == 1
    assert len(usage) == 1, "provider usage was lost when the client left"
    assert metered[0].terminal_state is TerminalState.CLIENT_DISCONNECT
    # Both ledgers present means the reconciler can attribute the gap rather
    # than force-settling it as an unexplained timeout.
    assert usage[0].completion_tokens == 99


async def test_normal_completion_is_not_mislabelled_a_disconnect(state):
    await run_pump(state, client_gone=False)
    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert metered[0].terminal_state is TerminalState.COMPLETED


async def test_pump_settles_exactly_once_for_a_departed_client(state):
    await run_pump(state, client_gone=True)
    assert len(state.bus.events_on(topics.REQUESTS_METERED)) == 1
    assert state.sessions == {}


async def test_slow_client_is_treated_as_gone_without_stalling_the_pump(make_state):
    """A client that stops draining must not block upstream consumption.

    The queue is bounded; once it fills, the client is declared gone and the
    pump keeps reading so the ledger still completes.
    """
    many = [f"chunk{i} " for i in range(400)]  # exceeds the 256-slot buffer
    # Budget must be out of the way, or the mid-stream cut fires first and this
    # stops testing the queue-full path at all.
    state = make_state(
        provider_with_usage(many, completion_tokens=400), tenant_budget_tokens=10_000_000
    )

    await asyncio.wait_for(run_pump(state, client_gone=False), timeout=10)

    metered = state.bus.events_on(topics.REQUESTS_METERED)
    assert len(metered) == 1
    assert metered[0].terminal_state is TerminalState.CLIENT_DISCONNECT
    assert len(state.bus.events_on(topics.PROVIDER_USAGE)) == 1
