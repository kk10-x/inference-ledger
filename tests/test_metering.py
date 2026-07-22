"""Ledger A must hold a usable answer after every chunk, not just at [DONE]."""

import json

from inference_ledger.events import TerminalState
from inference_ledger.gateway.metering import StreamMeter


def sse(content: str) -> str:
    return "data: " + json.dumps({"choices": [{"delta": {"content": content}}]})


def new_meter(tokenizer=len):
    return StreamMeter(
        request_id="req-1",
        idempotency_key="key-1",
        tenant_id="acme",
        model="gpt-4o-mini",
        prompt_tokens=10,
        tokenizer=tokenizer,
    )


def test_counts_accumulate_across_chunks():
    m = new_meter()
    for word in ["Hel", "lo ", "wor", "ld"]:
        m.consume(sse(word))
    assert m.completion_tokens == len("Hello world")


def test_partial_stream_still_finalizes():
    m = new_meter()
    m.consume(sse("half a resp"))
    ledger = m.finalize(TerminalState.CLIENT_DISCONNECT, ended_at=1000.0)
    assert ledger.completion_tokens == len("half a resp")
    assert ledger.terminal_state is TerminalState.CLIENT_DISCONNECT
    assert not m.saw_done


def test_malformed_chunk_does_not_abort_the_stream():
    m = new_meter()
    m.consume(sse("good "))
    m.consume("data: {not json")
    m.consume(sse("more"))
    assert m.completion_tokens == len("good more")


def test_done_marker_is_recorded_and_not_counted():
    m = new_meter()
    m.consume(sse("hi"))
    m.consume("data: [DONE]")
    assert m.saw_done
    assert m.completion_tokens == 2


def test_non_data_lines_are_ignored():
    m = new_meter()
    m.consume(": keep-alive")
    m.consume("")
    assert m.completion_tokens == 0
