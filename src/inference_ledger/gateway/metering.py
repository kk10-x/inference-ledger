"""Ledger A: count tokens off the SSE stream as it is proxied.

The whole reason this is counted on the wire rather than read from the
provider's final ``usage`` block is that the final block does not exist when
things go wrong. A client that hangs up at token 800 of 1200, a pod evicted
mid-stream, a budget cut — none of those produce a ``usage`` payload, and every
metering tool that only reads ``usage`` silently loses the request.

:class:`StreamMeter` is therefore incremental: it holds a valid answer after
*every* chunk, so a partial count is always available.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from inference_ledger.events import RequestMetered, TerminalState

_DONE = "[DONE]"


def _default_tokenizer(text: str) -> int:
    """Crude fallback so the meter works without a tokenizer download.

    Real deployments inject a ``tiktoken`` encoder. This exists so tests and a
    cold ``make up`` do not depend on the network — and its inaccuracy is itself
    a useful demo, since it surfaces as ``TOKENIZER_MISMATCH`` drift.
    """
    return max(1, len(text) // 4) if text else 0


class StreamMeter:
    """Accumulates completion tokens from an OpenAI-style SSE stream."""

    def __init__(
        self,
        request_id: str,
        idempotency_key: str,
        tenant_id: str,
        model: str,
        prompt_tokens: int,
        tokenizer: Callable[[str], int] = _default_tokenizer,
    ) -> None:
        self.request_id = request_id
        self.idempotency_key = idempotency_key
        self.tenant_id = tenant_id
        self.model = model
        self.prompt_tokens = prompt_tokens
        self._tokenizer = tokenizer
        self._buffer = ""
        self.completion_tokens = 0
        self.saw_done = False
        # Ledger B, if the provider volunteers it. Captured into a separate field
        # so it can never leak into the wire count above — the two must be able
        # to disagree. None means the provider reported nothing, which is itself
        # a signal the reconciler acts on (UNSETTLED_TIMEOUT).
        self.provider_prompt_tokens: int | None = None
        self.provider_completion_tokens: int | None = None

    def consume(self, line: str) -> None:
        """Feed one raw SSE line. Unparseable lines are ignored, never fatal.

        A malformed chunk must not abort a stream the client is happily
        receiving; the resulting undercount shows up as drift, which is the
        correct place for it to show up.
        """
        line = line.strip()
        if not line.startswith("data:"):
            return
        payload = line[len("data:") :].strip()
        if payload == _DONE:
            self.saw_done = True
            return
        try:
            chunk = json.loads(payload)
        except json.JSONDecodeError:
            return

        for choice in chunk.get("choices", []):
            content = choice.get("delta", {}).get("content")
            if content:
                self._buffer += content

        # A provider that emits a usage block (OpenAI does with
        # stream_options.include_usage) is reporting Ledger B. Capture it into
        # its own fields — never into completion_tokens — so the wire count and
        # the reported count remain independently computed and free to disagree.
        usage = chunk.get("usage")
        if isinstance(usage, dict):
            self.provider_prompt_tokens = usage.get("prompt_tokens")
            self.provider_completion_tokens = usage.get("completion_tokens")

        self.completion_tokens = self._tokenizer(self._buffer)

    @property
    def has_provider_usage(self) -> bool:
        return self.provider_completion_tokens is not None

    def finalize(self, terminal_state: TerminalState, ended_at: float) -> RequestMetered:
        """Emit Ledger A. Safe to call at any point, including mid-stream."""
        return RequestMetered(
            request_id=self.request_id,
            idempotency_key=self.idempotency_key,
            tenant_id=self.tenant_id,
            model=self.model,
            prompt_tokens=self.prompt_tokens,
            completion_tokens=self.completion_tokens,
            terminal_state=terminal_state,
            ended_at=ended_at,
        )
