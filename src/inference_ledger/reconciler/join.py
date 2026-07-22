"""Attribution: given two ledgers for one request, decide what happened.

This module is deliberately pure. Everything about Kafka, offsets, windows and
Postgres lives in ``consumer.py``; the decision of *why* two numbers disagree is
here, with no I/O, so it can be tested exhaustively against the failure matrix in
``tests/test_join.py``.
"""

from __future__ import annotations

import time

from inference_ledger.events import (
    DriftReason,
    ProviderUsage,
    RequestMetered,
    Settlement,
    SettlementStatus,
    TerminalState,
)

#: Terminal states where the gateway knows its own count is incomplete, mapped to
#: the reason that explains the resulting shortfall against the provider.
_PARTIAL_STATES: dict[TerminalState, DriftReason] = {
    TerminalState.CLIENT_DISCONNECT: DriftReason.CLIENT_DISCONNECT_PARTIAL,
    TerminalState.BUDGET_EXCEEDED: DriftReason.BUDGET_TRUNCATED,
    TerminalState.GATEWAY_SHUTDOWN: DriftReason.GATEWAY_CRASH_PARTIAL,
}


def attribute(
    metered: RequestMetered,
    usage: ProviderUsage | None,
    *,
    tolerance: int = 0,
    duplicate_key: bool = False,
    now: float | None = None,
) -> Settlement:
    """Join one request's two ledgers into a settlement.

    ``usage=None`` means the settlement window closed without the provider ever
    reporting. We still settle — on the gateway count alone — because an
    unsettled request that sits in ``PENDING`` forever is a silent revenue leak,
    which is exactly the failure mode this project exists to make loud.

    ``duplicate_key`` is set by the consumer when it has already seen a
    settlement for this idempotency key. That is checked first: a double-count is
    a bug in dedup, and misattributing it to a tokenizer difference would hide it.
    """
    now = now if now is not None else time.time()
    metered_total = metered.prompt_tokens + metered.completion_tokens

    if duplicate_key:
        return _drift(
            metered, usage, metered_total, DriftReason.RETRY_DOUBLE_COUNT, metered_total, now
        )

    if usage is None:
        return Settlement(
            request_id=metered.request_id,
            tenant_id=metered.tenant_id,
            model=metered.model,
            status=SettlementStatus.FORCE_SETTLED,
            metered_total=metered_total,
            provider_total=None,
            drift_tokens=0,
            drift_reason=DriftReason.UNSETTLED_TIMEOUT,
            settled_at=now,
        )

    provider_total = usage.prompt_tokens + usage.completion_tokens
    delta = provider_total - metered_total

    if abs(delta) <= tolerance:
        return Settlement(
            request_id=metered.request_id,
            tenant_id=metered.tenant_id,
            model=metered.model,
            status=SettlementStatus.SETTLED,
            metered_total=metered_total,
            provider_total=provider_total,
            settled_at=now,
        )

    if delta > 0:
        # Provider billed more than we saw. If we know the stream was cut short,
        # that fully explains it; if the stream completed cleanly, the two
        # tokenizers genuinely disagree and that is a different bug.
        reason = _PARTIAL_STATES.get(metered.terminal_state, DriftReason.TOKENIZER_MISMATCH)
    else:
        reason = DriftReason.PROVIDER_UNDERREPORT

    return _drift(metered, usage, metered_total, reason, delta, now)


def _drift(
    metered: RequestMetered,
    usage: ProviderUsage | None,
    metered_total: int,
    reason: DriftReason,
    drift_tokens: int,
    now: float,
) -> Settlement:
    return Settlement(
        request_id=metered.request_id,
        tenant_id=metered.tenant_id,
        model=metered.model,
        status=SettlementStatus.SETTLED_WITH_DRIFT,
        metered_total=metered_total,
        provider_total=(usage.prompt_tokens + usage.completion_tokens) if usage else None,
        drift_tokens=drift_tokens,
        drift_reason=reason,
        settled_at=now,
    )
