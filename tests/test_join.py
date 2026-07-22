"""The failure matrix: every way two ledgers can disagree, and what we call it."""

import pytest

from inference_ledger.events import (
    DriftReason,
    ProviderUsage,
    RequestMetered,
    SettlementStatus,
    TerminalState,
)
from inference_ledger.reconciler.join import attribute


def metered(completion: int = 100, state: TerminalState = TerminalState.COMPLETED):
    return RequestMetered(
        request_id="req-1",
        idempotency_key="key-1",
        tenant_id="acme",
        model="gpt-4o-mini",
        prompt_tokens=50,
        completion_tokens=completion,
        terminal_state=state,
        ended_at=1000.0,
    )


def usage(completion: int = 100, prompt: int = 50):
    return ProviderUsage(
        request_id="req-1", prompt_tokens=prompt, completion_tokens=completion, reported_at=1001.0
    )


def test_agreeing_ledgers_settle_clean():
    s = attribute(metered(), usage(), now=1002.0)
    assert s.status is SettlementStatus.SETTLED
    assert s.drift_reason is None
    assert s.drift_tokens == 0


def test_missing_provider_usage_is_force_settled_not_dropped():
    s = attribute(metered(), None, now=1002.0)
    assert s.status is SettlementStatus.FORCE_SETTLED
    assert s.drift_reason is DriftReason.UNSETTLED_TIMEOUT
    assert s.provider_total is None
    # Billed on our own count — the tenant is never charged zero for real work.
    assert s.metered_total == 150


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        (TerminalState.CLIENT_DISCONNECT, DriftReason.CLIENT_DISCONNECT_PARTIAL),
        (TerminalState.BUDGET_EXCEEDED, DriftReason.BUDGET_TRUNCATED),
        (TerminalState.GATEWAY_SHUTDOWN, DriftReason.GATEWAY_CRASH_PARTIAL),
    ],
)
def test_truncated_streams_are_attributed_to_their_cause(state, reason):
    # We saw 60 completion tokens; the provider billed the full 100.
    s = attribute(metered(completion=60, state=state), usage(completion=100), now=1002.0)
    assert s.status is SettlementStatus.SETTLED_WITH_DRIFT
    assert s.drift_reason is reason
    assert s.drift_tokens == 40


def test_clean_stream_that_disagrees_is_a_tokenizer_problem():
    s = attribute(metered(completion=98), usage(completion=100), now=1002.0)
    assert s.drift_reason is DriftReason.TOKENIZER_MISMATCH
    assert s.drift_tokens == 2


def test_provider_billing_less_than_we_saw():
    s = attribute(metered(completion=100), usage(completion=90), now=1002.0)
    assert s.drift_reason is DriftReason.PROVIDER_UNDERREPORT
    assert s.drift_tokens == -10


def test_duplicate_key_beats_every_other_explanation():
    # A double-count must not be laundered into "tokenizer mismatch".
    s = attribute(metered(completion=98), usage(completion=100), duplicate_key=True, now=1002.0)
    assert s.drift_reason is DriftReason.RETRY_DOUBLE_COUNT
    assert s.drift_tokens == 148


def test_tolerance_absorbs_small_disagreement():
    s = attribute(metered(completion=98), usage(completion=100), tolerance=5, now=1002.0)
    assert s.status is SettlementStatus.SETTLED
