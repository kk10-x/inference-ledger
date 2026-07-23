"""The ordering matrix: two ledgers, arriving in any order, possibly twice."""

import pytest

from inference_ledger.events import (
    DriftReason,
    ProviderUsage,
    RequestMetered,
    SettlementStatus,
    TerminalState,
)
from inference_ledger.reconciler.joiner import PendingJoiner


def metered(rid="req-1", completion=100, state=TerminalState.COMPLETED):
    return RequestMetered(
        request_id=rid,
        idempotency_key=f"key-{rid}",
        tenant_id="acme",
        model="gpt-4o-mini",
        prompt_tokens=50,
        completion_tokens=completion,
        terminal_state=state,
        ended_at=1000.0,
    )


def usage(rid="req-1", completion=100, prompt=50):
    return ProviderUsage(
        request_id=rid, prompt_tokens=prompt, completion_tokens=completion, reported_at=1001.0
    )


@pytest.fixture
def joiner():
    return PendingJoiner()


def test_metered_then_usage_settles_on_the_second(joiner):
    assert joiner.on_metered(metered()) is None  # A buffered, waiting for B
    settlement = joiner.on_usage(usage())
    assert settlement is not None
    assert settlement.status is SettlementStatus.SETTLED
    assert joiner.buffered_metered == 0


def test_usage_then_metered_settles_on_the_second(joiner):
    assert joiner.on_usage(usage()) is None  # B buffered, waiting for A
    settlement = joiner.on_metered(metered())
    assert settlement is not None
    assert settlement.status is SettlementStatus.SETTLED
    assert joiner.buffered_usage == 0


def test_disagreement_is_attributed_regardless_of_arrival_order(joiner):
    joiner.on_usage(usage(completion=100))
    settlement = joiner.on_metered(metered(completion=60, state=TerminalState.CLIENT_DISCONNECT))
    assert settlement.drift_reason is DriftReason.CLIENT_DISCONNECT_PARTIAL
    assert settlement.drift_tokens == 40


def test_duplicate_metered_after_settlement_is_a_double_count(joiner):
    joiner.on_metered(metered())
    joiner.on_usage(usage())  # settled
    # The gateway emits Ledger A a second time.
    dup = joiner.on_metered(metered())
    assert dup is not None
    assert dup.drift_reason is DriftReason.RETRY_DOUBLE_COUNT


def test_duplicate_usage_before_metered_does_not_double_settle(joiner):
    joiner.on_usage(usage())
    joiner.on_usage(usage())  # redelivered B, still no A
    assert joiner.buffered_usage == 1
    settlement = joiner.on_metered(metered())
    assert settlement.status is SettlementStatus.SETTLED
    # And a later duplicate A is caught as a double-count, not re-joined.
    assert joiner.on_metered(metered()).drift_reason is DriftReason.RETRY_DOUBLE_COUNT


def test_force_settle_uses_ledger_a_alone(joiner):
    joiner.on_metered(metered(completion=80))
    settlement = joiner.force_settle(metered(completion=80))
    assert settlement.status is SettlementStatus.FORCE_SETTLED
    assert settlement.drift_reason is DriftReason.UNSETTLED_TIMEOUT
    assert settlement.provider_total is None
    assert settlement.metered_total == 130


def test_independent_requests_do_not_interfere(joiner):
    joiner.on_metered(metered(rid="req-1"))
    joiner.on_usage(usage(rid="req-2"))
    assert joiner.buffered_metered == 1
    assert joiner.buffered_usage == 1
    # Completing req-1 leaves req-2 still waiting.
    assert joiner.on_usage(usage(rid="req-1")) is not None
    assert joiner.buffered_usage == 1


def test_forget_bounds_the_settled_set(joiner):
    joiner.on_metered(metered())
    joiner.on_usage(usage())
    joiner.forget("req-1")
    # After forget, a re-seen request is treated as fresh rather than a duplicate.
    # (In production this only happens past the settlement window.)
    assert joiner.on_metered(metered()) is None
