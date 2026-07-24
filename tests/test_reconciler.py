"""Reconciler and sweeper against an in-memory ledger — no broker, no database."""

import pytest

from inference_ledger import topics
from inference_ledger.events import (
    DriftReason,
    ProviderUsage,
    RequestMetered,
    RequestStarted,
    SettlementStatus,
    TerminalState,
)
from inference_ledger.ledger.repo import InMemoryLedger
from inference_ledger.reconciler.consumer import Reconciler
from inference_ledger.reconciler.joiner import PendingJoiner
from inference_ledger.reconciler.sweeper import Sweeper


def started(rid="req-1", at=1000.0):
    return RequestStarted(
        request_id=rid,
        idempotency_key=f"key-{rid}",
        tenant_id="acme",
        model="gpt-4o-mini",
        prompt_tokens=50,
        started_at=at,
    ).model_dump()


def metered(rid="req-1", completion=100, state=TerminalState.COMPLETED):
    return RequestMetered(
        request_id=rid,
        idempotency_key=f"key-{rid}",
        tenant_id="acme",
        model="gpt-4o-mini",
        prompt_tokens=50,
        completion_tokens=completion,
        terminal_state=state,
        ended_at=1001.0,
    ).model_dump()


def usage(rid="req-1", completion=100):
    return ProviderUsage(
        request_id=rid, prompt_tokens=50, completion_tokens=completion, reported_at=1002.0
    ).model_dump()


@pytest.fixture
def setup():
    repo = InMemoryLedger()
    drift_events = []
    reconciler = Reconciler(repo, drift_events.append)
    return repo, reconciler, drift_events


def test_full_join_writes_one_clean_settlement(setup):
    repo, reconciler, drift = setup
    reconciler.handle(topics.REQUESTS_STARTED, started())
    reconciler.handle(topics.REQUESTS_METERED, metered())
    reconciler.handle(topics.PROVIDER_USAGE, usage())

    assert len(repo.settlements) == 1
    assert repo.settlements["req-1"].status is SettlementStatus.SETTLED
    assert repo.pending == {}  # started recorded it, settle cleared it
    assert drift == []  # a clean settlement raises no drift


def test_disagreement_writes_settlement_and_publishes_drift(setup):
    repo, reconciler, drift = setup
    reconciler.handle(
        topics.REQUESTS_METERED, metered(completion=60, state=TerminalState.CLIENT_DISCONNECT)
    )
    reconciler.handle(topics.PROVIDER_USAGE, usage(completion=100))

    assert repo.settlements["req-1"].status is SettlementStatus.SETTLED_WITH_DRIFT
    assert len(drift) == 1
    assert drift[0].reason is DriftReason.CLIENT_DISCONNECT_PARTIAL
    assert drift[0].drift_tokens == 40


def test_duplicate_delivery_is_absorbed_by_the_ledger(setup):
    repo, reconciler, drift = setup
    reconciler.handle(topics.REQUESTS_METERED, metered())
    reconciler.handle(topics.PROVIDER_USAGE, usage())
    # The whole join redelivered — at-least-once in action.
    reconciler.handle(topics.REQUESTS_METERED, metered())
    reconciler.handle(topics.PROVIDER_USAGE, usage())

    assert len(repo.settlements) == 1
    # The redelivered metered was recognised as a double-count and re-settled,
    # but the ledger's primary key absorbed it rather than writing a second row.
    assert repo.duplicate_settle_attempts >= 1


def test_started_records_pending_for_the_sweeper(setup):
    repo, reconciler, _ = setup
    reconciler.handle(topics.REQUESTS_STARTED, started())
    assert "req-1" in repo.pending


def test_sweeper_force_settles_expired_pending():
    repo = InMemoryLedger()
    # A request that started but whose usage never came.
    repo.record_pending("req-1", "acme", started_at=1000.0)

    # Ledger A was seen by the joiner but never joined B.
    joiner = PendingJoiner()
    joiner.on_metered(RequestMetered.model_validate(metered(completion=80)))

    sweeper = Sweeper(repo, window_seconds=300, metered_lookup=lambda rid: joiner._metered.get(rid))
    swept = sweeper.sweep(now=1000.0 + 301)

    assert swept == 1
    settlement = repo.settlements["req-1"]
    assert settlement.status is SettlementStatus.FORCE_SETTLED
    assert settlement.drift_reason is DriftReason.UNSETTLED_TIMEOUT
    assert settlement.metered_total == 130  # 50 prompt + 80 completion
    assert repo.pending == {}


def test_sweeper_evicts_the_joiner_buffer_it_settles():
    """Otherwise buffered half-joins accumulate forever.

    The sweeper writes straight to the ledger, so without an eviction hook the
    joiner keeps the orphaned Ledger A indefinitely — a slow leak that also
    makes the buffered-events gauge useless as a health signal.
    """
    repo = InMemoryLedger()
    repo.record_pending("req-1", "acme", started_at=1000.0)
    joiner = PendingJoiner()
    joiner.on_metered(RequestMetered.model_validate(metered(completion=80)))
    assert joiner.buffered_metered == 1

    sweeper = Sweeper(
        repo,
        window_seconds=300,
        metered_lookup=joiner.peek_metered,
        on_settled=joiner.discard,
    )
    assert sweeper.sweep(now=1000.0 + 301) == 1
    assert joiner.buffered_metered == 0


def test_sweeper_ignores_requests_within_the_window():
    repo = InMemoryLedger()
    repo.record_pending("req-1", "acme", started_at=1000.0)
    sweeper = Sweeper(repo, window_seconds=300, metered_lookup=lambda rid: None)
    assert sweeper.sweep(now=1000.0 + 100) == 0
    assert "req-1" in repo.pending


def test_sweeper_settles_at_zero_when_ledger_a_never_arrived():
    """Gateway died before emitting Ledger A: the request must still leave pending."""
    repo = InMemoryLedger()
    repo.record_pending("req-1", "acme", started_at=1000.0)
    sweeper = Sweeper(repo, window_seconds=300, metered_lookup=lambda rid: None)
    swept = sweeper.sweep(now=1000.0 + 301)
    assert swept == 1
    assert repo.settlements["req-1"].metered_total == 0
    assert repo.settlements["req-1"].drift_reason is DriftReason.UNSETTLED_TIMEOUT
