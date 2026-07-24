"""Windowed join of ``requests.metered`` against ``provider.usage``.

The correctness argument, in one place:

* **Co-partitioning.** Both topics are keyed by ``request_id`` with the same
  partition count, so both sides of any join land on the same partition and are
  handled by the same consumer. The join needs no distributed state — see
  :class:`~inference_ledger.reconciler.joiner.PendingJoiner`.

* **Effectively-once.** Offsets are committed *after* the Postgres write, never
  before. That makes delivery at-least-once, and the ``ON CONFLICT DO NOTHING``
  in :meth:`PostgresLedger.settle` collapses the redelivered duplicates. Commit
  before the write and a crash in between would lose a settlement; this ordering
  cannot.

* **Rebalance safety.** On partition revocation, in-flight offsets are committed
  and pending buffers for those partitions are dropped; on assignment the state
  rebuilds by replaying from the last committed offset. A request half-joined at
  the moment of a rebalance is simply re-seen and re-joined — idempotently.
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge

from inference_ledger import topics
from inference_ledger.config import Settings
from inference_ledger.config import settings as default_settings
from inference_ledger.events import (
    DriftEvent,
    ProviderUsage,
    RequestMetered,
    RequestStarted,
    SettlementStatus,
)
from inference_ledger.ledger.repo import LedgerRepo, PostgresLedger
from inference_ledger.reconciler.joiner import PendingJoiner

log = logging.getLogger("reconciler")

CONSUMER_GROUP = "reconciler"

SETTLEMENTS = Counter("reconciler_settlements_total", "Settlements written", ["status"])
DRIFT = Counter("reconciler_drift_total", "Drift events by reason", ["reason"])
DRIFT_TOKENS = Counter("reconciler_drift_tokens_total", "Signed drift magnitude", ["reason"])
DUPLICATES = Counter(
    "reconciler_duplicate_settlements_total", "Redelivered settlements absorbed by the ledger"
)
SWEEPS = Counter("reconciler_sweeps_total", "Requests force-settled by the sweeper")
SWEEPS_DEFERRED = Counter(
    "reconciler_sweeps_deferred_total", "Sweeps skipped because the consumer was behind"
)
LAG = Gauge("reconciler_consumer_lag", "Unconsumed events across assigned partitions")
BUFFERED = Gauge("reconciler_buffered_events", "Half-joined events in memory", ["side"])


class Reconciler:
    """Drives a PendingJoiner from Kafka and persists what it produces.

    Split from the Kafka client so the whole settlement path can be tested by
    feeding raw event dicts to :meth:`handle`, with an ``InMemoryLedger`` and a
    fake bus, and no broker.
    """

    def __init__(self, repo: LedgerRepo, publish_drift, *, tolerance: int = 0) -> None:
        self._repo = repo
        self._publish_drift = publish_drift
        self.joiner = PendingJoiner(tolerance=tolerance)

    def handle(self, topic: str, value: dict) -> None:
        """Process one decoded event. Safe to call with duplicates."""
        if topic == topics.REQUESTS_STARTED:
            started = RequestStarted.model_validate(value)
            # Record the pending row so the sweeper can find a request whose
            # provider usage never arrives, even if this reconciler restarts.
            self._repo.record_pending(started.request_id, started.tenant_id, started.started_at)

        elif topic == topics.REQUESTS_METERED:
            self._commit(self.joiner.on_metered(RequestMetered.model_validate(value)))

        elif topic == topics.PROVIDER_USAGE:
            self._commit(self.joiner.on_usage(ProviderUsage.model_validate(value)))

        BUFFERED.labels("metered").set(self.joiner.buffered_metered)
        BUFFERED.labels("usage").set(self.joiner.buffered_usage)

    def _commit(self, settlement) -> None:
        if settlement is None:
            return
        created = self._repo.settle(settlement)
        if not created:
            # Duplicate delivery absorbed by the ledger's primary key. Expected
            # under at-least-once; a rising rate is the thing to alarm on.
            DUPLICATES.inc()
            log.debug("duplicate settlement absorbed: %s", settlement.request_id)
            return

        SETTLEMENTS.labels(settlement.status.value).inc()
        if (
            settlement.status
            in (
                SettlementStatus.SETTLED_WITH_DRIFT,
                SettlementStatus.FORCE_SETTLED,
            )
            and settlement.drift_reason
        ):
            DRIFT.labels(settlement.drift_reason.value).inc()
            DRIFT_TOKENS.labels(settlement.drift_reason.value).inc(abs(settlement.drift_tokens))
            self._publish_drift(
                DriftEvent(
                    request_id=settlement.request_id,
                    tenant_id=settlement.tenant_id,
                    reason=settlement.drift_reason,
                    drift_tokens=settlement.drift_tokens,
                    detail={"status": settlement.status.value},
                )
            )


METRICS_PORT = 9100

#: Offsets are committed in batches rather than per message. Correctness is
#: unaffected — messages are processed strictly in order and the Postgres write
#: precedes the commit, so committing after N of them still means "every
#: committed offset has been persisted". A synchronous commit per message costs
#: a broker round-trip each time, and the resulting lag is what let the sweeper
#: force-settle requests whose events were merely unread.
COMMIT_EVERY = 100
COMMIT_INTERVAL_SECONDS = 2.0


def consumer_lag(consumer) -> int:  # pragma: no cover - needs a broker
    """Events fetched-but-unread across assigned partitions.

    The sweeper depends on this. Force-settling on a timeout is only sound when
    we have actually read everything available: otherwise a backlog looks
    identical to a provider that never reported, and healthy requests get
    written off as ``UNSETTLED_TIMEOUT``.
    """
    total = 0
    for tp in consumer.assignment():
        try:
            _low, high = consumer.get_watermark_offsets(tp, timeout=2, cached=True)
            position = consumer.position([tp])[0].offset
        except Exception:
            # Unknown lag must never be read as "caught up".
            return -1
        if position is None or position < 0 or high < 0:
            return -1
        total += max(0, high - position)
    return total


def run(settings: Settings | None = None) -> None:  # pragma: no cover - needs a broker
    """Entry point for the reconciler process.

    Kept thin: all decision logic lives in :class:`Reconciler`, the joiner and
    the sweeper, every one of which is unit-tested. This function is only the
    Kafka plumbing, the metrics server, and the sweeper timer.

    The sweep runs *inline* on the poll loop rather than on its own thread. A
    ``confluent_kafka`` consumer is not thread-safe, and the sweeper reads the
    joiner's in-memory buffer that the consumer mutates — a background thread
    would race both. Polling with a 1s timeout gives the loop a natural tick to
    hang the periodic sweep on, single-threaded and race-free.
    """
    import contextlib
    import json
    import time

    from confluent_kafka import Consumer
    from prometheus_client import start_http_server

    from inference_ledger.bus import KafkaBus
    from inference_ledger.reconciler.sweeper import Sweeper

    settings = settings or default_settings
    logging.basicConfig(level=logging.INFO)

    bus = KafkaBus(settings.kafka_bootstrap)
    repo = PostgresLedger(settings.postgres_dsn)

    def publish_drift(event: DriftEvent) -> None:
        bus.publish(topics.DRIFT, event.request_id, event)

    reconciler = Reconciler(repo, publish_drift, tolerance=settings.drift_tolerance_tokens)
    sweeper = Sweeper(
        repo,
        settings.settlement_window_seconds,
        metered_lookup=lambda rid: reconciler.joiner._metered.get(rid),
    )

    start_http_server(METRICS_PORT)
    log.info("metrics on :%d", METRICS_PORT)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,  # we commit only after the DB write
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
        }
    )
    consumer.subscribe([topics.REQUESTS_STARTED, topics.REQUESTS_METERED, topics.PROVIDER_USAGE])
    log.info("reconciler up: group=%s bootstrap=%s", CONSUMER_GROUP, settings.kafka_bootstrap)

    # Sweep at most once per this interval; the window itself is much longer.
    sweep_every = max(5.0, settings.settlement_window_seconds / 10)
    next_sweep = time.monotonic() + sweep_every
    uncommitted = 0
    last_commit = time.monotonic()

    def commit_now() -> None:
        """Commit current positions. Every message up to here is already
        persisted, because processing is in-order and writes precede this."""
        nonlocal uncommitted, last_commit
        if uncommitted:
            consumer.commit(asynchronous=False)
            uncommitted = 0
        last_commit = time.monotonic()

    try:
        while True:
            now = time.monotonic()

            if now >= next_sweep:
                lag = consumer_lag(consumer)
                LAG.set(max(0, lag))
                if lag == 0:
                    swept = sweeper.sweep()
                    if swept:
                        SWEEPS.inc(swept)
                else:
                    # Behind, or lag unknown. A request looking "unsettled" here
                    # may simply have events we have not read yet.
                    SWEEPS_DEFERRED.inc()
                next_sweep = now + sweep_every

            if uncommitted and now - last_commit >= COMMIT_INTERVAL_SECONDS:
                commit_now()

            msg = consumer.poll(1.0)
            if msg is None:
                commit_now()  # idle: flush any partial batch
                continue
            if msg.error():
                log.warning("consume error: %s", msg.error())
                continue

            reconciler.handle(msg.topic(), json.loads(msg.value()))
            uncommitted += 1
            if uncommitted >= COMMIT_EVERY:
                commit_now()
    except KeyboardInterrupt:
        pass
    finally:
        with contextlib.suppress(Exception):
            commit_now()
        consumer.close()
        bus.flush()
