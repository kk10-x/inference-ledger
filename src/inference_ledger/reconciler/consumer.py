"""Windowed join of ``requests.metered`` against ``provider.usage``.

Design notes (implementation follows in the weekend-2 milestone):

* One consumer group subscribed to both topics. Because every event is keyed by
  ``request_id``, both sides of a join always land on the same partition, so the
  pending-state map can be plain per-partition memory rather than a distributed
  store.
* Pending state is rebuilt on rebalance by replaying from the last committed
  offset. The join is therefore idempotent under duplicate delivery: settling a
  ``request_id`` twice must be a no-op, enforced by the primary key on
  ``settlements``.
* Offsets are committed **after** the Postgres write, not before. That makes the
  pipeline at-least-once end to end, and the ``ON CONFLICT DO NOTHING`` insert
  turns it into effectively-once at the ledger.
* Whichever side of the join arrives second triggers attribution via
  :func:`inference_ledger.reconciler.join.attribute`.
"""

from __future__ import annotations


def run() -> None:
    """Entry point for the reconciler process."""
    raise NotImplementedError("reconciler consumer loop — milestone 2")
