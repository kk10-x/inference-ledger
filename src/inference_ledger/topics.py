"""Kafka topic names.

All topics are keyed by ``request_id`` so that every event for one request lands
on the same partition. That is what lets the reconciler do its join with plain
per-partition state instead of a distributed lookup.
"""

REQUESTS_STARTED = "requests.started"
REQUESTS_METERED = "requests.metered"
PROVIDER_USAGE = "provider.usage"
SETTLEMENTS = "settlements"
DRIFT = "drift"

ALL = (REQUESTS_STARTED, REQUESTS_METERED, PROVIDER_USAGE, SETTLEMENTS, DRIFT)
