"""Event-publishing seam.

The gateway depends on this protocol rather than on ``confluent-kafka`` directly,
for two reasons. Tests need to assert on published events without a broker, and
``flush()`` needs to be a first-class operation the shutdown path can block on —
an event enqueued but never flushed is a request that silently fails to settle.
"""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel


class EventBus(Protocol):
    def publish(self, topic: str, key: str, event: BaseModel) -> None:
        """Enqueue an event. Keyed by ``request_id`` so joins stay per-partition."""
        ...

    def flush(self, timeout: float = 10.0) -> int:
        """Block until the queue drains. Returns messages still unsent."""
        ...


class InMemoryBus:
    """Test double. Records everything; ``flush`` is a no-op that returns 0."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, BaseModel]] = []

    def publish(self, topic: str, key: str, event: BaseModel) -> None:
        self.published.append((topic, key, event))

    def flush(self, timeout: float = 10.0) -> int:
        return 0

    def events_on(self, topic: str) -> list[BaseModel]:
        return [event for published_topic, _, event in self.published if published_topic == topic]


class KafkaBus:
    """Production bus. ``confluent-kafka`` is imported lazily so that developer
    machines and CI can run the test suite without the C extension installed."""

    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                # Durability over latency: this is a billing ledger.
                "enable.idempotence": True,
                "acks": "all",
                # Buffer through short broker outages rather than failing requests.
                # The chaos suite's broker-partition scenario depends on this.
                "queue.buffering.max.ms": 50,
                "message.timeout.ms": 120_000,
                "compression.type": "lz4",
            }
        )

    def publish(self, topic: str, key: str, event: BaseModel) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=event.model_dump_json().encode(),
        )
        # Serve delivery callbacks without blocking the request path.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
