"""Redeliver already-consumed events, to prove duplicates cannot double-bill.

At-least-once delivery is not a hypothetical: a consumer that crashes between
processing a message and committing its offset will see that message again on
restart. This reads recent events straight off the topics and republishes them
verbatim, which is indistinguishable from that redelivery as far as the
reconciler is concerned.

The ledger should absorb every one of them via ``ON CONFLICT DO NOTHING`` and
the settlement count should not move.
"""

from __future__ import annotations

import argparse
import sys

DEFAULT_BOOTSTRAP = "localhost:19092"


def replay(topics: list[str], bootstrap: str, limit: int) -> int:
    from confluent_kafka import Consumer, Producer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            # A throwaway group so this never disturbs the reconciler's offsets.
            "group.id": "chaos-replay",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})

    metadata = consumer.list_topics(timeout=10)
    partitions: list[TopicPartition] = []
    for topic in topics:
        if topic not in metadata.topics:
            print(f"topic missing: {topic}", file=sys.stderr)
            continue
        for pid in metadata.topics[topic].partitions:
            partitions.append(TopicPartition(topic, pid, 0))
    consumer.assign(partitions)

    replayed = 0
    while replayed < limit:
        msg = consumer.poll(2.0)
        if msg is None:
            break  # drained
        if msg.error():
            continue
        producer.produce(topic=msg.topic(), key=msg.key(), value=msg.value())
        replayed += 1
        producer.poll(0)

    producer.flush(30)
    consumer.close()
    print(f"replayed {replayed} events across {topics}")
    return replayed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topics", nargs="+", required=True)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    replay(args.topics, args.bootstrap, args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
