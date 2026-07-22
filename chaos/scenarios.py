"""The failure matrix, declared once and driven by both the suite and the docs.

Each scenario asserts two things after the injected fault clears:

1. **Convergence** — every request reaches a terminal settlement. Nothing is left
   ``PENDING`` once the settlement window has elapsed.
2. **Attribution** — the drift that does appear carries the expected reason code.
   A scenario that converges to zero drift *for the wrong reason* is a failure,
   which is why ``expected_reasons`` is asserted and not merely logged.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from inference_ledger.events import DriftReason


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    #: Shell command that injects the fault, run mid-load.
    inject: str
    #: Drift reasons that are legitimate under this fault. Anything else fails.
    expected_reasons: frozenset[DriftReason] = field(default_factory=frozenset)
    #: Tokens of net drift tolerated once the system has settled.
    max_residual_drift: int = 0


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        name="broker-partition",
        description=(
            "Sever the gateway from Kafka for 20s. The gateway must keep serving "
            "traffic and buffer events locally; nothing may be lost on reconnect."
        ),
        inject="docker compose pause redpanda && sleep 20 && docker compose unpause redpanda",
    ),
    Scenario(
        name="gateway-sigkill",
        description=(
            "kill -9 the gateway with streams in flight. Those requests can only "
            "settle from the provider side; the sweeper must catch the rest."
        ),
        inject="docker compose kill -s SIGKILL gateway && docker compose up -d gateway",
        expected_reasons=frozenset({DriftReason.UNSETTLED_TIMEOUT}),
    ),
    Scenario(
        name="gateway-sigterm-drain",
        description=(
            "Graceful shutdown mid-stream. This is the one that should produce no "
            "drift at all: streams drain, meters finalize, the producer flushes."
        ),
        inject="docker compose stop -t 45 gateway && docker compose up -d gateway",
    ),
    Scenario(
        name="duplicate-delivery",
        description="Replay a committed offset range so every event is delivered twice.",
        inject="python -m chaos.replay --topic requests.metered --last 500",
    ),
    Scenario(
        name="client-disconnect",
        description="Drop 10% of client connections mid-stream.",
        inject="python -m chaos.load --disconnect-rate 0.1 --duration 60",
        expected_reasons=frozenset({DriftReason.CLIENT_DISCONNECT_PARTIAL}),
    ),
    Scenario(
        name="provider-usage-blackhole",
        description=(
            "Stop the provider-usage feed entirely. Everything must force-settle "
            "on the gateway count rather than silently going unbilled."
        ),
        inject="docker compose exec -T gateway touch /tmp/blackhole-usage && sleep 90",
        expected_reasons=frozenset({DriftReason.UNSETTLED_TIMEOUT}),
    ),
    Scenario(
        name="reconciler-rebalance",
        description="Scale the reconciler 1->3->1 under load to force partition rebalances.",
        inject=(
            "docker compose up -d --scale reconciler=3 && sleep 30 "
            "&& docker compose up -d --scale reconciler=1"
        ),
    ),
)
