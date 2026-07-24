"""The failure matrix, declared once and driven by both the suite and the docs.

Each scenario asserts two things after the injected fault clears:

1. **Convergence** — every request reaches a terminal settlement. Nothing is left
   ``PENDING`` once the settlement window has elapsed.
2. **Attribution** — the drift that does appear carries the expected reason code.
   A scenario that converges *for the wrong reason* is a failure, which is why
   ``expected_reasons`` is asserted and not merely logged.

``expected_reasons`` is a permitted set, not a required one: a scenario passes
with no drift at all, but fails the moment drift appears that it cannot explain.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field

from inference_ledger.events import DriftReason

#: Both compose files, since the suite only ever runs under the chaos overlay.
COMPOSE = "docker compose -f docker-compose.yml -f docker-compose.chaos.yml"
#: The interpreter running the suite — a bare `python` would not resolve to the
#: virtualenv that has confluent-kafka installed.
PY = sys.executable


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    #: Shell command that injects the fault, run mid-load. Empty means the fault
    #: is produced by the load generator itself rather than by the harness.
    inject: str
    #: Drift reasons that are legitimate under this fault. Anything else fails.
    expected_reasons: frozenset[DriftReason] = field(default_factory=frozenset)


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        name="baseline",
        description=(
            "No fault. Establishes that a healthy run settles cleanly — without "
            "this, a suite of passing failure scenarios proves nothing."
        ),
        inject="",
    ),
    Scenario(
        name="broker-partition",
        description=(
            "Sever the gateway from Kafka for 20s. The gateway must keep serving "
            "traffic and buffer events locally; nothing may be lost on reconnect."
        ),
        inject=f"{COMPOSE} pause redpanda && sleep 20 && {COMPOSE} unpause redpanda",
    ),
    Scenario(
        name="gateway-sigkill",
        description=(
            "kill -9 the gateway with streams in flight. Those requests lose their "
            "Ledger A entirely, so the sweeper is the only thing that can settle them."
        ),
        inject=f"{COMPOSE} kill -s SIGKILL gateway && {COMPOSE} up -d gateway",
        expected_reasons=frozenset({DriftReason.UNSETTLED_TIMEOUT}),
    ),
    Scenario(
        name="gateway-sigterm-drain",
        description=(
            "Graceful shutdown mid-stream. This is the one that should produce no "
            "drift at all: streams drain, meters finalize, the producer flushes."
        ),
        inject=f"{COMPOSE} stop -t 45 gateway && {COMPOSE} up -d gateway",
        expected_reasons=frozenset({DriftReason.GATEWAY_CRASH_PARTIAL}),
    ),
    Scenario(
        name="duplicate-delivery",
        description="Replay committed offsets so every event is delivered twice.",
        inject=f"{PY} -m chaos.replay --topics requests.metered provider.usage",
    ),
    Scenario(
        name="client-disconnect",
        description=(
            "Drop 10% of client connections mid-stream. The provider bills the "
            "full completion; the gateway saw only part of it."
        ),
        inject="",  # produced by the load generator's --disconnect-rate
        expected_reasons=frozenset({DriftReason.CLIENT_DISCONNECT_PARTIAL}),
    ),
    Scenario(
        name="provider-usage-blackhole",
        description=(
            "Stop the provider reporting usage at all. Everything must force-settle "
            "on the gateway count rather than silently going unbilled."
        ),
        inject=(f"{COMPOSE} stop provider && CHAOS_NO_USAGE_RATE=1 {COMPOSE} up -d provider"),
        expected_reasons=frozenset({DriftReason.UNSETTLED_TIMEOUT}),
    ),
    Scenario(
        name="provider-usage-skew",
        description=(
            "Provider reports 4 more tokens than it streamed. No infrastructure "
            "fails — this is the silent overbilling single-ledger tools cannot see."
        ),
        inject=(f"{COMPOSE} stop provider && CHAOS_USAGE_SKEW=4 {COMPOSE} up -d provider"),
        expected_reasons=frozenset({DriftReason.TOKENIZER_MISMATCH}),
    ),
    Scenario(
        name="reconciler-rebalance",
        description="Scale the reconciler 1->3->1 under load to force partition rebalances.",
        inject=(
            f"{COMPOSE} up -d --scale reconciler=3 && sleep 25 "
            f"&& {COMPOSE} up -d --scale reconciler=1"
        ),
    ),
)
