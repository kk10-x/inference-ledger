"""Run each scenario under load, then assert the ledger survived it.

Shape of a single scenario:

1. Truncate the ledger so the run is measured in isolation.
2. Start load and let it reach steady state.
3. Inject the fault while streams are in flight — the overlap is the point.
4. Stop injecting, let the load finish, then wait one full settlement window so
   the sweeper has had its chance.
5. Query Postgres and apply the invariants in ``verify.py``.

The output table is the artefact that belongs in the README.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time

from chaos import scenarios as scenario_defs
from chaos.load import generate
from chaos.verify import Report, check, collect

#: Unique per invocation. Scenario index alone is not enough: the ledger is not
#: truncated between runs (deliberately — the rows are evidence), so a fixed
#: prefix makes a rerun count the previous run's settlements as its own. That is
#: precisely how a clean baseline reported 199% accounted.
RUN_ID = f"r{int(time.time()) % 100_000:05d}"

DSN = os.getenv("POSTGRES_DSN", "postgresql://ledger:ledger@localhost:5432/ledger")
GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8080")
# Kept short for the suite; production defaults to 300s.
WINDOW = int(os.getenv("SETTLEMENT_WINDOW_SECONDS", "30"))


def _connect():
    import psycopg

    return psycopg.connect(DSN, autocommit=True)


def _run(command: str) -> None:
    if not command:
        return
    subprocess.run(command, shell=True, check=False, capture_output=True)


def _restore_provider() -> None:
    """Put the mock provider back to fault-free defaults.

    Scenarios mutate it by restarting it with CHAOS_* env set. Compose only
    recreates a container when its resolved config changes, so without an
    explicit restore a fault silently persists into whichever scenario runs
    next — which is exactly how usage-skew leaked into the rebalance run and
    produced tokenizer_mismatch drift nobody had injected.
    """
    _run(f"{scenario_defs.COMPOSE} up -d --force-recreate provider")
    # Recreating the provider leaves the gateway holding a pool of dead
    # keepalive connections. The gateway retries once, which clears one dead
    # connection per request — so a burst immediately afterwards still sees a
    # small residual failure rate as requests find *other* stale sockets. That
    # is a genuine property of connection pooling across an upstream restart,
    # not something the harness should hide; but it must not contaminate the
    # measurement of an unrelated fault either. Restarting the gateway empties
    # the pool, so each scenario starts from a known-clean connection state.
    _run(f"{scenario_defs.COMPOSE} restart gateway")
    _await_gateway()


def _await_gateway(timeout: float = 60.0) -> None:
    """Block until the gateway can actually serve a request end to end.

    A fixed sleep is not enough. Recreating the provider also invalidates the
    gateway's pooled keepalive connections, so the first requests after a
    restart can fail on a stale socket — which shows up as provider_error
    settlements that look like a system bug but are really the harness starting
    load too early.
    """
    import httpx

    probe = {
        "model": "gpt-4o-mini",
        "stream": True,
        "messages": [{"role": "user", "content": "readiness probe"}],
    }
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with httpx.Client(base_url=GATEWAY, timeout=15.0) as client:
                response = client.post(
                    "/v1/chat/completions",
                    json=probe,
                    headers={"X-Tenant-Id": "warmup", "Idempotency-Key": f"warmup-{time.time()}"},
                )
                if response.status_code == 200 and '"error"' not in response.text:
                    return
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
    print("    warning: gateway did not become ready; results may be unreliable")


async def _run_one(scenario, index: int, rps: float, duration: float) -> Report:
    # Tenants are namespaced per scenario so counting survives a shared cluster:
    # topics, reconciler offsets and join buffers all outlive a TRUNCATE.
    prefix = f"{RUN_ID}-s{index}"
    _restore_provider()

    disconnect_rate = 0.1 if scenario.name == "client-disconnect" else 0.0

    # Load runs concurrently with the fault; injection is fired partway in so it
    # lands on streams that are already open.
    load_task = asyncio.create_task(
        generate(
            GATEWAY,
            rps=rps,
            duration=duration,
            disconnect_rate=disconnect_rate,
            tenant_prefix=prefix,
        )
    )
    await asyncio.sleep(min(5.0, duration / 3))
    await asyncio.to_thread(_run, scenario.inject)
    stats = await load_task

    # Let the sweeper's window elapse plus a margin for the final sweep tick.
    await asyncio.sleep(WINDOW + 15)

    conn = _connect()  # connect late: the injected fault may have killed the DB link
    report = collect(conn, scenario.name, len(stats.request_ids), tenant_prefix=f"{prefix}-%")
    report = check(
        report,
        expected_reasons=frozenset(r.value for r in scenario.expected_reasons),
        allow_double_counts=scenario.name == "duplicate-delivery",
    )
    print(f"    load: {stats.summary()}")
    return report


def _print_table(reports: list[Report]) -> None:
    print()
    print(f"{'scenario':<28} {'reqs':>6} {'accounted':>10} {'pending':>8}  result")
    print("-" * 78)
    for r in reports:
        status = "PASS" if r.passed else "FAIL"
        print(
            f"{r.scenario:<28} {r.started:>6} {r.accounted_pct:>9.2f}% "
            f"{r.still_pending:>8}  {status}"
        )
        for reason, count in sorted(r.reasons.items()):
            tokens = r.drift_tokens_by_reason.get(reason, 0)
            print(f"{'':<28} └─ {reason}: {count} requests, {tokens} tokens")
        for failure in r.failures:
            print(f"{'':<28} !! {failure}")
    print("-" * 78)


async def _main_async(args) -> int:
    selected = [s for s in scenario_defs.SCENARIOS if not args.only or s.name in args.only]
    if not selected:
        print(f"no scenarios matched {args.only}", file=sys.stderr)
        return 2

    reports: list[Report] = []
    for index, scenario in enumerate(selected):
        print(f"\n[{scenario.name}] {scenario.description}")
        started = time.monotonic()
        reports.append(await _run_one(scenario, index, args.rps, args.duration))
        print(f"    took {time.monotonic() - started:.0f}s")

    _print_table(reports)
    failed = [r for r in reports if not r.passed]
    if failed:
        print(f"\n{len(failed)}/{len(reports)} scenarios FAILED")
        return 1
    print(f"\nall {len(reports)} scenarios passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rps", type=float, default=25.0)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--only", nargs="*", default=None, help="scenario names to run")
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    sys.exit(main())
