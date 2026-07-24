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

DSN = os.getenv("POSTGRES_DSN", "postgresql://ledger:ledger@localhost:5432/ledger")
GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8080")
# Kept short for the suite; production defaults to 300s.
WINDOW = int(os.getenv("SETTLEMENT_WINDOW_SECONDS", "30"))


def _connect():
    import psycopg

    return psycopg.connect(DSN, autocommit=True)


def _reset(conn) -> None:
    conn.execute("TRUNCATE settlements, pending_settlements, idempotency_index")


def _inject(command: str) -> None:
    if not command:
        return
    subprocess.run(command, shell=True, check=False, capture_output=True)


async def _run_one(scenario, rps: float, duration: float) -> Report:
    conn = _connect()
    _reset(conn)

    disconnect_rate = 0.1 if scenario.name == "client-disconnect" else 0.0

    # Load runs concurrently with the fault; injection is fired partway in so it
    # lands on streams that are already open.
    load_task = asyncio.create_task(
        generate(GATEWAY, rps=rps, duration=duration, disconnect_rate=disconnect_rate)
    )
    await asyncio.sleep(min(5.0, duration / 3))
    await asyncio.to_thread(_inject, scenario.inject)
    stats = await load_task

    # Let the sweeper's window elapse plus a margin for the final sweep tick.
    await asyncio.sleep(WINDOW + 10)

    conn = _connect()  # reconnect: the injected fault may have killed the old one
    report = collect(conn, scenario.name, started=len(stats.request_ids))
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
    for scenario in selected:
        print(f"\n[{scenario.name}] {scenario.description}")
        started = time.monotonic()
        reports.append(await _run_one(scenario, args.rps, args.duration))
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
