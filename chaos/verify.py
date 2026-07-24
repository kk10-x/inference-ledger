"""What "converged" means, stated precisely enough to fail on.

It is tempting to claim the system "drives drift to zero". That would be a lie.
Drift is *expected*: when a client hangs up at token 800 of 1200 the provider
really did bill tokens we never delivered, and the honest ledger records that
gap. A suite that reported zero drift under injected disconnects would only be
proving it had stopped looking.

The real invariants are these three, and they are what the suite asserts:

**1. Accountability** — every request that started reaches a terminal
settlement. ``settlements == started`` and ``pending_settlements`` is empty once
the settlement window has elapsed. A request that simply vanishes is the
failure mode this project exists to eliminate.

**2. Attribution** — every token of drift carries a reason code. Drift with a
NULL reason is unexplained money, which is exactly as bad as a missing request.

**3. No double-billing** — one settlement row per request, and zero
``retry_double_count`` rows in any scenario where dedup is supposed to hold.

Scenario-specific expectations layer on top: a disconnect run *should* produce
``client_disconnect_partial`` drift, and seeing something else is a failure even
if the three invariants hold.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Report:
    scenario: str
    started: int = 0
    settled: int = 0
    still_pending: int = 0
    unattributed_drift_tokens: int = 0
    double_counts: int = 0
    reasons: dict[str, int] = field(default_factory=dict)
    drift_tokens_by_reason: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    @property
    def accounted_pct(self) -> float:
        return 100.0 * self.settled / self.started if self.started else 0.0

    @property
    def passed(self) -> bool:
        return not self.failures


def check(
    report: Report,
    *,
    expected_reasons: frozenset[str],
    allow_double_counts: bool = False,
) -> Report:
    """Apply the three invariants plus the scenario's own expectations."""
    if report.started == 0:
        report.failures.append("no requests were recorded — the load generator did not run")
        return report

    if report.settled != report.started:
        report.failures.append(
            f"unaccounted requests: {report.started} started, {report.settled} settled "
            f"({report.started - report.settled} missing)"
        )

    if report.still_pending:
        report.failures.append(
            f"{report.still_pending} requests still pending past the settlement window "
            "— the sweeper did not force-settle them"
        )

    if report.unattributed_drift_tokens:
        report.failures.append(
            f"{report.unattributed_drift_tokens} drift tokens carry no reason code"
        )

    if report.double_counts and not allow_double_counts:
        report.failures.append(
            f"{report.double_counts} requests were billed twice (retry_double_count)"
        )

    if expected_reasons:
        unexpected = set(report.reasons) - set(expected_reasons)
        if unexpected:
            report.failures.append(
                f"unexpected drift reasons: {sorted(unexpected)} "
                f"(expected only {sorted(expected_reasons)})"
            )

    return report


SQL_STARTED = "SELECT count(*) FROM settlements WHERE tenant_id LIKE %s"
SQL_PENDING = "SELECT count(*) FROM pending_settlements"
SQL_UNATTRIBUTED = (
    "SELECT coalesce(sum(abs(drift_tokens)), 0) FROM settlements "
    "WHERE drift_reason IS NULL AND drift_tokens <> 0"
)
SQL_REASONS = (
    "SELECT drift_reason, count(*), coalesce(sum(abs(drift_tokens)), 0) FROM settlements "
    "WHERE drift_reason IS NOT NULL GROUP BY drift_reason"
)


def collect(conn, scenario: str, started: int, tenant_prefix: str = "%") -> Report:
    """Build a Report from the live ledger."""
    report = Report(scenario=scenario, started=started)
    report.settled = conn.execute(SQL_STARTED, (tenant_prefix,)).fetchone()[0]
    report.still_pending = conn.execute(SQL_PENDING).fetchone()[0]
    report.unattributed_drift_tokens = int(conn.execute(SQL_UNATTRIBUTED).fetchone()[0])
    for reason, count, tokens in conn.execute(SQL_REASONS).fetchall():
        report.reasons[reason] = count
        report.drift_tokens_by_reason[reason] = int(tokens)
    report.double_counts = report.reasons.get("retry_double_count", 0)
    return report
