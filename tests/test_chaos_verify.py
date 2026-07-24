"""The suite's own pass/fail logic, tested — a harness that cannot fail is decoration."""

from chaos.verify import Report, check


def report(**kwargs) -> Report:
    base = {"scenario": "test", "started": 100, "settled": 100}
    base.update(kwargs)
    return Report(**base)


def test_clean_run_passes():
    assert check(report(), expected_reasons=frozenset()).passed


def test_missing_requests_fail():
    r = check(report(settled=97), expected_reasons=frozenset())
    assert not r.passed
    assert "unaccounted requests" in r.failures[0]
    assert r.accounted_pct == 97.0


def test_still_pending_fails_because_the_sweeper_should_have_caught_them():
    r = check(report(still_pending=3), expected_reasons=frozenset())
    assert not r.passed
    assert any("still pending" in f for f in r.failures)


def test_unattributed_drift_fails():
    """Drift with no reason code is unexplained money — as bad as a lost request."""
    r = check(report(unattributed_drift_tokens=42), expected_reasons=frozenset())
    assert not r.passed
    assert any("no reason code" in f for f in r.failures)


def test_any_drift_fails_when_none_is_expected():
    """An empty expected set means 'no drift permitted', not 'skip the check'.

    The permissive reading let a fault-free baseline pass while a third of its
    requests were force-settling — the harness was grading a clean run on
    accounting alone and reporting success.
    """
    r = check(report(reasons={"unsettled_timeout": 100}), expected_reasons=frozenset())
    assert not r.passed
    assert any("unexpected drift reasons" in f for f in r.failures)


def test_expected_drift_passes():
    r = check(
        report(reasons={"client_disconnect_partial": 12}),
        expected_reasons=frozenset({"client_disconnect_partial"}),
    )
    assert r.passed


def test_unexpected_drift_reason_fails_even_when_totals_reconcile():
    """Converging for the wrong reason is still a failure."""
    r = check(
        report(reasons={"provider_underreport": 5}),
        expected_reasons=frozenset({"client_disconnect_partial"}),
    )
    assert not r.passed
    assert any("unexpected drift reasons" in f for f in r.failures)


def test_double_counts_fail_by_default():
    r = check(report(double_counts=2), expected_reasons=frozenset())
    assert not r.passed
    assert any("billed twice" in f for f in r.failures)


def test_double_counts_allowed_only_where_the_scenario_expects_them():
    r = check(
        report(double_counts=2, reasons={"retry_double_count": 2}),
        expected_reasons=frozenset({"retry_double_count"}),
        allow_double_counts=True,
    )
    assert r.passed


def test_zero_requests_is_a_failure_not_a_vacuous_pass():
    """A load generator that never ran must not report success."""
    r = check(report(started=0, settled=0), expected_reasons=frozenset())
    assert not r.passed
    assert "load generator did not run" in r.failures[0]
