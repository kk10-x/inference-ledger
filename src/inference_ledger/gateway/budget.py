"""Per-tenant token budgets as a Redis token bucket.

Enforced twice: once at admission using an estimated cost, and again *during*
the stream. The mid-stream check is the interesting one — the response is cut,
the client gets a well-formed terminal SSE frame rather than a dropped socket,
and the request settles as :attr:`TerminalState.BUDGET_EXCEEDED` so the
resulting gap against provider-reported usage is attributed rather than
appearing as mystery drift.
"""

from __future__ import annotations


def try_consume(tenant_id: str, tokens: int) -> bool:
    """Draw ``tokens`` from the tenant's bucket. False means the budget is spent."""
    raise NotImplementedError("budget bucket — milestone 1")
