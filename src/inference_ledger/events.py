"""Event schemas and the settlement state machine.

Two independent ledgers are kept for every request:

* **Ledger A** — ``RequestMetered``: tokens the gateway counted off the wire as
  it proxied the stream. Always present, even when the stream dies early.
* **Ledger B** — ``ProviderUsage``: tokens the provider says it billed. Arrives
  asynchronously, may never arrive at all.

Reconciliation is the join of A and B. A settlement is only clean when both
ledgers exist and agree; every other outcome carries a ``DriftReason`` that says
exactly *how* they disagreed. That reason code is the point of this project —
"usage is 3% off" is not actionable, "0.4% of streams were truncated by client
disconnect and the provider billed the full completion" is.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class TerminalState(StrEnum):
    """How a proxied stream ended, from the gateway's point of view."""

    COMPLETED = "completed"
    CLIENT_DISCONNECT = "client_disconnect"
    PROVIDER_ERROR = "provider_error"
    BUDGET_EXCEEDED = "budget_exceeded"
    GATEWAY_SHUTDOWN = "gateway_shutdown"


class DriftReason(StrEnum):
    """Why two ledgers disagreed.

    Ordering matters: the reconciler attributes drift to the *first* reason that
    applies, so the specific causes are listed before the generic ones.
    """

    RETRY_DOUBLE_COUNT = "retry_double_count"
    """Same idempotency key metered twice — dedup let a duplicate through."""

    CLIENT_DISCONNECT_PARTIAL = "client_disconnect_partial"
    """Client hung up mid-stream; provider billed tokens we never delivered."""

    BUDGET_TRUNCATED = "budget_truncated"
    """We cut the stream on a budget breach; provider billed the full response."""

    GATEWAY_CRASH_PARTIAL = "gateway_crash_partial"
    """Gateway died mid-stream; metered count is a floor, not a total."""

    TOKENIZER_MISMATCH = "tokenizer_mismatch"
    """Both ledgers are complete but our tokenizer disagrees with the provider's."""

    PROVIDER_UNDERREPORT = "provider_underreport"
    """Provider billed fewer tokens than we observed on the wire."""

    UNSETTLED_TIMEOUT = "unsettled_timeout"
    """Provider usage never arrived inside the settlement window."""


class SettlementStatus(StrEnum):
    PENDING = "pending"
    SETTLED = "settled"
    """Both ledgers present and in agreement."""
    SETTLED_WITH_DRIFT = "settled_with_drift"
    """Both ledgers present, attributed disagreement."""
    FORCE_SETTLED = "force_settled"
    """Swept after the window closed; billed on the gateway count alone."""


class RequestStarted(BaseModel):
    request_id: str
    idempotency_key: str
    tenant_id: str
    model: str
    prompt_tokens: int
    started_at: float


class RequestMetered(BaseModel):
    """Ledger A. Emitted once per request, on any terminal state."""

    request_id: str
    idempotency_key: str
    tenant_id: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    terminal_state: TerminalState
    """Partial streams are still emitted — a floor is more useful than a gap."""
    ended_at: float


class ProviderUsage(BaseModel):
    """Ledger B. May arrive before, after, or never relative to Ledger A."""

    request_id: str
    prompt_tokens: int
    completion_tokens: int
    reported_at: float


class Settlement(BaseModel):
    request_id: str
    tenant_id: str
    model: str
    status: SettlementStatus
    metered_total: int
    provider_total: int | None = None
    drift_tokens: int = 0
    drift_reason: DriftReason | None = None
    settled_at: float


class DriftEvent(BaseModel):
    request_id: str
    tenant_id: str
    reason: DriftReason
    drift_tokens: int
    detail: dict[str, str | int | float] = Field(default_factory=dict)
