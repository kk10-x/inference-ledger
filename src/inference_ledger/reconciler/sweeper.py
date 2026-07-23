"""Force-settles requests whose provider usage never arrived.

Runs on a timer inside the reconciler process. A request still pending past
``settlement_window_seconds`` is settled on the gateway's own count and tagged
``UNSETTLED_TIMEOUT``. Without this, a broken provider-usage feed is completely
silent: requests would sit in ``pending_settlements`` forever, unbilled, and the
ledger would look healthy. The sweep count is therefore a headline metric — a
rising rate means Ledger B has stopped flowing.

The sweeper needs Ledger A to settle, but a swept request is by definition one
whose Ledger A the reconciler already consumed and buffered (or already tried to
join). Rather than re-fetch it, the sweeper reconstructs the minimum Ledger A
from the pending row: the count is unknown here, so the sweep instead settles
directly from persisted state. See :meth:`sweep` for how the count is sourced.
"""

from __future__ import annotations

import logging
import time

from inference_ledger.events import (
    DriftReason,
    Settlement,
    SettlementStatus,
)
from inference_ledger.ledger.repo import LedgerRepo

log = logging.getLogger("reconciler.sweeper")


class Sweeper:
    def __init__(self, repo: LedgerRepo, window_seconds: int, metered_lookup) -> None:
        self._repo = repo
        self._window = window_seconds
        # Returns the last-known metered total for a request_id, or None. In the
        # process this is the joiner's buffer; the sweeper stays testable by
        # taking it as a callable rather than reaching into the joiner.
        self._metered_lookup = metered_lookup

    def sweep(self, now: float | None = None) -> int:
        """Force-settle everything past the window. Returns the count swept."""
        now = now if now is not None else time.time()
        cutoff = now - self._window
        swept = 0

        for request_id, tenant_id in self._repo.expired_pending(cutoff):
            metered = self._metered_lookup(request_id)
            # A pending request with no buffered Ledger A means the reconciler
            # never saw the gateway's metered event — the gateway died before
            # emitting it. Settle at zero completion so the request leaves
            # pending; the prompt was still spent and is not written off.
            metered_total = metered.prompt_tokens + metered.completion_tokens if metered else 0
            model = metered.model if metered else "unknown"

            created = self._repo.settle(
                Settlement(
                    request_id=request_id,
                    tenant_id=tenant_id,
                    model=model,
                    status=SettlementStatus.FORCE_SETTLED,
                    metered_total=metered_total,
                    provider_total=None,
                    drift_tokens=0,
                    drift_reason=DriftReason.UNSETTLED_TIMEOUT,
                    settled_at=now,
                )
            )
            if created:
                swept += 1

        if swept:
            log.info("swept %d unsettled requests (window=%ds)", swept, self._window)
        return swept
