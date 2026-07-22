"""Force-settles requests whose provider usage never arrived.

Runs on a timer inside the reconciler process. Anything still ``PENDING`` past
``settlement_window_seconds`` is settled on the gateway count alone and tagged
``UNSETTLED_TIMEOUT``. The count of these is a headline metric: a rising sweep
rate means the provider-usage path is broken, and without the sweeper that
failure is completely silent.
"""

from __future__ import annotations


def sweep_once() -> int:
    """Force-settle expired pending requests. Returns how many were swept."""
    raise NotImplementedError("sweeper — milestone 2")
