"""Idempotency-key dedup, borrowed wholesale from payments-API practice.

A key moves through three states in Redis: ``IN_FLIGHT`` -> ``COMPLETED``, or
``IN_FLIGHT`` -> expired-by-crash. A retry that arrives while the original is
``IN_FLIGHT`` gets 409, not a second upstream call — otherwise one client retry
storm bills the tenant twice.

The subtle case is a gateway crash: the key is left ``IN_FLIGHT`` with a short
TTL so a legitimate retry can proceed once it lapses, while the reconciler still
sees the original partial as ``GATEWAY_CRASH_PARTIAL``.
"""

from __future__ import annotations


def claim(key: str, tenant_id: str) -> bool:
    """Atomically claim an idempotency key. False means a retry is in flight."""
    raise NotImplementedError("idempotency store — milestone 1")


def complete(key: str, request_id: str) -> None:
    """Mark a key completed so later retries resolve to the same request."""
    raise NotImplementedError("idempotency store — milestone 1")
