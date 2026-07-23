"""Per-tenant token budgets as a lazily-refilled Redis token bucket.

Enforced twice per request:

1. **At admission**, against an estimate of what the response will cost. Cheap,
   and rejects hopeless requests before spending a cent upstream.
2. **During the stream**, drawing real tokens as they arrive. This is the one
   that matters — an estimate is a guess, and a tenant at their limit must be
   stopped at the moment they cross it, not after a 4000-token response has
   already been paid for.

The mid-stream cut is deliberately graceful: the client receives a well-formed
terminal SSE frame rather than a dropped socket, and the request settles as
``BUDGET_EXCEEDED`` so the resulting shortfall against provider-reported usage is
*attributed* rather than showing up as unexplained drift.

Refill is lazy — computed from elapsed time on read — so there is no sweeper and
no per-tenant timer. The whole operation is one Lua script, because read-then-write
from the application would let two concurrent requests both see the last token.
"""

from __future__ import annotations

from dataclasses import dataclass

import redis.asyncio as redis

# KEYS[1] bucket hash. ARGV: capacity, refill_per_sec, now_ms, requested, ttl.
# Returns {granted, remaining_millitokens}.
#
# Tokens are tracked in millitokens because refill over a 50ms chunk interval is
# routinely fractional, and integer truncation would silently refill at zero.
_CONSUME_LUA = """
local bucket = KEYS[1]
local capacity = tonumber(ARGV[1]) * 1000
-- Caller passes tokens/ms; scale to the millitoken unit the bucket stores in.
local refill_per_ms = tonumber(ARGV[2]) * 1000
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4]) * 1000
local ttl = tonumber(ARGV[5])

local state = redis.call('HMGET', bucket, 'tokens', 'updated_at')
local tokens = tonumber(state[1])
local updated_at = tonumber(state[2])

if tokens == nil then
  tokens = capacity
  updated_at = now_ms
end

-- Lazy refill, clamped at capacity so an idle tenant cannot bank credit.
local elapsed = math.max(0, now_ms - updated_at)
tokens = math.min(capacity, tokens + elapsed * refill_per_ms)

local granted = 0
if tokens >= requested then
  tokens = tokens - requested
  granted = 1
end

redis.call('HMSET', bucket, 'tokens', tokens, 'updated_at', now_ms)
redis.call('EXPIRE', bucket, ttl)
return {granted, math.floor(tokens / 1000)}
"""


@dataclass(frozen=True)
class BudgetDecision:
    granted: bool
    remaining: int


class TokenBudget:
    def __init__(
        self,
        client: redis.Redis,
        capacity_tokens: int,
        refill_tokens_per_second: float,
    ) -> None:
        self._redis = client
        self._capacity = capacity_tokens
        self._refill_per_ms = refill_tokens_per_second / 1000.0
        # A bucket idle for longer than a full refill holds no information —
        # letting it expire keeps Redis bounded by active tenants, not total ones.
        self._ttl = max(60, int(capacity_tokens / max(refill_tokens_per_second, 0.001)) * 2)
        self._script = client.register_script(_CONSUME_LUA)

    async def consume(self, tenant_id: str, tokens: int, now_ms: int) -> BudgetDecision:
        """Draw ``tokens`` from the tenant's bucket. Atomic, never partial.

        A request drawing more than the bucket's entire capacity can never be
        granted; admission is expected to reject those with a clear error rather
        than letting them spin forever against a bucket that will never fill.
        """
        if tokens <= 0:
            return BudgetDecision(True, self._capacity)

        granted, remaining = await self._script(
            keys=[f"budget:{tenant_id}"],
            args=[self._capacity, self._refill_per_ms, now_ms, tokens, self._ttl],
        )
        return BudgetDecision(bool(granted), int(remaining))

    @property
    def capacity(self) -> int:
        return self._capacity
