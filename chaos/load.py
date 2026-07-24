"""Poisson-burst load generator with deliberate mid-stream disconnects.

Bursty arrivals matter. A uniform request rate rarely produces the overlapping
in-flight streams that make shutdown, rebalance and partition failures
interesting — the bugs live in the overlap. Exponential inter-arrival times
give genuine clustering, so a `SIGTERM` or a broker pause has a realistic
chance of landing while several streams are half-written.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
import uuid
from dataclasses import dataclass, field

import httpx


@dataclass
class LoadStats:
    sent: int = 0
    completed: int = 0
    disconnected: int = 0
    rejected: int = 0
    errored: int = 0
    request_ids: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"sent={self.sent} completed={self.completed} disconnected={self.disconnected} "
            f"rejected={self.rejected} errored={self.errored}"
        )


async def _one_request(
    client: httpx.AsyncClient,
    stats: LoadStats,
    tenant: str,
    disconnect_rate: float,
    rng: random.Random,
) -> None:
    idem = f"load-{uuid.uuid4()}"
    body = {
        "model": "gpt-4o-mini",
        "stream": True,
        "messages": [{"role": "user", "content": "reconcile these two ledgers please"}],
    }
    cut_after = None
    if rng.random() < disconnect_rate:
        # Hang up partway through, the way a real client closing a tab does.
        cut_after = rng.randint(2, 12)

    stats.sent += 1
    try:
        async with client.stream(
            "POST",
            "/v1/chat/completions",
            json=body,
            headers={"Idempotency-Key": idem, "X-Tenant-Id": tenant},
        ) as response:
            if response.status_code == 429:
                stats.rejected += 1
                return
            if response.status_code >= 400:
                stats.errored += 1
                return

            rid = response.headers.get("X-Request-Id")
            if rid:
                stats.request_ids.append(rid)

            seen = 0
            async for _line in response.aiter_lines():
                seen += 1
                if cut_after is not None and seen >= cut_after:
                    stats.disconnected += 1
                    # Closing the response mid-iteration is the disconnect.
                    await response.aclose()
                    return
            stats.completed += 1
    except (httpx.HTTPError, httpx.StreamError):
        # A deliberate disconnect surfaces here too; only count it once.
        if cut_after is None:
            stats.errored += 1


async def generate(
    base_url: str,
    rps: float,
    duration: float,
    disconnect_rate: float = 0.0,
    tenants: int = 4,
    seed: int = 7,
) -> LoadStats:
    rng = random.Random(seed)
    stats = LoadStats()
    tasks: set[asyncio.Task] = set()
    deadline = time.monotonic() + duration

    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    async with httpx.AsyncClient(
        base_url=base_url, timeout=httpx.Timeout(60.0), limits=limits
    ) as client:
        while time.monotonic() < deadline:
            tenant = f"tenant-{rng.randrange(tenants)}"
            task = asyncio.create_task(_one_request(client, stats, tenant, disconnect_rate, rng))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            # Exponential inter-arrival = Poisson process = real bursts.
            await asyncio.sleep(rng.expovariate(rps) if rps > 0 else 0.01)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--rps", type=float, default=20.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--disconnect-rate", type=float, default=0.0)
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    stats = asyncio.run(
        generate(args.url, args.rps, args.duration, args.disconnect_rate, args.tenants, args.seed)
    )
    print(stats.summary())
    return 0


if __name__ == "__main__":
    sys.exit(main())
