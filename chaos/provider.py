"""A mock upstream provider that can be made to fail on demand.

The chaos suite measures whether the *ledger* stays correct while infrastructure
breaks. That requires an upstream whose behaviour is reproducible and whose
failures are deliberate — neither of which a real provider API offers. You
cannot ask OpenAI to truncate one stream in ten, or to misreport usage by four
tokens, and you certainly cannot do it a thousand times per run for free.

So this stands in. It streams real SSE, counts what it emitted with the *same*
tokenizer the gateway uses (so a healthy request genuinely agrees on both
ledgers), and injects faults on request:

``truncate_rate``
    Stop mid-response and never send a usage block — the shape of a provider
    dropping a connection.
``usage_skew``
    Report a usage block that disagrees with what was actually streamed. This is
    the one real metering tools cannot detect at all.
``no_usage_rate``
    Complete normally but omit usage, forcing the sweeper to force-settle.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from inference_ledger.tokenizer import encoder_for

app = FastAPI(title="chaos-provider")

# Deterministic by default so a rerun of the suite produces the same faults.
SEED = int(os.getenv("CHAOS_PROVIDER_SEED", "1337"))
WORDS = [
    "ledger",
    "reconcile",
    "settle",
    "drift",
    "token",
    "stream",
    "partition",
    "offset",
    "commit",
    "broker",
    "idempotent",
    "retry",
    "sweep",
    "attribute",
    "gateway",
    "tenant",
    "budget",
    "usage",
]


def _config(body: dict) -> dict:
    """Per-request fault config, overridable by header-free request extras."""
    return {
        "chunks": int(body.get("_chaos_chunks", os.getenv("CHAOS_CHUNKS", "40"))),
        "truncate_rate": float(os.getenv("CHAOS_TRUNCATE_RATE", "0")),
        "usage_skew": int(os.getenv("CHAOS_USAGE_SKEW", "0")),
        "no_usage_rate": float(os.getenv("CHAOS_NO_USAGE_RATE", "0")),
        "chunk_delay_ms": float(os.getenv("CHAOS_CHUNK_DELAY_MS", "8")),
    }


@app.post("/chat/completions")
async def chat_completions(request: Request) -> StreamingResponse:
    body = await request.json()
    cfg = _config(body)
    rng = random.Random(f"{SEED}:{time.time_ns()}")
    wants_usage = bool(body.get("stream_options", {}).get("include_usage"))
    encode = encoder_for(body.get("model", "gpt-4o-mini"))

    async def stream():
        emitted: list[str] = []
        truncate_at = (
            rng.randint(1, max(1, cfg["chunks"] - 1))
            if rng.random() < cfg["truncate_rate"]
            else None
        )

        for i in range(cfg["chunks"]):
            if truncate_at is not None and i >= truncate_at:
                # Vanish mid-stream: no terminal frame, no usage. The gateway's
                # partial count is the only record that this request happened.
                return
            word = rng.choice(WORDS) + " "
            emitted.append(word)
            yield "data: " + json.dumps({"choices": [{"delta": {"content": word}}]}) + "\n\n"
            if cfg["chunk_delay_ms"]:
                await asyncio.sleep(cfg["chunk_delay_ms"] / 1000.0)

        if wants_usage and rng.random() >= cfg["no_usage_rate"]:
            # Count what was actually emitted, with the gateway's tokenizer, so
            # a healthy request agrees on both ledgers and any disagreement the
            # suite observes is a real one rather than a units artefact.
            completion = encode("".join(emitted)) + cfg["usage_skew"]
            prompt = sum(
                encode(m.get("content", ""))
                for m in body.get("messages", [])
                if isinstance(m.get("content"), str)
            )
            usage = {
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "total_tokens": prompt + completion,
            }
            yield "data: " + json.dumps({"choices": [], "usage": usage}) + "\n\n"

        yield "data: [DONE]\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
