"""FastAPI app exposing an OpenAI-compatible surface.

Endpoints:

* ``POST /v1/chat/completions`` — proxied upstream. Streaming responses are
  relayed chunk-by-chunk with no buffering, so time-to-first-token is unaffected
  by metering.
* ``GET /healthz`` / ``GET /readyz`` — liveness and readiness. Readiness flips
  false the instant shutdown begins, so Kubernetes stops routing new traffic
  while in-flight streams drain. Liveness stays true throughout: being killed for
  "failing" liveness during a deliberate drain is a self-inflicted outage.
* ``GET /metrics`` — Prometheus.

The shutdown sequence is the hard part and is specified in ``docs/shutdown.md``.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
import redis.asyncio as redis
from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from inference_ledger import topics
from inference_ledger.bus import EventBus, KafkaBus
from inference_ledger.config import Settings
from inference_ledger.config import settings as default_settings
from inference_ledger.events import RequestStarted, TerminalState
from inference_ledger.gateway.budget import TokenBudget
from inference_ledger.gateway.idempotency import ClaimOutcome, IdempotencyStore
from inference_ledger.gateway.metering import StreamMeter
from inference_ledger.tokenizer import encoder_for

REQUESTS = Counter(
    "gateway_requests_total", "Requests by terminal state", ["tenant", "terminal_state"]
)
TOKENS_METERED = Counter("gateway_tokens_metered_total", "Ledger A tokens", ["tenant", "model"])
BUDGET_REJECTIONS = Counter(
    "gateway_budget_rejections_total", "Rejections by stage", ["tenant", "stage"]
)
IDEMPOTENCY_OUTCOMES = Counter(
    "gateway_idempotency_outcomes_total", "Idempotency claim outcomes", ["outcome"]
)
TTFT = Histogram(
    "gateway_time_to_first_token_seconds",
    "Time to first token",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
IN_FLIGHT = Gauge("gateway_streams_in_flight", "Streams currently open")


@dataclass
class StreamSession:
    """One open stream, tracked so shutdown can settle it."""

    meter: StreamMeter
    tenant_id: str
    idempotency_key: str
    settled: bool = False
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class GatewayState:
    settings: Settings
    bus: EventBus
    idempotency: IdempotencyStore
    budget: TokenBudget
    client: httpx.AsyncClient
    sessions: dict[str, StreamSession] = field(default_factory=dict)
    ready: bool = True

    async def settle(self, request_id: str, terminal_state: TerminalState) -> None:
        """Emit Ledger A exactly once for a request.

        Guarded because two paths race to settle the same stream: the normal end
        of the response generator, and the shutdown drain. Whichever arrives
        first wins; a second settlement would be a duplicate billing record.
        """
        session = self.sessions.get(request_id)
        if session is None:
            return
        async with session._lock:
            if session.settled:
                return
            session.settled = True

        metered = session.meter.finalize(terminal_state, ended_at=time.time())
        self.bus.publish(topics.REQUESTS_METERED, request_id, metered)
        await self.idempotency.complete(session.tenant_id, session.idempotency_key, request_id)

        REQUESTS.labels(session.tenant_id, terminal_state.value).inc()
        TOKENS_METERED.labels(session.tenant_id, metered.model).inc(
            metered.prompt_tokens + metered.completion_tokens
        )
        self.sessions.pop(request_id, None)
        IN_FLIGHT.set(len(self.sessions))


def build_state(settings: Settings) -> GatewayState:
    client = redis.from_url(settings.redis_url, decode_responses=True)
    return GatewayState(
        settings=settings,
        bus=KafkaBus(settings.kafka_bootstrap),
        idempotency=IdempotencyStore(client, settings.idempotency_ttl_seconds),
        budget=TokenBudget(
            client, settings.tenant_budget_tokens, settings.tenant_refill_tokens_per_second
        ),
        client=httpx.AsyncClient(
            base_url=settings.provider_base_url,
            timeout=httpx.Timeout(connect=10.0, read=300.0, write=30.0, pool=10.0),
        ),
    )


async def drain(state: GatewayState, grace_seconds: float, flush_reserve: float) -> None:
    """Let open streams finish, then settle the rest and flush the bus.

    ``flush_reserve`` is carved out of the grace period up front rather than
    hoped for at the end. An event enqueued but not flushed dies with the
    process, and a request whose Ledger A entry never left the pod is one that
    gets force-settled later as ``UNSETTLED_TIMEOUT`` — real money, silently
    wrong. Reserving the time is what makes that impossible.
    """
    state.ready = False
    deadline = time.monotonic() + max(0.0, grace_seconds - flush_reserve)

    while state.sessions and time.monotonic() < deadline:
        await asyncio.sleep(0.1)

    # Anything still open at the deadline is settled on its partial count. A
    # floor is strictly better than a gap.
    for request_id in list(state.sessions):
        await state.settle(request_id, TerminalState.GATEWAY_SHUTDOWN)

    await asyncio.to_thread(state.bus.flush, flush_reserve)
    await state.client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state: GatewayState = getattr(app.state, "gateway", None) or build_state(default_settings)
    app.state.gateway = state
    try:
        yield
    finally:
        await drain(
            state,
            grace_seconds=state.settings.shutdown_grace_seconds,
            flush_reserve=state.settings.shutdown_flush_reserve_seconds,
        )


router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    state: GatewayState = request.app.state.gateway
    if not state.ready:
        return JSONResponse({"status": "draining"}, status_code=503)
    return JSONResponse({"status": "ready"})


@router.get("/metrics")
async def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)


def _count_prompt_tokens(body: dict, model: str) -> int:
    encode = encoder_for(model)
    total = 0
    for message in body.get("messages", []):
        content = message.get("content")
        if isinstance(content, str):
            total += encode(content)
        elif isinstance(content, list):
            # Multimodal content parts; only text is counted here.
            total += sum(encode(p.get("text", "")) for p in content if isinstance(p, dict))
    return total


def _terminal_frame(reason: str) -> str:
    """A well-formed end-of-stream, so a cut client sees a clean finish."""
    payload = json.dumps({"choices": [{"delta": {}, "finish_reason": reason}]})
    return f"data: {payload}\n\ndata: [DONE]\n\n"


# response_model=None: the handler returns either a stream or an error body, and
# FastAPI cannot derive a schema from that union.
@router.post("/v1/chat/completions", response_model=None)
async def chat_completions(
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    tenant_id: str = Header(default="default", alias="X-Tenant-Id"),
) -> StreamingResponse | JSONResponse:
    state: GatewayState = request.app.state.gateway
    if not state.ready:
        raise HTTPException(status_code=503, detail="gateway draining")

    body = await request.json()
    model = body.get("model", "unknown")
    request_id = str(uuid.uuid4())
    # Without a client-supplied key every retry is a distinct request. Deriving
    # one from the request id at least keeps the ledger's uniqueness invariant.
    idem_key = idempotency_key or f"auto-{request_id}"

    claim = await state.idempotency.claim(tenant_id, idem_key, request_id)
    IDEMPOTENCY_OUTCOMES.labels(claim.outcome.value).inc()
    if claim.outcome is ClaimOutcome.IN_FLIGHT:
        raise HTTPException(
            status_code=409,
            detail={"error": "request in flight", "request_id": claim.request_id},
        )
    if claim.outcome is ClaimOutcome.REPLAY:
        return JSONResponse(
            {"error": "already completed", "request_id": claim.request_id}, status_code=409
        )

    prompt_tokens = _count_prompt_tokens(body, model)
    # Admission uses the prompt plus a declared or assumed completion size. It is
    # a guess by construction; the mid-stream draw is what actually enforces.
    declared = body.get("max_tokens") or state.settings.admission_estimate_tokens
    estimate = prompt_tokens + int(declared)
    decision = await state.budget.consume(tenant_id, estimate, int(time.time() * 1000))
    if not decision.granted:
        BUDGET_REJECTIONS.labels(tenant_id, "admission").inc()
        # No work happened, so the key must not be burned — a client that fixes
        # its budget and retries with the same key deserves to be served.
        await state.idempotency.release(tenant_id, idem_key)
        raise HTTPException(
            status_code=429,
            detail={"error": "token budget exhausted", "remaining": decision.remaining},
        )

    state.bus.publish(
        topics.REQUESTS_STARTED,
        request_id,
        RequestStarted(
            request_id=request_id,
            idempotency_key=idem_key,
            tenant_id=tenant_id,
            model=model,
            prompt_tokens=prompt_tokens,
            started_at=time.time(),
        ),
    )

    meter = StreamMeter(
        request_id=request_id,
        idempotency_key=idem_key,
        tenant_id=tenant_id,
        model=model,
        prompt_tokens=prompt_tokens,
        tokenizer=encoder_for(model),
    )
    state.sessions[request_id] = StreamSession(meter, tenant_id, idem_key)
    IN_FLIGHT.set(len(state.sessions))

    return StreamingResponse(
        _proxy(state, request_id, body, estimate),
        media_type="text/event-stream",
        headers={"X-Request-Id": request_id, "Cache-Control": "no-cache"},
    )


async def _proxy(
    state: GatewayState, request_id: str, body: dict, admitted: int
) -> AsyncIterator[str]:
    """Relay the upstream stream, metering and drawing budget as it passes."""
    session = state.sessions[request_id]
    meter = session.meter
    terminal = TerminalState.COMPLETED
    started = time.monotonic()
    first_token_seen = False
    # The admission draw already covers `admitted` tokens; only the overshoot
    # needs drawing again, or a tenant would be charged twice for the estimate.
    drawn = admitted

    try:
        async with state.client.stream(
            "POST",
            "/chat/completions",
            json=body,
            headers={"Authorization": f"Bearer {state.settings.provider_api_key}"},
        ) as response:
            if response.status_code >= 400:
                terminal = TerminalState.PROVIDER_ERROR
                detail = (await response.aread()).decode(errors="replace")[:500]
                yield f"data: {json.dumps({'error': detail})}\n\n"
                return

            async for line in response.aiter_lines():
                meter.consume(line)

                if not first_token_seen and meter.completion_tokens > 0:
                    TTFT.observe(time.monotonic() - started)
                    first_token_seen = True

                total = meter.prompt_tokens + meter.completion_tokens
                overshoot = total - drawn
                if overshoot > 0:
                    decision = await state.budget.consume(
                        session.tenant_id, overshoot, int(time.time() * 1000)
                    )
                    drawn = total
                    if not decision.granted:
                        BUDGET_REJECTIONS.labels(session.tenant_id, "mid_stream").inc()
                        terminal = TerminalState.BUDGET_EXCEEDED
                        yield _terminal_frame("length")
                        return

                yield line + "\n"

    except (httpx.HTTPError, httpx.StreamError):
        terminal = TerminalState.PROVIDER_ERROR
        raise
    except asyncio.CancelledError:
        # Starlette cancels the response task when the client goes away. The
        # partial count is real and must still be billed and attributed.
        terminal = TerminalState.CLIENT_DISCONNECT
        raise
    except GeneratorExit:
        terminal = TerminalState.CLIENT_DISCONNECT
        raise
    finally:
        await state.settle(request_id, terminal)


def create_app(state: GatewayState | None = None) -> FastAPI:
    app = FastAPI(title="inference-ledger", version="0.1.0", lifespan=lifespan)
    if state is not None:
        app.state.gateway = state
    app.include_router(router)
    return app


app = create_app()
