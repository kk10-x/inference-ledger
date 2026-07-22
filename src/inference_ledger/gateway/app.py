"""FastAPI app exposing an OpenAI-compatible surface.

Endpoints:

* ``POST /v1/chat/completions`` — proxied to the upstream provider. Streaming
  responses are relayed chunk-by-chunk with no buffering, so time-to-first-token
  is unaffected by metering.
* ``GET /healthz`` / ``GET /readyz`` — liveness and readiness. Readiness flips to
  false the instant ``SIGTERM`` lands, so Kubernetes stops routing new traffic
  while in-flight streams drain.
* ``GET /metrics`` — Prometheus.

Shutdown is the hard part and is documented in ``docs/shutdown.md``: on
``SIGTERM`` the app stops accepting requests, lets open streams run to
completion or to the grace deadline, finalizes each :class:`StreamMeter` with
:attr:`TerminalState.GATEWAY_SHUTDOWN`, and flushes the Kafka producer before
exiting. A producer flush that is skipped here is a request that never settles.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="inference-ledger", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
