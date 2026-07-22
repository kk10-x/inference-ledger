# Draining in-flight streams on shutdown

The hardest correctness problem in this system is not the Kafka join — it is
losing a pod while it is halfway through streaming a response to a client.

## What makes it hard

A normal HTTP service drains by refusing new connections and letting open
requests finish. A streaming gateway cannot do only that:

- Streams are **long and open-ended**. A 90-second generation does not fit in a
  30-second grace period, so "wait for everything" is not a strategy.
- The token count is **incremental**. At `SIGTERM` the gateway holds a partial
  count that exists nowhere else — the provider has not reported yet, and it may
  never report for a connection we are about to sever.
- The Kafka producer **buffers**. An event enqueued but not flushed dies with the
  process, and a request whose Ledger A entry never left the pod is a request
  that will be force-settled later as `UNSETTLED_TIMEOUT` — real money, silently
  wrong.

## Sequence

1. `SIGTERM` arrives. Flip readiness to false immediately. Kubernetes removes the
   pod from Service endpoints; new requests stop arriving within one probe
   interval. Keep liveness true — being killed for "failing" liveness during a
   deliberate drain is a self-inflicted outage.
2. Stop accepting new connections at the ASGI layer, but do **not** cancel open
   ones.
3. Wait up to `grace - flush_reserve` for streams to end on their own. Most
   finish; this is the cheap win.
4. For streams still open at the deadline: send a well-formed terminal SSE frame
   (`data: [DONE]` after an error delta) rather than dropping the socket, so
   clients see a clean end and can retry with the same idempotency key.
5. Finalize every remaining `StreamMeter` with `TerminalState.GATEWAY_SHUTDOWN`.
   The partial count is emitted — a floor is strictly better than a gap.
6. **Flush the producer and block on it.** This is the step that must not be
   skipped, which is why `flush_reserve` is carved out of the grace period up
   front rather than hoped for at the end.
7. Exit 0.

## Kubernetes and Compose settings that must agree

| Setting | Value | Why |
|---|---|---|
| `terminationGracePeriodSeconds` | 45 | Must exceed step 3 + `flush_reserve` |
| `preStop` sleep | 5s | Covers endpoint-propagation lag before the drain starts |
| readiness `periodSeconds` | 2 | Bounds how long traffic keeps arriving post-`SIGTERM` |
| compose `stop_grace_period` | 45s | Keeps local runs faithful to the cluster |

A grace period shorter than the drain turns every deploy into a burst of
`GATEWAY_CRASH_PARTIAL` drift. That is the failure this table prevents, and the
`gateway-sigterm-drain` chaos scenario is what proves it stays prevented.

## What we deliberately do not do

We do not wait indefinitely for streams. A pod that refuses to die blocks the
rollout, and the ledger already has a correct answer for a truncated stream. The
design choice throughout is that **an attributed partial beats a delayed total**.
