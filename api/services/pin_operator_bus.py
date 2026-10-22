"""Deliver a job to the worker that actually holds the operator's socket.

THE BUG THIS EXISTS FOR

    No operators available for model: muse-local:latest.
    Connected operators in this process: none.

...logged in the same minute that the operator's own daemon reported
`Authenticated!`, `VERIFIED`, `21.3 tokens/sec`. Both statements were true.

An operator's connection — WebSocket today, SSE next — lives in the memory of
exactly ONE worker process. `pin_ws_manager.operator_map` is a plain dict. The
gateway runs several workers behind one socket, so an inference request lands on
a worker chosen by the kernel, and with N workers roughly (N-1)/N of them are
holding no operator at all. Those workers cannot serve, no matter how healthy the
operator is or how correct the Redis registry is:

    operator = storage.pin_select_operator(...)        # shared Redis: finds it
    if not ws_manager.is_operator_online(operator.id): # LOCAL memory: does not
        raise ...

Every reconnect re-rolls which worker holds the socket, which is why a system
that worked at 22:02 was dead by 22:30 with nothing changed. `Connection reset by
peer` in the daemon log is the dice being thrown again.

WHY A BUS AND NOT STICKY ROUTING

Sticky routing (hash the operator to a worker at the load balancer) needs the
balancer to know an application-level identity it cannot see on an inference
request, and breaks the moment worker count changes. A bus needs neither: any
worker can hand the job to whichever worker is holding the connection, and that
is true of WebSocket now and of the SSE downlink that replaces it.

SWAPPING THE TRANSPORT DOES NOT FIX THIS BY ITSELF. An SSE downlink held in
worker 3 is exactly as invisible to worker 1 as a WebSocket is. This module is
the piece that makes either one work, so it is deliberately transport-agnostic:
it moves an envelope to the process holding a named connection and brings a
reply back.

THE SHAPE

    dispatch   PUBLISH  pin:bus:op:{operator_id}   {request_id, kind, payload}
    reply      RPUSH    pin:bus:reply:{request_id} {...}     + EXPIRE

PUBLISH returns the number of subscribers, which is the honest answer to "is any
worker holding this operator" — better than an online-set cache, because it is
measured at the moment of asking rather than remembered from a previous one.

Replies go through a LIST rather than a second pub/sub channel on purpose: a
publish with no listener is lost silently, and a reply that arrives a
millisecond before the waiter subscribes would vanish. A list holds the value
until it is collected, and the TTL cleans up after a waiter that gave up.

WHY POLLING FOR THE REPLY

`storage.r` is the synchronous client this codebase uses everywhere. A blocking
BLPOP on it would stall the whole event loop for the duration of an inference —
one wedged job would freeze the worker. Polling at a short interval costs a
handful of cheap Redis calls per second and cannot stall anything.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

from api.services.redis_storage import storage

DISPATCH_PREFIX = "aiconsult:pin:bus:op:"
REPLY_PREFIX = "aiconsult:pin:bus:reply:"
STREAM_PREFIX = "aiconsult:pin:bus:stream:"

# A reply nobody collects should not live in Redis forever. Comfortably longer
# than any inference we would still be waiting on.
REPLY_TTL_SECONDS = 900

POLL_INTERVAL_SECONDS = 0.1


def dispatch_channel(operator_id: str) -> str:
    return f"{DISPATCH_PREFIX}{operator_id}"


def reply_key(request_id: str) -> str:
    return f"{REPLY_PREFIX}{request_id}"


def holders(operator_id: str) -> int:
    """How many worker processes are subscribed for this operator, right now.

    Uses PUBSUB NUMSUB, so it reports the live subscription count rather than a
    cached claim about who is online. Zero means genuinely nobody is holding a
    connection for this operator in any process.
    """
    try:
        result = storage.r.pubsub_numsub(dispatch_channel(operator_id))
        # redis-py returns [(channel, count)]; tolerate either shape.
        if isinstance(result, list) and result:
            return int(result[0][1])
        if isinstance(result, dict):
            return int(next(iter(result.values()), 0))
    except Exception as exc:  # noqa: BLE001 -- liveness must never raise
        print(f"[PIN BUS] numsub failed for {operator_id}: {exc}")
    return 0


def dispatch(operator_id: str, request_id: str, kind: str, payload: dict) -> int:
    """Hand a job to whichever worker holds this operator. Returns the number of
    workers that received it — 0 means it went nowhere and the caller must NOT
    wait for a reply that can never come."""
    envelope = json.dumps({
        "request_id": request_id,
        "kind": kind,
        "payload": payload,
        "at": time.time(),
    })
    try:
        return int(storage.r.publish(dispatch_channel(operator_id), envelope))
    except Exception as exc:  # noqa: BLE001
        print(f"[PIN BUS] dispatch failed for {operator_id}/{request_id}: {exc}")
        return 0


def post_reply(request_id: str, result: Any) -> None:
    """Called by the worker that owns the connection, once it has an answer."""
    key = reply_key(request_id)
    try:
        storage.r.rpush(key, json.dumps({"result": result}))
        storage.r.expire(key, REPLY_TTL_SECONDS)
    except Exception as exc:  # noqa: BLE001
        print(f"[PIN BUS] reply failed for {request_id}: {exc}")


async def await_reply(request_id: str, timeout: float) -> Optional[Any]:
    """Wait for the owning worker's answer. None on timeout.

    None is also what `send_inference_request` returns on a local timeout, so
    the caller's existing "no result" handling covers this path unchanged.
    """
    key = reply_key(request_id)
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            raw = storage.r.lpop(key)
            if raw is not None:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                try:
                    return json.loads(raw).get("result")
                except (ValueError, AttributeError):
                    return None
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        try:
            storage.r.delete(key)
        except Exception:  # noqa: BLE001
            pass
    return None


async def serve(operator_id: str, handler) -> None:
    """Run in the worker that HOLDS the connection: consume jobs for this
    operator and post each result back.

    `handler(operator_id, kind, request_id, payload)` is awaited and must return
    the value to send back — for chat that is exactly what
    `send_inference_request` returns.

    THE OPERATOR ID IS PASSED, NOT INFERRED. The first version left the handler
    to work out who the job was for by scanning the WebSocket map, which was
    wrong the moment a second transport existed: an SSE operator is in no such
    map, so the scan found nothing and every dispatched job returned None —
    indistinguishable from a timeout. The subscription already knows the answer;
    withholding it was the bug.

    Subscribing is what makes `holders()` true, so this coroutine's lifetime IS
    the operator's routability: it must start when the connection is accepted
    and stop when it closes. Cancellation is the normal exit and is silent.
    """
    pubsub = None
    try:
        pubsub = storage.r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(dispatch_channel(operator_id))
        print(f"[PIN BUS] serving jobs for {operator_id}")

        while True:
            message = pubsub.get_message(timeout=0)
            if not message:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            data = message.get("data")
            if isinstance(data, bytes):
                data = data.decode()
            try:
                envelope = json.loads(data)
            except (TypeError, ValueError):
                continue

            request_id = envelope.get("request_id")
            if not request_id:
                continue

            # Each job runs on its own task: a slow inference must not stop this
            # loop from accepting the next one, and the operator's own capacity
            # limit is enforced downstream where it belongs.
            asyncio.create_task(_run_one(handler, operator_id, envelope, request_id))
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"[PIN BUS] serve loop failed for {operator_id}: {exc}")
    finally:
        try:
            if pubsub is not None:
                pubsub.close()
        except Exception:  # noqa: BLE001
            pass


async def _run_one(handler, operator_id: str, envelope: dict, request_id: str) -> None:
    """One dispatched job. A failure here must still produce a reply, or the
    requesting worker waits out its whole timeout for an answer that was decided
    seconds ago."""
    try:
        result = await handler(
            operator_id, envelope.get("kind", "chat"), request_id,
            envelope.get("payload") or {})
        post_reply(request_id, result)
    except Exception as exc:  # noqa: BLE001
        print(f"[PIN BUS] job {request_id} failed: {exc}")
        post_reply(request_id, {"error": str(exc)})


# --------------------------------------------------------------------------
# The stream leg: chunks by request_id, not jobs by operator_id.
#
# THE BUG THIS EXISTS FOR
#
# A streaming consumer waits on an asyncio.Queue in ONE worker's memory. The
# operator's uplink POSTs — /stream/chunk per token, /stream/result at the
# end — are ordinary HTTP requests, so the kernel hands them to ANY worker.
# The daemon's keep-alive connection makes it worse, not better: every uplink
# rides one TCP connection to one consistently WRONG worker, so the consumer
# sees ZERO deltas — not a fraction of them — and then dies on the 90s
# inter-chunk watchdog. Measured on this stack: the operator's own log said
# "queued for send" for a 19k-token generation while the caller was told
# "operator produced nothing for 90s". Both were true, in different processes.
#
# Same shape as the dispatch bug at the top of this file, opposite direction.
# Dispatch moves a job TO the worker holding the operator; this moves the
# tokens BACK to the worker holding the consumer.
#
# WHY PUB/SUB HERE WHEN THE REPLY LEG USES A LIST
#
# A reply is one value that must not be lost, so it sits in a list until
# collected. A stream is thousands of values whose only worth is immediacy —
# nobody wants token 1 replayed after the stream died, and a list would need
# its own GC discipline per request. PUBLISH's failure mode (no subscriber →
# dropped) is exactly the existing contract: push_chunk already drops chunks
# for unknown request_ids on purpose.
#
# THE SUBSCRIBE-BEFORE-DISPATCH RULE
#
# Pub/sub has no memory. The consumer MUST be subscribed before the operator
# can produce the first token, which means open_stream_relay() is called
# BEFORE the INFERENCE_REQUEST is dispatched, never after. subscribe() on the
# sync client completes the subscription before returning, so once the
# constructor returns the channel is live.
# --------------------------------------------------------------------------


def stream_channel(request_id: str) -> str:
    return f"{STREAM_PREFIX}{request_id}"


def publish_stream_event(request_id: str, event: dict) -> int:
    """Send one stream event toward whoever is waiting on this request.

    Returns the number of subscribers reached. ZERO IS INFORMATION: it means
    no worker anywhere holds a relay for this request — the caller falls back
    to its local delivery so a single-process deploy (or a Redis outage)
    behaves exactly as the code did before this module existed.
    """
    try:
        return int(storage.r.publish(stream_channel(request_id),
                                     json.dumps(event)))
    except Exception as exc:  # noqa: BLE001 -- delivery may degrade, not raise
        print(f"[PIN BUS] stream publish failed for {request_id}: {exc}")
        return 0


class StreamRelay:
    """Feed one consumer's local queue from the stream channel.

    Owned by the worker holding the consumer's SSE response. Constructing it
    subscribes SYNCHRONOUSLY (the no-lost-first-token guarantee); the pump
    task then drains the channel into the queue the consumer is already
    reading. close() is idempotent and must run in the same finally that
    discards the queue.
    """

    def __init__(self, request_id: str, queue: "asyncio.Queue"):
        self.request_id = request_id
        self.queue = queue
        self.pubsub = storage.r.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(stream_channel(request_id))
        self._task = asyncio.get_event_loop().create_task(self._pump())

    async def _pump(self) -> None:
        try:
            while True:
                # Drain everything available, then yield: a 20 tok/s model
                # outruns a poll-per-token loop, and chunks must arrive in
                # publish order with no artificial one-per-tick throttle.
                message = self.pubsub.get_message(timeout=0)
                if message is None:
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode()
                try:
                    event = json.loads(data)
                except (TypeError, ValueError):
                    continue
                if isinstance(event, dict) and event.get("type"):
                    self.queue.put_nowait(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            # The relay dying must look like a silent operator, not a hang
            # with no diagnosis: the consumer's idle timeout will fire and
            # say how long it waited.
            print(f"[PIN BUS] stream relay failed for {self.request_id}: {exc}")

    def close(self) -> None:
        self._task.cancel()
        try:
            self.pubsub.close()
        except Exception:  # noqa: BLE001
            pass


def open_stream_relay(request_id: str, queue: "asyncio.Queue") -> Optional[StreamRelay]:
    """A relay, or None when Redis is unavailable.

    None is a real answer the caller must handle: without a relay only
    same-worker delivery works, which is exactly the pre-bus behavior — so
    the degraded mode is the old code, not a new failure.
    """
    try:
        return StreamRelay(request_id, queue)
    except Exception as exc:  # noqa: BLE001
        print(f"[PIN BUS] could not open stream relay for {request_id}: {exc}")
        return None
