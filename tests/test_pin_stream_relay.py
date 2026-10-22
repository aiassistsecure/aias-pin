"""Streaming chunks must reach a consumer in ANOTHER worker process.

THE BUG

A streaming consumer waits on an asyncio.Queue in one worker's memory. The
operator's uplink POSTs — /stream/chunk per token, /stream/result at the end —
ride the daemon's keep-alive connection to ONE kernel-chosen worker. When that
worker is not the one holding the consumer, the consumer sees ZERO deltas (not
some — the keep-alive pins EVERY uplink to the same wrong process) and dies on
the inter-chunk watchdog. Measured on this stack, 2026-08-29:

    pin-clientd:  Completion generated (19k tokens) ... queued for send
    Yente:        NETWORK_ERROR: operator produced nothing for 90s

Both true, in different processes — the same two-sided symptom the operator
bus was built for, pointing the other way. A manual curl through the same
gateway streamed perfectly whenever the dice landed the POSTs on the right
worker, which is exactly what made this look intermittent.

THE SHAPE OF THE FIX

    uplink POST  →  PUBLISH aiconsult:pin:bus:stream:{request_id}
    consumer     →  StreamRelay subscribed BEFORE dispatch, pumping into
                    the same local queue the consumer already reads

The load-bearing details tested here:

  * subscribe happens synchronously at construction — a chunk published the
    instant the relay exists is NOT lost (pub/sub has no memory, so
    subscribe-after-dispatch would silently eat the first tokens of every
    fast model);
  * the final rides the SAME channel as the chunks, so it cannot overtake
    tokens still in flight;
  * zero subscribers is INFORMATION — the uplink handlers fall back to local
    delivery, which is byte-for-byte the pre-bus behavior for single-process
    deploys, Redis outages, and consumers that already gave up.
"""

import asyncio
import importlib
import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_pin_operator_bus import FakeRedis  # noqa: E402


def load_bus(fake):
    """Import the bus bound to THIS fake — actually reimported.

    The harness in test_pin_operator_bus pops sys.modules and then uses
    `from api.services import pin_operator_bus`, which the import machinery
    satisfies from the parent package's ATTRIBUTE without re-executing the
    module — so every fake after the first was silently ignored, and the
    suite passed only because those tests reach Redis exclusively through
    the returned module. These tests hand the module a BROKEN fake on
    purpose, so the rebind has to be real: importlib.import_module consults
    sys.modules and re-executes when the entry is gone.
    """
    stub = types.ModuleType("api.services.redis_storage")
    stub.storage = types.SimpleNamespace(r=fake)
    saved_storage = sys.modules.get("api.services.redis_storage")
    saved_bus = sys.modules.get("api.services.pin_operator_bus")
    sys.modules["api.services.redis_storage"] = stub
    sys.modules.pop("api.services.pin_operator_bus", None)
    try:
        module = importlib.import_module("api.services.pin_operator_bus")
    finally:
        # Restore IMMEDIATELY: the freshly-executed module already holds its
        # `storage` binding, and leaving the stub in sys.modules poisons every
        # later test file that imports the real storage in this process.
        if saved_storage is not None:
            sys.modules["api.services.redis_storage"] = saved_storage
        else:
            sys.modules.pop("api.services.redis_storage", None)
        if saved_bus is not None:
            sys.modules["api.services.pin_operator_bus"] = saved_bus
            setattr(sys.modules["api.services"], "pin_operator_bus", saved_bus)
    return module


class StreamRelayTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.fake = FakeRedis()
        self.bus = load_bus(self.fake)

    async def test_chunks_published_in_worker_b_arrive_in_worker_a_in_order(self):
        """The whole point: the queue's worker and the POST's worker differ."""
        q: asyncio.Queue = asyncio.Queue()
        relay = self.bus.open_stream_relay("req_stream", q)
        self.assertIsNotNone(relay)

        # "Worker B": three uplink POSTs, then the final.
        for i, delta in enumerate(["Hel", "lo", "!"]):
            self.assertEqual(
                self.bus.publish_stream_event(
                    "req_stream", {"type": "chunk", "index": i, "delta": delta,
                                   "kind": "content"}),
                1, "the relay must be visible to the publishing worker")
        self.bus.publish_stream_event(
            "req_stream", {"type": "final", "result": {"ok": True}})

        events = []
        for _ in range(4):
            events.append(await asyncio.wait_for(q.get(), timeout=2.0))
        relay.close()

        self.assertEqual([e["type"] for e in events],
                         ["chunk", "chunk", "chunk", "final"],
                         "the final must not overtake chunks still in flight")
        self.assertEqual("".join(e["delta"] for e in events[:3]), "Hello!")
        self.assertEqual(events[3]["result"], {"ok": True})

    async def test_the_subscription_exists_before_the_constructor_returns(self):
        """Publish IMMEDIATELY after construction — before the pump task has
        ever run — and the event must still arrive. This is the guarantee that
        lets the caller dispatch right after opening the relay: pub/sub has no
        memory, so anything weaker loses the first tokens of a fast model."""
        q: asyncio.Queue = asyncio.Queue()
        relay = self.bus.open_stream_relay("req_first_token", q)
        delivered = self.bus.publish_stream_event(
            "req_first_token", {"type": "chunk", "index": 0, "delta": "F",
                                "kind": "content"})
        self.assertEqual(delivered, 1,
                         "subscribe must complete synchronously at construction")
        event = await asyncio.wait_for(q.get(), timeout=2.0)
        relay.close()
        self.assertEqual(event["delta"], "F")

    async def test_zero_subscribers_is_reported_so_the_caller_can_fall_back(self):
        self.assertEqual(
            self.bus.publish_stream_event(
                "req_nobody", {"type": "chunk", "index": 0, "delta": "x",
                               "kind": "content"}),
            0, "no relay anywhere must read as 0, not as success")

    async def test_close_unsubscribes_so_late_chunks_read_as_undeliverable(self):
        """A consumer that gave up must stop counting as a subscriber —
        otherwise the uplink keeps 'delivering' into a queue nobody reads and
        never takes the local-fallback path for a waiter that IS local."""
        q: asyncio.Queue = asyncio.Queue()
        relay = self.bus.open_stream_relay("req_gone", q)
        relay.close()
        self.assertEqual(
            self.bus.publish_stream_event(
                "req_gone", {"type": "chunk", "index": 0, "delta": "late",
                             "kind": "content"}),
            0)

    async def test_a_broken_redis_yields_none_not_an_exception(self):
        """None means 'no cross-worker delivery' and the caller degrades to
        the pre-bus code path. Raising here would turn a Redis outage into a
        hard failure for same-worker streams that were about to work."""
        class BrokenRedis:
            def pubsub(self, **kw):
                raise ConnectionError("redis is down")

        bus = load_bus(BrokenRedis())
        self.assertIsNone(bus.open_stream_relay("req_x", asyncio.Queue()))

    async def test_garbage_on_the_channel_does_not_kill_the_relay(self):
        q: asyncio.Queue = asyncio.Queue()
        relay = self.bus.open_stream_relay("req_noise", q)
        self.fake.publish(self.bus.stream_channel("req_noise"), "not json {")
        self.bus.publish_stream_event(
            "req_noise", {"type": "chunk", "index": 0, "delta": "ok",
                          "kind": "content"})
        event = await asyncio.wait_for(q.get(), timeout=2.0)
        relay.close()
        self.assertEqual(event["delta"], "ok",
                         "a malformed frame must be skipped, not fatal")


class UplinkHandlersRideTheBus(unittest.TestCase):
    """The route handlers are read as source, the same technique
    test_pin_tool_calling.py uses and for the same reason: both live inside a
    FastAPI app whose import drags in the full storage stack, so driving them
    end to end here would test the harness. What matters is a property of the
    code: publish FIRST, local delivery only as the zero-subscriber fallback —
    and the consumer subscribed BEFORE anything was dispatched.
    """

    @classmethod
    def setUpClass(cls):
        cls.source = (Path(__file__).resolve().parents[1]
                      / "api" / "routes" / "pin.py").read_text(encoding="utf-8")

    def test_the_chunk_uplink_publishes_and_only_falls_back_on_zero(self):
        self.assertIn("bus.publish_stream_event(req_id, event) == 0", self.source,
                      "/stream/chunk must try the bus first and fall back local")

    def test_the_result_uplink_publishes_on_the_same_channel(self):
        self.assertIn(
            'bus.publish_stream_event(request_id, {"type": "final", "result": result}) == 0',
            self.source,
            "/stream/result must ride the chunk channel so order is preserved, "
            "with the pre-bus local resolve as the zero-subscriber fallback")

    def test_the_stream_consumer_subscribes_before_it_dispatches(self):
        """Queue registration + relay must precede every dispatch attempt in
        stream_inference_request. Dispatch-then-subscribe loses the first
        tokens; this is the ordering bug that cannot be caught at runtime
        because it only eats the opening chunks of fast generations."""
        body = self.source.split("async def stream_inference_request")[1]
        relay_at = body.index("bus.open_stream_relay")
        queue_at = body.index("self.stream_queues[request_id] = q")
        ws_dispatch_at = body.index("await ws.send_json(message)")
        sse_dispatch_at = body.index("sse_transport.queue(operator_id, message)")
        bus_dispatch_at = body.index('"chat_stream"')
        self.assertLess(queue_at, relay_at)
        for dispatch_at in (ws_dispatch_at, sse_dispatch_at, bus_dispatch_at):
            self.assertLess(relay_at, dispatch_at,
                            "the relay must exist before any transport is tried")

    def test_streaming_can_be_dispatched_across_workers_but_only_with_a_relay(self):
        """bus.dispatch for a stream whose tokens cannot come back would burn
        the operator's whole generation to produce a guaranteed timeout."""
        body = self.source.split("async def stream_inference_request")[1]
        guard = body.index("relay is not None and bus.dispatch")
        self.assertGreater(guard, 0)

    def test_the_bus_job_handler_forwards_chat_stream_without_waiting(self):
        body = self.source.split("async def _serve_bus_job")[1]
        head = body.split("async def ")[0]
        self.assertIn('if kind == "chat_stream":', head)
        arm = (head.split('if kind == "chat_stream":')[1]
               .split("return await self.send_inference_request")[0])
        self.assertIn('return {"queued": True}', arm)
        self.assertNotIn("await self.send_inference_request", arm,
                         "the forwarding arm must not wait on a future — the "
                         "reply path for a stream is the stream channel itself")
        self.assertNotIn("await_reply", arm)

    def test_the_nonstreaming_future_also_listens_on_the_channel(self):
        """send_inference_request has the same uplink gap: its future lives in
        this worker, the result POST lands wherever the keep-alive points."""
        body = self.source.split("async def send_inference_request")[1]
        # Boundary is the NEXT manager method, not "async def": the relay's
        # resolver is itself a nested async def inside this function.
        head = body.split("async def send_tts_request")[0]
        self.assertIn("bus.open_stream_relay(request_id, relay_q)", head)
        self.assertIn("future.set_result(event.get(\"result\"))", head)
        relay_at = head.index("bus.open_stream_relay")
        for dispatch in ("await ws.send_json(message)",
                         "sse_transport.queue(operator_id, message)"):
            self.assertLess(relay_at, head.index(dispatch),
                            "subscribe-before-dispatch holds here too")


if __name__ == "__main__":
    unittest.main()
