"""The operator bus: can a worker that does NOT hold the socket still be served?

WHAT THIS GUARDS

    No operators available for model: muse-local:latest.
    Connected operators in this process: none.

An operator's connection lives in one worker's memory. With several workers
behind one port, most inference requests land somewhere that holds nothing, and
every reconnect re-rolls which worker is the lucky one. These tests drive the
bus end to end against a fake Redis so the round trip is asserted rather than
assumed — including the two failure modes that would be worse than the bug:
dispatching into the void and waiting out the full timeout, and a reply that
arrives before its waiter is listening.
"""

import asyncio
import json
import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class FakeRedis:
    """Enough Redis for the bus: pub/sub with real subscriber counts, plus lists."""

    def __init__(self):
        self.channels = {}      # channel -> [FakePubSub]
        self.lists = {}         # key -> [str]
        self.expires = {}

    # --- pub/sub ---
    def pubsub(self, ignore_subscribe_messages=False):
        return FakePubSub(self)

    def publish(self, channel, message):
        subs = self.channels.get(channel, [])
        for sub in subs:
            sub.inbox.append({"type": "message", "channel": channel, "data": message})
        return len(subs)

    def pubsub_numsub(self, channel):
        return [(channel, len(self.channels.get(channel, [])))]

    # --- lists ---
    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    def lpop(self, key):
        queue = self.lists.get(key) or []
        return queue.pop(0) if queue else None

    def expire(self, key, ttl):
        self.expires[key] = ttl
        return True

    def delete(self, key):
        self.lists.pop(key, None)
        return True


class FakePubSub:
    def __init__(self, redis):
        self.redis = redis
        self.inbox = []
        self.subscribed = []

    def subscribe(self, channel):
        self.redis.channels.setdefault(channel, []).append(self)
        self.subscribed.append(channel)

    def get_message(self, timeout=0):
        return self.inbox.pop(0) if self.inbox else None

    def close(self):
        for channel in self.subscribed:
            holders = self.redis.channels.get(channel, [])
            if self in holders:
                holders.remove(self)


def load_bus(fake):
    """Import the bus with its storage replaced — it must not need a real Redis."""
    stub = types.ModuleType("api.services.redis_storage")
    stub.storage = types.SimpleNamespace(r=fake)
    sys.modules["api.services.redis_storage"] = stub
    sys.modules.pop("api.services.pin_operator_bus", None)
    from api.services import pin_operator_bus  # noqa: PLC0415
    return pin_operator_bus


class OperatorBusTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.fake = FakeRedis()
        self.bus = load_bus(self.fake)

    async def test_a_worker_without_the_socket_still_gets_an_answer(self):
        """The whole point. Worker A dispatches, worker B serves, A gets the result.

        The handler takes `operator_id` FIRST: the subscription is per operator,
        so the bus always knows who a job is for. Making the handler work it out
        by scanning a transport-specific map is what broke SSE delivery.
        """
        served = []

        async def handler(operator_id, kind, request_id, payload):
            served.append((kind, request_id, payload["model"]))
            return {"choices": [{"message": {"content": "ok"}}]}

        serving = asyncio.create_task(self.bus.serve("op_1", handler))
        await asyncio.sleep(0.05)   # let the subscription land

        self.assertEqual(self.bus.holders("op_1"), 1,
                         "a serving worker must be visible to every other worker")

        delivered = self.bus.dispatch("op_1", "req_1", "chat", {"model": "muse-local:latest"})
        self.assertEqual(delivered, 1)

        result = await self.bus.await_reply("req_1", timeout=3.0)
        self.assertIsNotNone(result, "the dispatching worker must receive the answer")
        self.assertEqual(result["choices"][0]["message"]["content"], "ok")
        self.assertEqual(served, [("chat", "req_1", "muse-local:latest")])

        serving.cancel()

    async def test_nobody_holding_is_reported_immediately(self):
        """`holders() == 0` is the honest 'unreachable', and dispatch says so.

        This matters more than it looks: a caller that waits out a 65s timeout
        for a job that was never delivered turns an instant, accurate failure
        into a minute of silence — which is exactly how the original bug
        presented.
        """
        self.assertEqual(self.bus.holders("op_ghost"), 0)
        self.assertEqual(self.bus.dispatch("op_ghost", "req_x", "chat", {}), 0)

    async def test_a_reply_that_arrives_early_is_not_lost(self):
        """Replies go through a LIST, not a second pub/sub channel.

        A publish with no listener vanishes. If the serving worker answered a
        millisecond before the waiter started listening, a pub/sub reply would
        be gone and the caller would wait out the timeout for an answer that had
        already been produced.
        """
        self.bus.post_reply("req_early", {"done": True})
        result = await self.bus.await_reply("req_early", timeout=1.0)
        self.assertEqual(result, {"done": True})

    async def test_a_handler_that_raises_still_answers(self):
        """A failure must come back as a failure, not as a timeout."""
        async def boom(operator_id, kind, request_id, payload):
            raise RuntimeError("operator exploded")

        serving = asyncio.create_task(self.bus.serve("op_2", boom))
        await asyncio.sleep(0.05)

        self.bus.dispatch("op_2", "req_boom", "chat", {})
        result = await self.bus.await_reply("req_boom", timeout=3.0)

        self.assertIsInstance(result, dict)
        self.assertIn("operator exploded", result.get("error", ""))
        serving.cancel()

    async def test_timeout_returns_none_and_cleans_up(self):
        """None is what send_inference_request returns on a local timeout too, so
        every caller's existing no-result handling covers this path unchanged."""
        result = await self.bus.await_reply("req_never", timeout=0.3)
        self.assertIsNone(result)
        self.assertNotIn(self.bus.reply_key("req_never"), self.fake.lists)

    async def test_a_closed_connection_stops_holding(self):
        """A subscription that outlives its socket keeps attracting jobs into a
        worker that can no longer deliver them."""
        async def handler(operator_id, kind, request_id, payload):
            return {"ok": True}

        serving = asyncio.create_task(self.bus.serve("op_3", handler))
        await asyncio.sleep(0.05)
        self.assertEqual(self.bus.holders("op_3"), 1)

        serving.cancel()
        try:
            await serving
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.05)

        self.assertEqual(self.bus.holders("op_3"), 0,
                         "cancellation must release the subscription")

    async def test_two_jobs_do_not_serialise_behind_each_other(self):
        """A slow inference must not stop the loop accepting the next job."""
        order = []

        async def handler(operator_id, kind, request_id, payload):
            delay = payload["delay"]
            await asyncio.sleep(delay)
            order.append(request_id)
            return {"id": request_id}

        serving = asyncio.create_task(self.bus.serve("op_4", handler))
        await asyncio.sleep(0.05)

        self.bus.dispatch("op_4", "slow", "chat", {"delay": 0.3})
        await asyncio.sleep(0.02)
        self.bus.dispatch("op_4", "fast", "chat", {"delay": 0.01})

        fast = await self.bus.await_reply("fast", timeout=3.0)
        self.assertEqual(fast, {"id": "fast"})
        self.assertEqual(order, ["fast"], "the fast job must not wait for the slow one")

        slow = await self.bus.await_reply("slow", timeout=3.0)
        self.assertEqual(slow, {"id": "slow"})
        serving.cancel()


if __name__ == "__main__":
    unittest.main()
