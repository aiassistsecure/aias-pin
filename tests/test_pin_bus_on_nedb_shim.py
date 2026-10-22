"""The operator bus, driven against the REAL shim instead of a fake Redis.

WHAT THIS GUARDS

    [PIN BUS] serve loop failed for op_a32a252d-292:
        'RedisOnNedb' object has no attribute 'pubsub'

`pin_operator_bus` needs four redis primitives — pubsub(), publish(),
pubsub_numsub() and lpop(). `RedisOnNedb` implemented none of them, so on the
NEDB backend the entire cross-worker bus was inert: serve() raised on its first
line, dispatch() and holders() had their exceptions swallowed and returned 0,
and await_reply() returned None. A permanent `holders() == 0` reads to the chat
path as "no worker is holding this operator", so healthy connected operators
were excluded and the gateway answered "No operators available".

WHY THE EXISTING SUITE DID NOT CATCH IT

tests/test_pin_operator_bus.py drives the bus against a `FakeRedis` that
implements all four primitives. The double was strictly MORE capable than
production, so the suite proved a round trip that could not happen on the real
backend — it passed for free. The fix is not a better fake; it is putting the
double UNDERNEATH the code under test. Here the stub is the nedbd store (three
methods, faithful CAS), and everything above it — shim and bus — is the real
shipped code.

Run:  python3 -m unittest tests.test_pin_bus_on_nedb_shim -v
"""

import asyncio
import sys
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.services.nedb_redis_shim import RedisOnNedb  # noqa: E402


class StubStore:
    """The nedbd surface the shim actually uses: get_doc / tx / query.

    CAS is enforced for real — `if_seq == -1` means "must not exist", any other
    value must match the doc's current `_seq`. Getting this wrong would let the
    shim's retry loop pass on a store that never rejects anything, which is the
    same class of free-pass this file exists to close.
    """

    def __init__(self):
        self.docs = {}          # (coll, id) -> doc
        self.seq = 0
        self.conflicts = 0

    def get_doc(self, coll, doc_id):
        doc = self.docs.get((coll, doc_id))
        if doc is None:
            return None
        return dict(doc)

    def tx(self, ops, client="stub"):
        for op in ops:
            addr = (op["coll"], op["id"])
            if op["op"] == "del":
                self.docs.pop(addr, None)
                continue
            assert op["op"] == "put", f"stub implements put/del, got {op['op']}"
            cur = self.docs.get(addr)
            if_seq = op["if_seq"]
            if if_seq == -1:
                if cur is not None:
                    self.conflicts += 1
                    return {"_status": 409, "error": "precondition_failed"}
            elif cur is None or cur["_seq"] != if_seq:
                self.conflicts += 1
                return {"_status": 409, "error": "precondition_failed"}
            self.seq += 1
            doc = dict(op["doc"])
            doc["_seq"] = self.seq
            doc["_id"] = op["id"]
            if op.get("ttl_s") is not None:
                doc["_expires_at"] = time.time() + float(op["ttl_s"])
            self.docs[addr] = doc
        return {"_status": 200}

    def query(self, nql):
        return []


def load_bus(redis_like):
    """Import the bus with its storage replaced by the shim under test."""
    stub = types.ModuleType("api.services.redis_storage")
    stub.storage = types.SimpleNamespace(r=redis_like)
    sys.modules["api.services.redis_storage"] = stub
    sys.modules.pop("api.services.pin_operator_bus", None)
    from api.services import pin_operator_bus  # noqa: PLC0415
    return pin_operator_bus


class ShimHasTheBusPrimitives(unittest.TestCase):
    """The absence itself, asserted. Cheap, and it fails loudly on a regression
    that would otherwise only surface as a swallowed exception in production."""

    def setUp(self):
        self.shim = RedisOnNedb(StubStore())

    def test_every_primitive_the_bus_calls_exists(self):
        for name in ("pubsub", "publish", "pubsub_numsub", "lpop", "rpush"):
            self.assertTrue(callable(getattr(self.shim, name, None)),
                            f"pin_operator_bus calls .{name}() — missing means "
                            f"the whole cross-worker bus is dead")

    def test_lpop_is_fifo_and_rpop_is_lifo(self):
        self.shim.rpush("aiconsult:t:q", "a", "b", "c")
        self.assertEqual(self.shim.lpop("aiconsult:t:q"), "a")
        self.assertEqual(self.shim.rpop("aiconsult:t:q"), "c")
        self.assertEqual(self.shim.lpop("aiconsult:t:q"), "b")

    def test_popping_nothing_returns_none_not_an_error(self):
        """await_reply polls this on every tick before a reply exists, so the
        empty and missing cases are the HOT path, not edge cases."""
        self.assertIsNone(self.shim.lpop("aiconsult:t:never_written"))
        self.shim.rpush("aiconsult:t:drained", "only")
        self.assertEqual(self.shim.lpop("aiconsult:t:drained"), "only")
        self.assertIsNone(self.shim.lpop("aiconsult:t:drained"))
        self.assertIsNone(self.shim.rpop("aiconsult:t:drained"))


class PubSubOverTheStore(unittest.TestCase):

    def setUp(self):
        self.shim = RedisOnNedb(StubStore())

    def test_publish_reaches_every_subscriber_and_counts_them(self):
        a = self.shim.pubsub(ignore_subscribe_messages=True)
        b = self.shim.pubsub(ignore_subscribe_messages=True)
        a.subscribe("chan")
        b.subscribe("chan")

        self.assertEqual(self.shim.pubsub_numsub("chan"), [("chan", 2)])
        self.assertEqual(self.shim.publish("chan", "hello"), 2)

        for sub in (a, b):
            msg = sub.get_message(timeout=0)
            self.assertIsNotNone(msg)
            self.assertEqual(msg["data"], "hello")
            self.assertIsNone(sub.get_message(timeout=0), "exactly once")

    def test_publish_with_no_subscriber_reports_zero(self):
        """dispatch() returns this number, and the caller MUST NOT wait for a
        reply that can never come. A wrong non-zero here converts an instant
        accurate failure into a full inference timeout."""
        self.assertEqual(self.shim.publish("empty", "x"), 0)
        self.assertEqual(self.shim.pubsub_numsub("empty"), [("empty", 0)])

    def test_a_lapsed_lease_stops_counting_as_a_holder(self):
        """A worker killed -9 has no socket to close, so the lease is the only
        thing that can tell the truth. Simulated by expiring it directly."""
        sub = self.shim.pubsub()
        sub.subscribe("chan")
        token = sub._channels["chan"]
        self.assertEqual(self.shim.pubsub_numsub("chan"), [("chan", 1)])

        # The lease lapses. Dropping the key is exactly what the engine TTL
        # does on read, and unlike a 0-second setex it leaves no doubt about
        # whether the expiry actually took effect.
        self.shim.delete(self.shim._ps_live_key(token))

        self.assertEqual(self.shim.pubsub_numsub("chan"), [("chan", 0)])
        self.assertEqual(self.shim.publish("chan", "x"), 0,
                         "a dead subscriber must not be delivered to")

    def test_close_is_idempotent_and_removes_the_subscription(self):
        sub = self.shim.pubsub()
        sub.subscribe("chan")
        sub.close()
        sub.close()   # serve()'s finally can run twice down an exception path
        self.assertEqual(self.shim.pubsub_numsub("chan"), [("chan", 0)])


class BusRoundTripOnTheRealShim(unittest.IsolatedAsyncioTestCase):
    """The production failure, end to end, with nothing faked above the store."""

    def setUp(self):
        self.store = StubStore()
        self.shim = RedisOnNedb(self.store)
        self.bus = load_bus(self.shim)

    async def test_a_worker_without_the_connection_still_gets_an_answer(self):
        served = []

        async def handler(operator_id, kind, request_id, payload):
            served.append((operator_id, kind, request_id, payload.get("model")))
            return {"choices": [{"message": {"content": "ok"}}]}

        serving = asyncio.create_task(self.bus.serve("op_a32a252d-292", handler))
        await asyncio.sleep(0.05)

        self.assertEqual(self.bus.holders("op_a32a252d-292"), 1,
                         "the serving worker must be visible to every other worker")

        delivered = self.bus.dispatch("op_a32a252d-292", "req_1", "chat",
                                      {"model": "GLM-4-32B"})
        self.assertEqual(delivered, 1)

        result = await self.bus.await_reply("req_1", timeout=3.0)
        self.assertIsNotNone(result, "the dispatching worker must receive the answer")
        self.assertEqual(result["choices"][0]["message"]["content"], "ok")
        self.assertEqual(served,
                         [("op_a32a252d-292", "chat", "req_1", "GLM-4-32B")])

        serving.cancel()

    async def test_tts_kind_survives_the_round_trip(self):
        """The `kind` the TTS path needs. `_serve_bus_job` has had a
        `kind == "tts"` branch all along with nothing able to publish to it."""
        seen = {}

        async def handler(operator_id, kind, request_id, payload):
            seen.update(kind=kind, voice=payload.get("voice"))
            return {"audio_b64": "UklGR...", "content_type": "audio/wav"}

        serving = asyncio.create_task(self.bus.serve("op_tts", handler))
        await asyncio.sleep(0.05)

        self.assertEqual(
            self.bus.dispatch("op_tts", "pin_tts_1", "tts",
                              {"model": "chatterbox-turbo", "input": "hi",
                               "voice": "ris"}), 1)
        result = await self.bus.await_reply("pin_tts_1", timeout=3.0)

        self.assertEqual(seen, {"kind": "tts", "voice": "ris"})
        self.assertEqual(result["content_type"], "audio/wav")
        serving.cancel()

    async def test_nobody_holding_is_reported_immediately(self):
        self.assertEqual(self.bus.holders("op_ghost"), 0)
        self.assertEqual(self.bus.dispatch("op_ghost", "req_x", "chat", {}), 0)

    async def test_a_failing_handler_still_produces_a_reply(self):
        """Otherwise the requesting worker waits out its entire timeout for an
        answer that was decided in milliseconds."""
        async def handler(operator_id, kind, request_id, payload):
            raise RuntimeError("operator exploded")

        serving = asyncio.create_task(self.bus.serve("op_boom", handler))
        await asyncio.sleep(0.05)
        self.bus.dispatch("op_boom", "req_boom", "chat", {})

        result = await self.bus.await_reply("req_boom", timeout=3.0)
        self.assertIsNotNone(result)
        self.assertIn("operator exploded", result.get("error", ""))
        serving.cancel()

    async def test_serve_does_not_raise_on_this_backend(self):
        """The regression itself. Before the fix this task died instantly with
        AttributeError and `holders()` was 0 forever after."""
        async def handler(*_a):
            return {}

        serving = asyncio.create_task(self.bus.serve("op_live", handler))
        await asyncio.sleep(0.05)
        self.assertFalse(serving.done(),
                         "serve() must still be running, not dead on a missing "
                         "primitive")
        self.assertEqual(self.bus.holders("op_live"), 1)
        serving.cancel()


if __name__ == "__main__":
    unittest.main(verbosity=2)
