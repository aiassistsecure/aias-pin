"""A dispatched job must reach the operator over EITHER transport.

THE BUG

`bus.serve` is subscribed per operator, so it always knew which operator a job
was for — and did not pass it. `_serve_bus_job` therefore worked the answer out
by scanning `operator_map`, which is a WebSocket-only structure. With the SSE
downlink live there is no socket and no map entry, so the scan returned None and
every dispatched job came back empty. Permanent failure wearing a timeout's
clothes:

    Yente:   No connected operators available for model: muse-local:latest
    Thunder: SSE downlink open ... Registered 1 node(s) ... [NODE] UPDATED

Both true, again.

These tests assert the two halves of the fix: the bus TELLS the handler who the
job is for, and the handler can deliver to an operator that has no socket.
"""

import asyncio
import json
import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_pin_operator_bus import FakeRedis, load_bus  # noqa: E402


class BusPassesTheOperatorTests(unittest.IsolatedAsyncioTestCase):
    """The bus's contract with its handler."""

    def setUp(self):
        self.fake = FakeRedis()
        self.bus = load_bus(self.fake)

    async def test_the_handler_is_told_which_operator_the_job_is_for(self):
        seen = []

        async def handler(operator_id, kind, request_id, payload):
            seen.append(operator_id)
            return {"ok": True}

        serving = asyncio.create_task(self.bus.serve("op_sse_only", handler))
        await asyncio.sleep(0.05)

        self.bus.dispatch("op_sse_only", "req_1", "chat", {})
        result = await self.bus.await_reply("req_1", timeout=3.0)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(seen, ["op_sse_only"],
                         "the handler must not have to guess who the job is for")
        serving.cancel()

    async def test_two_operators_on_one_worker_do_not_get_confused(self):
        """The scan-the-map version returned whichever operator it found FIRST.

        With two operators held by one process that is a coin flip, and the job
        goes to the wrong node — which is worse than failing, because it
        succeeds against a model the caller did not select.
        """
        routed = {}

        async def handler(operator_id, kind, request_id, payload):
            routed[request_id] = operator_id
            return {"served_by": operator_id}

        a = asyncio.create_task(self.bus.serve("op_a", handler))
        b = asyncio.create_task(self.bus.serve("op_b", handler))
        await asyncio.sleep(0.05)

        self.bus.dispatch("op_a", "for_a", "chat", {})
        self.bus.dispatch("op_b", "for_b", "chat", {})

        self.assertEqual(await self.bus.await_reply("for_a", 3.0), {"served_by": "op_a"})
        self.assertEqual(await self.bus.await_reply("for_b", 3.0), {"served_by": "op_b"})
        self.assertEqual(routed, {"for_a": "op_a", "for_b": "op_b"})

        a.cancel(); b.cancel()


class DeliveryOverEitherTransportTests(unittest.IsolatedAsyncioTestCase):
    """`send_inference_request` against an operator with no WebSocket."""

    def setUp(self):
        sys.modules.pop("api.services.pin_sse_transport", None)
        from api.services.pin_sse_transport import PinSseTransport  # noqa: PLC0415
        self.transport = PinSseTransport()

    async def test_an_sse_operator_receives_the_request(self):
        """No socket, but an open lease: the job must be queued, not refused."""
        token = self.transport.open("op_sse", "client_1")
        self.assertTrue(self.transport.queue("op_sse", {
            "type": "INFERENCE_REQUEST", "request_id": "req_x", "payload": {},
        }))

        session = self.transport.get(token)
        event = session.events.get_nowait()
        self.assertEqual(event["type"], "INFERENCE_REQUEST")
        self.assertEqual(event["request_id"], "req_x")

    async def test_an_operator_with_neither_transport_is_refused_immediately(self):
        """No lease, no socket: `queue` returns False so the caller can fail fast
        rather than wait out a 60s deadline for a request nobody received."""
        self.assertFalse(self.transport.queue("op_ghost", {"type": "INFERENCE_REQUEST"}))

    async def test_an_expired_lease_does_not_look_live(self):
        """A lease that ran out must stop accepting work, or jobs queue into a
        session whose stream is gone and resolve only by timeout."""
        self.transport.open("op_stale", "client_2")
        token = self.transport.by_operator["op_stale"]
        self.transport.sessions[token].lease_until = 0

        self.assertFalse(self.transport.queue("op_stale", {"type": "INFERENCE_REQUEST"}))
        self.assertNotIn("op_stale", self.transport.operator_ids())


class SourceWiringTests(unittest.TestCase):
    """Asserted against the shipped source, because the bug class here is
    "the code exists and nothing calls it" — invisible to a test that supplies
    the call itself."""

    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        cls.routes = (root / "api/routes/pin.py").read_text()
        cls.service = (root / "api/services/pin_service.py").read_text()

    def test_send_inference_request_can_use_sse(self):
        block = self.routes[self.routes.index("async def send_inference_request"):]
        block = block[:block.index("async def send_tts_request")]
        self.assertIn("sse_transport.queue", block,
                      "an SSE operator has no WebSocket; refusing on `not ws` "
                      "made every SSE operator permanently unreachable")

    def test_the_handler_takes_the_operator_id(self):
        self.assertIn("async def _serve_bus_job(self, operator_id: str", self.routes)
        self.assertNotIn("for cid, oid in list(self.operator_map.items())", self.routes,
                         "scanning the WebSocket map is what broke SSE")

    def test_reachability_counts_an_sse_lease(self):
        self.assertIn("sse_transport.operator_ids()", self.service,
                      "an operator on an SSE lease in THIS process must count as "
                      "local, or it is dispatched onto the bus back to itself")

    def test_sse_transport_is_imported_at_module_scope(self):
        # `_reachability`, `pin_chat_completion` and the streaming path all
        # name `sse_transport`. Importing it only inside `_connected_operator_ids`
        # made those names raise NameError — which Keystone and Playground
        # surfaced as "name 'sse_transport' is not defined".
        self.assertRegex(
            self.service,
            r"(?m)^from api\.services\.pin_sse_transport import transport as sse_transport$",
            "sse_transport must be a module global, not a function-local import",
        )

    def test_stream_inference_request_dispatches_over_sse(self):
        # The streaming path used to check WebSocket only and yield "operator
        # is not connected" for every SSE operator. It must fall through to
        # sse_transport.queue, the same two-transport shape send_inference_request
        # uses — the chunk/result uplinks already feed push_chunk/resolve_request
        # into the same stream_queues, so only the DISPATCH was missing.
        block = self.routes[self.routes.index("async def stream_inference_request"):]
        block = block[:block.index("    async def heartbeat")]
        self.assertIn("sse_transport.queue", block,
                      "stream_inference_request must queue onto the SSE downlink "
                      "when no WebSocket exists, not yield 'operator is not connected'")
        self.assertIn("dispatched", block,
                      "the dispatch must track whether either transport accepted "
                      "the request, so an operator on neither is refused immediately")


if __name__ == "__main__":
    unittest.main()
