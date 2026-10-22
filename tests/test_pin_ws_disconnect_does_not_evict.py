"""A closing WebSocket must not evict an operator that is live on SSE.

THE BUG THIS GUARDS (observed in production 2026-09-07)

The daemon held an SSE downlink and heartbeated every 15s, every heartbeat
answering 200. The gateway still refused every request:

    No operators available for model: tts:chatterbox-turbo.
    Connected operators in this process: ['op_a32a252d-292'].

On disk, two artifacts told the story together:

    pin:operators:online          -> {'members': []}      (freshly written)
    pin:heartbeats:op_a32a252d-292 -> None                 (deleted)

Exactly one code path does BOTH of those things: `PinClientConnectionManager
.disconnect()`. A stale WebSocket for the same operator id — left over from an
earlier run, a reconnect, a dashboard tab — closed, and its cleanup removed an
operator whose SSE downlink was alive and heartbeating.

`pin_sse_transport.is_current_lease` documents this failure verbatim and guards
the SSE teardown against it ("The corpse must not evict the living"). The
WebSocket teardown never got the same guard, so the identical bug survived on
the other transport.

The rule: eviction is only correct when NOTHING else is holding the operator —
not another socket in this worker, and not an SSE lease in any worker.

These are behavioural tests: the real manager, the real shim, a faithful store
stub underneath. `disconnect()` is synchronous and self-contained, so there is
no reason to assert against source text here.
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.services.nedb_redis_shim import RedisOnNedb  # noqa: E402
from tests.test_pin_bus_on_nedb_shim import StubStore  # noqa: E402

ONLINE = "aiconsult:pin:operators:online"
OP = "op_a32a252d-292"


class WsDisconnectEviction(unittest.TestCase):

    def setUp(self):
        import api.routes.pin as pin
        from api.services.pin_sse_transport import transport

        self.pin = pin
        self.transport = transport
        self.shim = RedisOnNedb(StubStore())

        # Real shim, faithful store, in place of the module's storage.
        self._real_storage = pin.storage
        pin.storage = types.SimpleNamespace(r=self.shim)

        # A fresh transport state so leases from other tests cannot leak in.
        transport.sessions.clear()
        transport.by_operator.clear()

        self.manager = pin.PinClientConnectionManager()

    def tearDown(self):
        self.pin.storage = self._real_storage
        self.transport.sessions.clear()
        self.transport.by_operator.clear()

    def _mark_online(self):
        self.shim.sadd(ONLINE, OP)
        self.shim.setex(f"aiconsult:pin:heartbeats:{OP}", 300, "1757000000.0")

    def _attach_ws(self, client_id):
        self.manager.active_connections[client_id] = object()
        self.manager.operator_map[client_id] = OP

    def test_a_stale_ws_close_leaves_a_live_sse_operator_online(self):
        """THE PRODUCTION BUG. SSE downlink is live; an old WebSocket closes."""
        self.transport.open(OP, "sse-client")          # live SSE lease
        self._attach_ws("stale-ws-client")
        self._mark_online()

        self.manager.disconnect("stale-ws-client")

        self.assertIn(OP, self.shim.smembers(ONLINE),
                      "an operator with a live SSE downlink must stay online "
                      "when an unrelated WebSocket closes")
        self.assertIsNotNone(
            self.shim.get(f"aiconsult:pin:heartbeats:{OP}"),
            "deleting the heartbeat makes the reaper evict it moments later, "
            "so the eviction survives even if the online set is repaired")

    def test_a_second_live_ws_also_prevents_eviction(self):
        """Two sockets, one operator. Closing one must not evict the other."""
        self._attach_ws("ws-a")
        self._attach_ws("ws-b")
        self._mark_online()

        self.manager.disconnect("ws-a")

        self.assertIn(OP, self.shim.smembers(ONLINE),
                      "ws-b is still holding this operator")

    def test_the_last_connection_closing_DOES_evict(self):
        """The guard must not become a leak: when nothing holds the operator,
        eviction is correct and must still happen. A guard that never evicts
        leaves dead operators selectable, which is worse than this bug — it
        turns an instant 'no operator' into a full inference timeout."""
        self._attach_ws("only-ws")
        self._mark_online()

        self.manager.disconnect("only-ws")

        self.assertNotIn(OP, self.shim.smembers(ONLINE),
                         "with no socket and no lease left, the operator is "
                         "genuinely gone and must be removed")
        self.assertIsNone(self.shim.get(f"aiconsult:pin:heartbeats:{OP}"))

    def test_disconnect_still_clears_its_own_bookkeeping(self):
        """Whatever the eviction decision, the socket itself is always dropped."""
        self.transport.open(OP, "sse-client")
        self._attach_ws("ws-x")
        self._mark_online()

        self.manager.disconnect("ws-x")

        self.assertNotIn("ws-x", self.manager.active_connections)
        self.assertNotIn("ws-x", self.manager.operator_map)


if __name__ == "__main__":
    unittest.main(verbosity=2)
