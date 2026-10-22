import asyncio
import time
import unittest

from api.services.pin_sse_transport import PinSseTransport, LEASE_SECONDS


class PinSseTransportTest(unittest.TestCase):
    def test_open_returns_unique_token_per_operator(self):
        t = PinSseTransport()
        a = t.open("op_one", "cid_one")
        b = t.open("op_two", "cid_two")
        self.assertNotEqual(a, b)

    def test_new_open_for_same_operator_revokes_old_lease(self):
        t = PinSseTransport()
        first = t.open("op_one", "cid_one")
        second = t.open("op_one", "cid_two")
        self.assertIsNone(t.get(first))
        self.assertIsNotNone(t.get(second))

    def test_expired_lease_is_invisible(self):
        t = PinSseTransport()
        token = t.open("op_one", "cid_one")
        t.sessions[token].lease_until = time.time() - 1
        self.assertIsNone(t.get(token))

    def test_heartbeat_renews_lease_and_models(self):
        t = PinSseTransport()
        token = t.open("op_one", "cid_one")
        before = t.get(token).lease_until
        time.sleep(0.01)
        session = t.heartbeat(token, models=["muse-local:latest"])
        self.assertIsNotNone(session)
        self.assertGreater(session.lease_until, before)
        self.assertIn("muse-local:latest", session.models)

    def test_queue_delivers_event_to_operator_session(self):
        t = PinSseTransport()
        token = t.open("op_one", "cid_one")
        ok = t.queue("op_one", {"type": "HEARTBEAT_ACK"})
        self.assertTrue(ok)
        event = asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(t.get(token).events.get(), timeout=1.0)
        )
        self.assertEqual(event["type"], "HEARTBEAT_ACK")

    def test_queue_for_unknown_operator_returns_false(self):
        t = PinSseTransport()
        self.assertFalse(t.queue("op_missing", {"type": "x"}))

    def test_operator_ids_lists_live_leases_only(self):
        t = PinSseTransport()
        t.open("op_one", "cid_one")
        dead = t.open("op_two", "cid_two")
        t.sessions[dead].lease_until = time.time() - 1
        self.assertEqual(t.operator_ids(), ["op_one"])

    def test_close_drops_the_lease(self):
        t = PinSseTransport()
        token = t.open("op_one", "cid_one")
        closed = t.close(token)
        self.assertEqual(closed.operator_id, "op_one")
        self.assertEqual(t.operator_ids(), [])
        self.assertFalse(t.queue("op_one", {"type": "INFERENCE_REQUEST"}))


if __name__ == "__main__":
    unittest.main()
