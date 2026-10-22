"""An abandoned generation is CANCELLED, not merely unobserved.

THE ZOMBIE THIS KILLS (observed 2026-08-29)

The caller aborts a looping stream — correctly — and retries — correctly.
The operator keeps generating the aborted attempt to its last token, because
nothing ever told it to stop. Each retry stacks another full-resume
generation onto one GPU, and ollama splits its context across parallel
slots, so the requests do not even degrade gracefully. The visible symptom:
'model online, PIN queue overflowed', with every layer locally right.

THE RULE

Every exit from a waiting path that is NOT the operator's own terminal event
sends CANCEL_REQUEST back down the operator's downlink:

  * the streaming consumer disconnects mid-generation (GeneratorExit),
  * the inter-chunk idle timeout fires,
  * the non-streaming future times out.

Best-effort by design: an operator that never hears the cancel just finishes
work nobody collects — exactly today's behavior — and a pre-cancel daemon
ignores the unknown message type. Delivery is the same three hops dispatch
uses: local WebSocket, local SSE queue, operator bus.

Source-read tests, the house style and for the house reason: these paths
live inside long async functions gated on credits and live sockets.
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_pin_stream_relay import FakeRedis, load_bus  # noqa: E402


class CancelReachesTheOperator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.source = (Path(__file__).resolve().parents[1]
                      / "api" / "routes" / "pin.py").read_text(encoding="utf-8")

    def test_the_manager_has_a_cancel_with_all_three_hops(self):
        body = self.source.split("def cancel_operator_job")[1].split("\n    def ")[0]
        self.assertIn('{"type": "CANCEL_REQUEST", "request_id": request_id}', body)
        self.assertIn("get_websocket_for_operator", body)
        self.assertIn("sse_transport.queue", body)
        self.assertIn('bus.dispatch(operator_id, request_id, "cancel", {})', body,
                      "a downlink held by ANOTHER worker must still hear the cancel")

    def test_a_stream_that_ends_without_a_terminal_event_cancels(self):
        body = self.source.split("async def stream_inference_request")[1]
        self.assertIn("finished = False", body)
        self.assertIn("finished = True", body)
        finally_block = body.split("finally:")[1][:900]
        self.assertIn("if not finished:", finally_block)
        self.assertIn("self.cancel_operator_job(operator_id, request_id)", finally_block,
                      "GeneratorExit and the idle timeout both land in this finally — "
                      "both mean nobody is waiting anymore")

    def test_the_operator_s_own_terminal_event_does_not_cancel(self):
        # finished flips BEFORE the terminal yield: a final that arrived is a
        # generation that already stopped, and cancelling it is noise.
        body = self.source.split("async def stream_inference_request")[1]
        flip = body.index("finished = True")
        terminal_yield = body.index("yield event\n                return")
        self.assertLess(flip, terminal_yield)

    def test_the_nonstreaming_timeout_cancels_too(self):
        body = self.source.split("async def send_inference_request")[1]
        head = body.split("async def send_tts_request")[0]
        timeout_arm = head.split("except asyncio.TimeoutError:")[1].split("finally:")[0]
        self.assertIn("self.cancel_operator_job(operator_id, request_id)", timeout_arm,
                      "a timed-out job must not keep burning the GPU in the dark")

    def test_the_bus_job_handler_forwards_a_cancel_without_waiting(self):
        body = self.source.split("async def _serve_bus_job")[1]
        head = body.split("async def ")[0]
        self.assertIn('if kind == "cancel":', head)
        arm = head.split('if kind == "cancel":')[1].split('if kind == "chat_stream":')[0]
        self.assertIn('{"type": "CANCEL_REQUEST", "request_id": request_id}', arm)
        self.assertNotIn("await_reply", arm, "there is no consumer; that is the point")
        self.assertNotIn("send_inference_request", arm)


class CancelCrossesWorkers(unittest.IsolatedAsyncioTestCase):
    """The bus leg, driven for real against the fake."""

    async def test_a_cancel_dispatched_on_the_bus_reaches_the_holding_worker(self):
        bus = load_bus(FakeRedis())
        seen = []

        async def handler(operator_id, kind, request_id, payload):
            seen.append((operator_id, kind, request_id))
            return {"queued": True}

        serving = asyncio.create_task(bus.serve("op_1", handler))
        await asyncio.sleep(0.05)

        delivered = bus.dispatch("op_1", "req_zombie", "cancel", {})
        self.assertEqual(delivered, 1)
        await asyncio.sleep(0.15)
        serving.cancel()

        self.assertEqual(seen, [("op_1", "cancel", "req_zombie")],
                         "the worker holding the downlink hears which request to kill")


if __name__ == "__main__":
    unittest.main()
