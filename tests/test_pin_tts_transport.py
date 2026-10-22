"""TTS must reach an SSE operator, cross workers, and say WHY when it cannot.

THE AFTERNOON THIS COST (2026-09-07)

    HTTP 503
    {"detail":"No operators available for model: tts:chatterbox-turbo.
      Connected operators in this process: ['op_a32a252d-292']."}

Every layer was healthy and said so:

  * the sidecar rendered a real 111KB wav locally in 25s, {"ok":true} on /health
  * `tts:chatterbox-turbo` was in `pin:models:available` on the live gateway
  * chat completions to the SAME operator succeeded minutes apart
  * the operator was connected IN THE VERY PROCESS that refused the call

and the error distinguished none of the states that produce it: an empty model
index, a candidate set resolving to a different (offline) operator, a `failed`
tier tombstone, or an unreadable operator row. `pin_chat_completion` has
printed `selection_evidence` all along. `pin_tts_speech` threw a bare string.

Three defects, all fixed for chat previously and never ported to TTS:

  1. the reachability gate read the WebSocket map only, so an operator on an
     SSE lease -- the default transport since 2026-08-26 -- was excluded no
     matter what selection returned;
  2. nothing ever dispatched onto the bus, leaving `_serve_bus_job`'s
     `kind == "tts"` branch unreachable code and cross-worker speech
     nonexistent;
  3. the failure carried no evidence, which is why the diagnosis took an
     afternoon instead of reading it off the first 503.
"""
import pytest

import api.services.pin_service as svc

from tests.test_pin_stream_e2e import FakeOperator, FakeStorage
from tests.test_pin_tts import GOOD, LINE, FakeTtsWsManager


class SseOnlyWsManager(FakeTtsWsManager):
    """An operator with NO WebSocket: exactly the production shape."""

    def __init__(self, result):
        super().__init__(result, online=False)

    def is_operator_online(self, operator_id):
        return False


def install_sse(monkeypatch, result, sse_ids=("op_test",), holders=0):
    storage = FakeStorage()
    mgr = SseOnlyWsManager(result)
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "get_pin_ws_manager", lambda: mgr)
    monkeypatch.setattr(svc, "reconcile_online_set", lambda m: None)
    monkeypatch.setattr(svc, "select_from_live_sockets", lambda m, model, exc: None)
    monkeypatch.setattr(svc.sse_transport, "operator_ids", lambda: list(sse_ids))
    monkeypatch.setattr(svc.bus, "holders", lambda op: holders)
    return storage, mgr


@pytest.mark.asyncio
async def test_an_sse_leased_operator_is_reachable(monkeypatch):
    """THE PRODUCTION SHAPE. No WebSocket, live SSE downlink.

    Before the fix `is_operator_online()` returned False and the operator was
    excluded three times over, so speech was unreachable for every SSE
    operator even when selection handed it over.
    """
    storage, mgr = install_sse(monkeypatch, GOOD)

    out = await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE, voice="ris")

    assert mgr.sent is not None, "an SSE operator must be sent the TTS request"
    assert mgr.sent["payload"]["model"] == "chatterbox-turbo"
    assert out["pin_metadata"]["routing"] == "local", (
        "an SSE lease in THIS process is local; 'websocket' was hardcoded here "
        "and hid which path actually ran")


@pytest.mark.asyncio
async def test_a_remote_operator_is_dispatched_over_the_bus_as_kind_tts(monkeypatch):
    """The unreachable branch. `_serve_bus_job` handles kind == "tts"; nothing
    published to it, so a job for an operator held by another worker went
    nowhere and surfaced as a timeout."""
    storage, mgr = install_sse(monkeypatch, GOOD, sse_ids=(), holders=1)
    dispatched = {}

    def fake_dispatch(operator_id, request_id, kind, payload):
        dispatched.update(operator_id=operator_id, kind=kind, payload=payload)
        return 1

    async def fake_await_reply(request_id, timeout):
        dispatched["timeout"] = timeout
        return GOOD

    monkeypatch.setattr(svc.bus, "dispatch", fake_dispatch)
    monkeypatch.setattr(svc.bus, "await_reply", fake_await_reply)

    out = await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)

    assert dispatched["kind"] == "tts", "the bus branch is keyed on this string"
    assert dispatched["payload"]["model"] == "chatterbox-turbo", (
        "the daemon must get the BARE name, prefix is routing metadata only")
    assert dispatched["timeout"] == 300.0, (
        "a cold TTS model can load for minutes; chat's 65s would abort it")
    assert mgr.sent is None, "must not also send locally"
    assert out["pin_metadata"]["routing"] == "bus"


@pytest.mark.asyncio
async def test_unreachable_everywhere_reports_why_not_just_that(monkeypatch):
    """No WebSocket, no SSE lease, no bus holder anywhere."""
    storage, mgr = install_sse(monkeypatch, GOOD, sse_ids=(), holders=0)

    with pytest.raises(ValueError) as err:
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)

    msg = str(err.value)
    assert "tts:chatterbox-turbo" in msg
    for token in ("ws_here=", "sse_here=", "bus_holders=", "pid="):
        assert token in msg, (
            f"the reachability verdict must name {token} -- four different "
            f"states produce this failure and the message has to say which")


@pytest.mark.asyncio
async def test_no_candidate_error_names_who_is_registered(monkeypatch):
    """The exact 503 from production, now carrying its own diagnosis.

    `Registered for this model:` is the line that would have ended the hunt on
    the first attempt: it distinguishes "the index is empty" from "the index
    points at an operator that is not online", which the old message merged
    into one indistinguishable string.
    """
    storage = FakeStorage()
    mgr = SseOnlyWsManager(GOOD)
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "get_pin_ws_manager", lambda: mgr)
    monkeypatch.setattr(svc, "reconcile_online_set", lambda m: None)
    monkeypatch.setattr(svc, "select_from_live_sockets", lambda m, model, exc: None)
    monkeypatch.setattr(storage, "pin_select_operator",
                        lambda model, exclude=None, max_price=None: None)
    # The index resolves to an operator that is NOT the connected one -- the
    # state a two-operator deployment lands in after a credential rotation.
    monkeypatch.setattr(storage, "_pin_operators_for_model",
                        lambda model: {"op_someone_else"}, raising=False)
    monkeypatch.setattr(svc, "_connected_operator_ids", lambda m: ["op_a32a252d-292"])
    monkeypatch.setattr(svc, "selection_evidence",
                        lambda m, model: "op_a32a252d-292[serves=False "
                                         "failed_tier=False record=True]")

    with pytest.raises(ValueError) as err:
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)

    msg = str(err.value)
    assert "Registered for this model: ['op_someone_else']" in msg, msg
    assert "serves=False" in msg, "the per-operator verdict must survive"
    assert "op_a32a252d-292" in msg


@pytest.mark.asyncio
async def test_a_diagnostic_failure_never_masks_the_real_error(monkeypatch):
    """A broken index read must not turn 'no operators' into a 500.

    The evidence gathering runs on the failure path, where something is
    already wrong. If it can raise, it replaces an accurate error with an
    unrelated one -- the single worst outcome for the next person debugging.
    """
    storage = FakeStorage()
    mgr = SseOnlyWsManager(GOOD)
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "get_pin_ws_manager", lambda: mgr)
    monkeypatch.setattr(svc, "reconcile_online_set", lambda m: None)
    monkeypatch.setattr(svc, "select_from_live_sockets", lambda m, model, exc: None)
    monkeypatch.setattr(storage, "pin_select_operator",
                        lambda model, exclude=None, max_price=None: None)

    def boom(model):
        raise RuntimeError("index unreadable")

    monkeypatch.setattr(storage, "_pin_operators_for_model", boom, raising=False)

    with pytest.raises(ValueError) as err:
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)

    msg = str(err.value)
    assert "No operators available" in msg, "the real error must survive"
    assert "index unreadable" in msg, "and the diagnostic failure is reported"
