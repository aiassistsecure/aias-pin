"""pin_tts_speech end-to-end with storage and the WS manager stubbed.

The daemon side (pin-clientd v2.4.0) is tested in its own repo; this covers
the service stretch — model-name normalization, selection on the tts:-prefixed
name, the job record, the PROPOSED char-based billing, and error surfacing.
"""
import pytest

import api.services.pin_service as svc

from tests.test_pin_stream_e2e import FakeBilling, FakeJob, FakeOperator, FakeStorage


class FakeTtsWsManager:
    def __init__(self, result, online=True):
        self.result = result
        self.operator_map = {"cid": "op_test"} if online else {}
        self._online = online
        self.sent = None

    def is_operator_online(self, operator_id):
        return self._online

    async def send_tts_request(self, operator_id, request_id, payload, timeout=300.0):
        self.sent = {"operator_id": operator_id, "request_id": request_id, "payload": payload}
        return self.result


def install(monkeypatch, result, online=True):
    storage = FakeStorage()
    mgr = FakeTtsWsManager(result, online=online)
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "get_pin_ws_manager", lambda: mgr)
    monkeypatch.setattr(svc, "reconcile_online_set", lambda m: None)
    monkeypatch.setattr(svc, "select_from_live_sockets", lambda m, model, exc: None)
    return storage, mgr


GOOD = {
    "model": "chatterbox-turbo",
    "voice": "ris",
    "format": "wav",
    "content_type": "audio/wav",
    "bytes": 6,
    "audio_b64": "//MAAf6A",
}

LINE = "Welcome to the room. No strangers. Just your circle."


@pytest.mark.asyncio
async def test_tts_round_trip_normalizes_names_and_bills_chars(monkeypatch):
    storage, mgr = install(monkeypatch, GOOD)

    # Consumer may pass the prefixed name; the daemon must get the bare name.
    out = await svc.pin_tts_speech("user_1", "tts:chatterbox-turbo", LINE, voice="ris")

    assert mgr.sent["payload"]["model"] == "chatterbox-turbo"
    assert mgr.sent["payload"]["input"] == LINE
    assert mgr.sent["payload"]["voice"] == "ris"
    assert "speed" not in mgr.sent["payload"], "None speed must not go over the wire"

    assert out["audio_b64"] == "//MAAf6A"
    assert out["content_type"] == "audio/wav"
    assert out["model"] == "chatterbox-turbo"

    # PROPOSED billing: input chars as tokens, through the same atomic path.
    assert len(storage.completed) == 1
    billed = storage.completed[0]
    assert billed["total_tokens"] == len(LINE)
    assert billed["model"] == "tts:chatterbox-turbo"
    assert out["pin_metadata"]["billed_unit"] == "input_chars"
    assert out["pin_metadata"]["billed_units"] == len(LINE)


@pytest.mark.asyncio
async def test_bare_model_name_is_selected_with_the_tts_prefix(monkeypatch):
    storage, mgr = install(monkeypatch, GOOD)
    seen = {}

    def selecting(model=None, exclude=None, max_price=None):
        seen["model"] = model
        return FakeOperator()

    monkeypatch.setattr(storage, "pin_select_operator", selecting)
    await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)
    assert seen["model"] == "tts:chatterbox-turbo", "selection must use the registered name"


@pytest.mark.asyncio
async def test_operator_error_fails_the_job_and_raises(monkeypatch):
    storage, mgr = install(monkeypatch, {"error": "no TTS backend configured on this operator"})
    with pytest.raises(ValueError, match="All operators failed"):
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)
    assert storage.failed, "the job must be marked failed"
    assert not storage.completed, "billing must not run on a failed render"


@pytest.mark.asyncio
async def test_missing_audio_is_an_error_not_an_empty_clip(monkeypatch):
    storage, mgr = install(monkeypatch, {"model": "chatterbox-turbo"})  # no audio_b64
    with pytest.raises(ValueError, match="All operators failed"):
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)
    assert not storage.completed


@pytest.mark.asyncio
async def test_empty_input_refuses_before_touching_an_operator(monkeypatch):
    storage, mgr = install(monkeypatch, GOOD)
    with pytest.raises(ValueError, match="empty"):
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", "   ")
    assert mgr.sent is None, "no operator call for empty input"
    assert not storage.completed and not storage.failed


@pytest.mark.asyncio
async def test_no_credits_refuses_before_touching_an_operator(monkeypatch):
    storage, mgr = install(monkeypatch, GOOD)

    class Broke:
        credits_balance = 0

    monkeypatch.setattr(storage, "pin_get_user_billing", lambda uid: Broke())
    with pytest.raises(ValueError, match="Insufficient"):
        await svc.pin_tts_speech("user_1", "chatterbox-turbo", LINE)
    assert mgr.sent is None
