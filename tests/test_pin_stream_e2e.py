"""End-to-end pass through pin_chat_completion_stream with storage stubbed.

The manager-level tests cover the socket plumbing. This exercises the service
generator itself -- selection, the job record, billing, and the shape of every
chunk handed to ProviderRouter -- which is the stretch of code that runs
inside the live SSE response and whose exceptions surface to the browser as a
bare "network error".
"""
import asyncio
import types

import pytest

import api.services.pin_service as svc


class FakeOperator:
    id = "op_test"
    pricing_per_1k_tokens = 0.01


class FakeJob:
    id = "job_test"


class FakeBilling:
    credits_balance = 10.0


class FakeStorage:
    def __init__(self):
        self.completed = []
        self.failed = []

    def pin_get_user_billing(self, user_id):
        return FakeBilling()

    def pin_select_operator(self, model=None, exclude=None, max_price=None):
        return FakeOperator()

    def pin_create_job(self, user_id, operator_id, model):
        return FakeJob()

    def pin_complete_job_atomic(self, **kw):
        self.completed.append(kw)
        return {"success": True, "total_cost": 0.05}

    def pin_fail_job(self, job_id, reason):
        self.failed.append((job_id, reason))

    def pin_update_reputation(self, operator_id, success=True, latency_ms=0):
        pass


class FakeWsManager:
    """Stands in for PinClientConnectionManager."""

    def __init__(self, events, online=True):
        self.events = events
        self.operator_map = {"cid": "op_test"} if online else {}
        self._online = online

    def is_operator_online(self, operator_id):
        return self._online

    async def stream_inference_request(self, operator_id, request_id, payload,
                                       idle_timeout=90.0):
        self.payload = payload
        for e in self.events:
            yield e


def install(monkeypatch, events, online=True):
    storage = FakeStorage()
    mgr = FakeWsManager(events, online=online)
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "get_pin_ws_manager", lambda: mgr)
    monkeypatch.setattr(svc, "reconcile_online_set", lambda m: None)
    monkeypatch.setattr(svc, "select_from_live_sockets", lambda m, model, exc: None)
    return storage, mgr


FINAL = {
    "type": "final",
    "result": {
        "choices": [{"message": {"role": "assistant", "content": "Hello"},
                     "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
    },
}


async def drain(agen):
    return [c async for c in agen]


@pytest.mark.asyncio
async def test_streams_chunks_then_a_terminal_chunk_with_usage(monkeypatch):
    storage, mgr = install(monkeypatch, [
        {"type": "chunk", "index": 0, "delta": "Hel"},
        {"type": "chunk", "index": 1, "delta": "lo"},
        FINAL,
    ])

    chunks = await drain(svc.pin_chat_completion_stream(
        user_id="u1", model="muse-local:latest",
        messages=[{"role": "user", "content": "hi"}]))

    # every chunk must be OpenAI-shaped: the playground reads
    # chunk["choices"][0]["delta"]["content"] without guarding
    for c in chunks:
        assert c["object"] == "chat.completion.chunk"
        assert c["choices"] and "delta" in c["choices"][0]

    text = "".join(c["choices"][0]["delta"].get("content", "") for c in chunks)
    assert text == "Hello"

    assert chunks[-1]["choices"][0]["finish_reason"] == "stop"
    assert chunks[-1]["usage"]["total_tokens"] == 5
    # NB: `stream: True` is added inside the real manager's
    # stream_inference_request, not by the service -- asserted in
    # tests/test_pin_streaming.py against the real class. Here we only check
    # the service passes through what the operator needs.
    assert mgr.payload["model"] == "muse-local:latest"
    assert mgr.payload["messages"][0]["content"] == "hi"

    # billed exactly once, off the final message
    assert len(storage.completed) == 1
    assert storage.completed[0]["total_tokens"] == 5
    assert storage.failed == []


@pytest.mark.asyncio
async def test_old_daemon_sending_no_chunks_still_delivers(monkeypatch):
    install(monkeypatch, [FINAL])
    chunks = await drain(svc.pin_chat_completion_stream(
        user_id="u1", model="m", messages=[{"role": "user", "content": "hi"}]))
    text = "".join(c["choices"][0]["delta"].get("content", "") for c in chunks)
    assert text == "Hello", "pre-2.3.0 daemon must not produce an empty stream"


@pytest.mark.asyncio
async def test_operator_error_fails_the_job_and_raises(monkeypatch):
    storage, _ = install(monkeypatch, [
        {"type": "chunk", "index": 0, "delta": "partial"},
        {"type": "error", "error": "operator disconnected mid-stream"},
    ])
    with pytest.raises(Exception) as exc:
        await drain(svc.pin_chat_completion_stream(
            user_id="u1", model="m", messages=[{"role": "user", "content": "hi"}]))
    assert "disconnected" in str(exc.value)
    assert storage.failed and storage.completed == []


@pytest.mark.asyncio
async def test_no_credits_refuses_before_touching_an_operator(monkeypatch):
    storage, _ = install(monkeypatch, [FINAL])
    storage.pin_get_user_billing = lambda uid: types.SimpleNamespace(credits_balance=0)
    with pytest.raises(ValueError) as exc:
        await drain(svc.pin_chat_completion_stream(
            user_id="u1", model="m", messages=[{"role": "user", "content": "hi"}]))
    assert "credits" in str(exc.value).lower()


@pytest.mark.asyncio
async def test_billing_failure_after_delivery_does_not_kill_the_stream(monkeypatch):
    # The tokens are already in the user's hands; raising here would blank a
    # visibly-complete answer. It must be recorded, not thrown.
    storage, _ = install(monkeypatch, [
        {"type": "chunk", "index": 0, "delta": "Hello"}, FINAL])
    storage.pin_complete_job_atomic = lambda **kw: {"success": False,
                                                    "error": "INSUFFICIENT_CREDITS"}
    chunks = await drain(svc.pin_chat_completion_stream(
        user_id="u1", model="m", messages=[{"role": "user", "content": "hi"}]))
    assert "".join(c["choices"][0]["delta"].get("content", "") for c in chunks) == "Hello"
    assert storage.failed, "the failure has to be recorded somewhere"


@pytest.mark.asyncio
async def test_reasoning_deltas_become_delta_reasoning(monkeypatch):
    """A thinking model's reasoning must reach the browser as it happens.

    muse-local emits its reasoning with content EMPTY for the whole thinking
    phase. Those chunks arrive with kind="thinking" and go out as
    `delta.reasoning`, which the playground already streams as a live
    thinking trace -- same channel the anthropic adapter uses.
    """
    install(monkeypatch, [
        {"type": "chunk", "index": 0, "delta": "hmm", "kind": "thinking"},
        {"type": "chunk", "index": 1, "delta": "...", "kind": "thinking"},
        {"type": "chunk", "index": 2, "delta": "Hello", "kind": "content"},
        FINAL,
    ])
    chunks = await drain(svc.pin_chat_completion_stream(
        user_id="u1", model="muse-local:latest",
        messages=[{"role": "user", "content": "hi"}]))

    reasoning = "".join(c["choices"][0]["delta"].get("reasoning", "") for c in chunks)
    content = "".join(c["choices"][0]["delta"].get("content", "") for c in chunks)
    assert reasoning == "hmm..."
    assert content == "Hello", "reasoning must not leak into the answer"


@pytest.mark.asyncio
async def test_chunks_without_a_kind_are_treated_as_content(monkeypatch):
    # A pre-2.4 daemon sends no `kind`. It must keep working unchanged.
    install(monkeypatch, [
        {"type": "chunk", "index": 0, "delta": "Hello"}, FINAL])
    chunks = await drain(svc.pin_chat_completion_stream(
        user_id="u1", model="m", messages=[{"role": "user", "content": "hi"}]))
    assert "".join(c["choices"][0]["delta"].get("content", "") for c in chunks) == "Hello"


@pytest.mark.asyncio
async def test_reasoning_with_no_answer_says_so_instead_of_ending_silent(monkeypatch):
    # Budget exhausted mid-thought: the old degrade path would emit nothing at
    # all and the user would see an empty bubble.
    install(monkeypatch, [
        {"type": "chunk", "index": 0, "delta": "thinking hard", "kind": "thinking"},
        {"type": "final", "result": {"choices": [{"message": {"content": ""}}],
                                     "usage": {"total_tokens": 1024}}},
    ])
    chunks = await drain(svc.pin_chat_completion_stream(
        user_id="u1", model="m", messages=[{"role": "user", "content": "hi"}]))
    content = "".join(c["choices"][0]["delta"].get("content", "") for c in chunks)
    assert "no answer" in content and "max tokens" in content
