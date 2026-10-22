"""The /api/v1/pin/chat/completions ROUTE honours stream=True.

The streaming service (pin_chat_completion_stream) and the WebSocket chunk
plumbing were both implemented and tested. The public route was not wired to
either: it accepted `stream: true` in the schema and then always called the
non-streaming path, so a client asking for SSE got one buffered JSON object
after the entire generation finished.

That is invisible in a service-level test — every existing streaming test
calls the service directly. These tests exercise the ROUTE, which is the layer
that was actually broken.

Found live: a reasoning model (muse-local) spent minutes in its thinking phase
with the connection idle, and the request died at the gateway with HTTP 504
before it ever produced a token.
"""
import json

import pytest


def sse_events(body: str):
    """Parse an SSE body into its data payloads, [DONE] excluded."""
    out = []
    for line in body.split("\n"):
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]":
            continue
        out.append(json.loads(payload))
    return out


class FakeUser:
    id = "user_test"


async def drain(response):
    """Collect an SSE StreamingResponse body without an HTTP server."""
    parts = []
    async for chunk in response.body_iterator:
        parts.append(chunk if isinstance(chunk, str) else chunk.decode())
    return "".join(parts)


def request(**over):
    from api.models.schemas import PinChatCompletionRequest
    body = {
        "model": "muse-local:latest",
        "messages": [{"role": "user", "content": "hi"}],
    }
    body.update(over)
    return PinChatCompletionRequest(**body)


def _stub_stream(monkeypatch, chunks, error=None):
    """Replace pin_chat_completion_stream with a scripted async generator."""
    import api.services.pin_service as svc

    async def fake_stream(**kwargs):
        fake_stream.kwargs = kwargs
        for c in chunks:
            yield c
        if error is not None:
            raise error

    fake_stream.kwargs = {}
    monkeypatch.setattr(svc, "pin_chat_completion_stream", fake_stream)
    return fake_stream


def _stub_plain(monkeypatch, result):
    import api.services.pin_service as svc

    async def fake_plain(**kwargs):
        fake_plain.kwargs = kwargs
        return result

    fake_plain.kwargs = {}
    monkeypatch.setattr(svc, "pin_chat_completion", fake_plain)
    return fake_plain


def chunk(content=None, reasoning=None, finish=None):
    delta = {}
    if content is not None:
        delta["content"] = content
    if reasoning is not None:
        delta["reasoning"] = reasoning
    return {
        "id": "pin-test",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "muse-local:latest",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }


async def call(**over):
    from api.routes.pin import pin_chat_completions
    return await pin_chat_completions(request(**over), user=FakeUser())


@pytest.mark.asyncio
async def test_stream_true_returns_sse_frames(monkeypatch):
    _stub_stream(monkeypatch, [chunk(content="hello "), chunk(content="world"), chunk(finish="stop")])
    r = await call(stream=True)

    assert r.media_type == "text/event-stream"
    body = await drain(r)
    events = sse_events(body)
    text = "".join(e["choices"][0]["delta"].get("content", "") for e in events)
    assert text == "hello world"
    assert body.rstrip().endswith("data: [DONE]")


@pytest.mark.asyncio
async def test_thinking_reaches_the_client_as_reasoning_deltas(monkeypatch):
    """A reasoning model streams its thinking FIRST. It must not be dropped."""
    _stub_stream(monkeypatch, [
        chunk(reasoning="weighing a couple of angles"),
        chunk(content="the answer"),
        chunk(finish="stop"),
    ])
    body = await drain(await call(stream=True))
    reasoning = "".join(
        e["choices"][0]["delta"].get("reasoning", "") for e in sse_events(body)
    )
    assert reasoning == "weighing a couple of angles"


@pytest.mark.asyncio
async def test_stream_false_still_returns_one_json_object(monkeypatch):
    """The non-streaming contract is unchanged — no regression for callers."""
    payload = {"id": "pin-1", "object": "chat.completion", "choices": []}
    _stub_plain(monkeypatch, payload)
    assert await call(stream=False) == payload


@pytest.mark.asyncio
async def test_omitting_stream_defaults_to_the_buffered_path(monkeypatch):
    payload = {"id": "pin-1", "choices": []}
    _stub_plain(monkeypatch, payload)
    assert await call() == payload


@pytest.mark.asyncio
async def test_error_after_headers_is_reported_IN_the_stream(monkeypatch):
    """Once headers are sent an error cannot be an HTTP status.

    Silently truncating leaves the caller unable to tell a failure from a
    model that simply stopped. The reason goes into the stream instead.
    """
    _stub_stream(monkeypatch, [chunk(content="partial")],
                 error=ValueError("Insufficient PIN credits."))
    body = await drain(await call(stream=True))

    errors = [e for e in sse_events(body) if "error" in e]
    assert len(errors) == 1
    assert "Insufficient PIN credits" in errors[0]["error"]["message"]
    assert body.rstrip().endswith("data: [DONE]")


@pytest.mark.asyncio
async def test_nginx_buffering_is_disabled(monkeypatch):
    """Without X-Accel-Buffering nginx re-buffers and undoes the streaming."""
    _stub_stream(monkeypatch, [chunk(content="x")])
    r = await call(stream=True)
    assert r.headers.get("x-accel-buffering") == "no"
    assert r.headers.get("cache-control") == "no-cache"


@pytest.mark.asyncio
async def test_request_params_are_passed_through_to_the_stream(monkeypatch):
    spy = _stub_stream(monkeypatch, [chunk(content="x")])
    await drain(await call(stream=True, temperature=1.0, max_tokens=16384))

    assert spy.kwargs["model"] == "muse-local:latest"
    assert spy.kwargs["max_tokens"] == 16384
    assert spy.kwargs["temperature"] == 1.0


def test_max_tokens_default_fits_a_reasoning_model():
    """1024 cut muse off mid-thought, with an empty content channel.

    A caller who omits max_tokens must get a budget that a thinking model can
    actually finish inside.
    """
    assert request().max_tokens >= 8192
