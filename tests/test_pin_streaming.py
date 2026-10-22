"""PIN WebSocket streaming plumbing.

Covers the manager layer that carries INFERENCE_CHUNK from an operator's
socket to a waiting consumer. Before this existed, ProviderRouter fell into
its `if not hasattr(adapter, 'stream')` branch and delivered the whole
completion as a single chunk at the end.

No network and no storage: a fake socket stands in for the operator, which is
enough to exercise every path that decides whether a consumer gets tokens,
an error, or hangs.
"""
import asyncio

import pytest

from api.routes.pin import PinClientConnectionManager


class FakeWebSocket:
    def __init__(self, fail: bool = False):
        self.sent = []
        self.fail = fail

    async def send_json(self, payload):
        if self.fail:
            raise RuntimeError("socket is gone")
        self.sent.append(payload)


def connect(mgr, operator_id="op_1", client_id="cid_1", **kw):
    ws = FakeWebSocket(**kw)
    mgr.active_connections[client_id] = ws
    mgr.operator_map[client_id] = operator_id
    return ws


async def collect(agen):
    return [e async for e in agen]


@pytest.mark.asyncio
async def test_chunks_stream_then_final_terminates():
    mgr = PinClientConnectionManager()
    ws = connect(mgr)

    async def operator():
        # Wait for the request to be registered, then answer like a daemon.
        while "req1" not in mgr.stream_queues:
            await asyncio.sleep(0.001)
        mgr.push_chunk("req1", 0, "Hel")
        mgr.push_chunk("req1", 1, "lo")
        mgr.resolve_request("req1", {
            "choices": [{"message": {"role": "assistant", "content": "Hello"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
        })

    task = asyncio.create_task(operator())
    events = await collect(mgr.stream_inference_request("op_1", "req1", {"model": "m"}))
    await task

    assert [e["type"] for e in events] == ["chunk", "chunk", "final"]
    assert "".join(e["delta"] for e in events if e["type"] == "chunk") == "Hello"
    assert events[-1]["result"]["usage"]["total_tokens"] == 5

    # stream=True must be set for the daemon to stream at all
    assert ws.sent[0]["payload"]["stream"] is True
    assert ws.sent[0]["request_id"] == "req1"

    # no leaks once the stream ends
    assert mgr.stream_queues == {}
    assert mgr.stream_owners == {}


@pytest.mark.asyncio
async def test_offline_operator_errors_instead_of_hanging():
    mgr = PinClientConnectionManager()
    events = await collect(mgr.stream_inference_request("op_missing", "r", {"model": "m"}))
    assert events == [{"type": "error", "error": "operator is not connected"}]


@pytest.mark.asyncio
async def test_send_failure_is_reported_not_swallowed():
    mgr = PinClientConnectionManager()
    connect(mgr, fail=True)
    events = await collect(mgr.stream_inference_request("op_1", "r", {"model": "m"}))
    assert len(events) == 1 and events[0]["type"] == "error"
    assert "failed to reach operator" in events[0]["error"]
    assert mgr.stream_queues == {}


@pytest.mark.asyncio
async def test_idle_timeout_bounds_the_gap_between_chunks_not_total_duration():
    mgr = PinClientConnectionManager()
    connect(mgr)

    async def slow_but_alive():
        # Three chunks, each arriving well inside the idle window. Total
        # elapsed exceeds the idle timeout -- a long answer must not be
        # killed for taking a while.
        for i in range(3):
            while "req" not in mgr.stream_queues:
                await asyncio.sleep(0.001)
            await asyncio.sleep(0.06)
            mgr.push_chunk("req", i, "x")
        mgr.resolve_request("req", {"choices": [], "usage": {}})

    task = asyncio.create_task(slow_but_alive())
    events = await collect(
        mgr.stream_inference_request("op_1", "req", {"model": "m"}, idle_timeout=0.1))
    await task
    assert [e["type"] for e in events] == ["chunk", "chunk", "chunk", "final"]


@pytest.mark.asyncio
async def test_silent_operator_times_out():
    mgr = PinClientConnectionManager()
    connect(mgr)
    events = await collect(
        mgr.stream_inference_request("op_1", "req", {"model": "m"}, idle_timeout=0.05))
    assert len(events) == 1 and events[0]["type"] == "error"
    assert "produced nothing" in events[0]["error"]
    assert mgr.stream_queues == {}


@pytest.mark.asyncio
async def test_disconnect_fails_inflight_streams_immediately():
    mgr = PinClientConnectionManager()
    connect(mgr)

    async def dies_midway():
        while "req" not in mgr.stream_queues:
            await asyncio.sleep(0.001)
        mgr.push_chunk("req", 0, "partial")
        mgr.disconnect("cid_1")

    task = asyncio.create_task(dies_midway())
    # A generous idle timeout: if the disconnect did NOT unblock the consumer,
    # this test would take 30s instead of milliseconds.
    events = await asyncio.wait_for(
        collect(mgr.stream_inference_request("op_1", "req", {"model": "m"},
                                             idle_timeout=30.0)),
        timeout=5.0,
    )
    await task
    assert [e["type"] for e in events] == ["chunk", "error"]
    assert "disconnected" in events[-1]["error"]


@pytest.mark.asyncio
async def test_chunks_for_unknown_request_are_ignored():
    # A cancelled or already-finished request can still have chunks in
    # flight. That must not raise.
    mgr = PinClientConnectionManager()
    mgr.push_chunk("never-registered", 0, "x")
    mgr.resolve_request("never-registered", {"choices": []})


@pytest.mark.asyncio
async def test_two_concurrent_streams_do_not_cross():
    mgr = PinClientConnectionManager()
    connect(mgr, operator_id="op_1", client_id="cid_1")
    connect(mgr, operator_id="op_2", client_id="cid_2")

    async def feed():
        while len(mgr.stream_queues) < 2:
            await asyncio.sleep(0.001)
        mgr.push_chunk("rA", 0, "A1")
        mgr.push_chunk("rB", 0, "B1")
        mgr.push_chunk("rA", 1, "A2")
        mgr.resolve_request("rB", {"choices": []})
        mgr.resolve_request("rA", {"choices": []})

    task = asyncio.create_task(feed())
    a, b = await asyncio.gather(
        collect(mgr.stream_inference_request("op_1", "rA", {"model": "m"})),
        collect(mgr.stream_inference_request("op_2", "rB", {"model": "m"})),
    )
    await task
    assert "".join(e["delta"] for e in a if e["type"] == "chunk") == "A1A2"
    assert "".join(e["delta"] for e in b if e["type"] == "chunk") == "B1"


@pytest.mark.asyncio
async def test_adapter_exposes_stream_so_router_stops_using_the_fallback():
    # The router does `if not hasattr(adapter, 'stream')` and, when true,
    # awaits complete() and yields the whole answer as one chunk. This is the
    # assertion that keeps PIN out of that branch.
    from api.providers.adapters.pin import PinAdapter
    import inspect

    assert hasattr(PinAdapter, "stream")
    assert inspect.isasyncgenfunction(PinAdapter.stream)
