"""An SSE uplink must authenticate on ANY worker, and a dead stream must not
evict the operator that replaced it.

THE PRODUCTION FAILURE (2026-08-28)

A diagnostic run against the live store, while the daemon was connected and
completing jobs:

    pin:models:muse-local:latest       -> ['op_a32a252d-292']
    _operator_serves(model)            : True
    _is_failed                         : False
    pin_get_operator loads             : True
    in pin:operators:online            : False        <-- the gate
    online set                         : EMPTY

Everything the selector needed was correct except one derived cache, and the
request was refused with "No operators available for model".

TWO CAUSES, BOTH WORKER LOCALITY

1. `/stream/heartbeat` validated the stream token against `sse_transport`,
   which is a dict in ONE worker's memory. Uplinks are ordinary HTTP requests
   that the balancer sprays across all workers, so most heartbeats 404'd
   BEFORE the `sadd(pin:operators:online, ...)` below the check — and the
   heartbeat reaper duly evicted a live operator. `/stream/register` had the
   same gate, which is why model advertisement only ever landed by luck.

2. The downlink generator's `finally` ran an unconditional
   `srem(pin:operators:online, operator_id)`. A reconnect revokes the old
   lease; the old generator then finishes and removes the operator that just
   reconnected. The corpse evicts the living.
"""
import asyncio
import importlib.util
import pathlib
import sys

import pytest


@pytest.fixture(autouse=True)
def _event_loop_for_queue():
    """`OperatorSession` builds an `asyncio.Queue`, which needs a loop on 3.9.

    In production that always happens inside the request's loop. Under pytest,
    a sibling test can close the loop and leave the policy holding a dead one,
    so these tests install their own rather than depending on collection order.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield
    loop.close()


def _fresh_transport_module():
    """Load the transport straight from disk, immune to sibling stubs."""
    path = (pathlib.Path(__file__).resolve().parent.parent
            / "api" / "services" / "pin_sse_transport.py")
    spec = importlib.util.spec_from_file_location(
        "_pin_sse_transport_lease_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeRedis:
    """setex/get/delete with no TTL expiry — the tests drive time explicitly."""

    def __init__(self):
        self.kv = {}

    def setex(self, k, _ttl, v):
        self.kv[k] = v
        return True

    def get(self, k):
        return self.kv.get(k)

    def delete(self, k):
        self.kv.pop(k, None)
        return 1


OP = "op_a32a252d-292"


def _two_workers():
    """Two transports sharing one Redis — the production topology."""
    mod = _fresh_transport_module()
    shared = FakeRedis()
    mod._redis = lambda: shared
    holder = mod.PinSseTransport()      # holds the downlink
    other = mod.PinSseTransport()       # where the uplink happens to land
    return mod, shared, holder, other


def test_an_uplink_authenticates_on_a_worker_that_does_not_hold_the_downlink():
    """The heartbeat 404 that emptied the online set."""
    _, _, holder, other = _two_workers()
    token = holder.open(OP, "client-1")

    assert other.get(token) is None, "the other worker really has no local session"
    assert other.resolve(token) == OP, \
        "but it must still be able to authenticate the uplink"


def test_renewing_from_another_worker_keeps_the_lease_alive():
    """A heartbeat landing anywhere must count, or the downlink lapses under a
    worker that never sees the traffic keeping it alive."""
    _, shared, holder, other = _two_workers()
    token = holder.open(OP, "client-1")

    assert other.renew(token) == OP
    assert shared.get(f"aiconsult:pin:sse_lease:{token}") == OP


def test_an_unknown_token_is_still_rejected_everywhere():
    """Sharing the lease must not become trusting any string."""
    _, _, holder, other = _two_workers()
    holder.open(OP, "client-1")
    assert other.resolve("not-a-real-token") is None
    assert other.renew("not-a-real-token") is None


def test_a_reconnect_makes_the_previous_lease_stale():
    """The condition the teardown path has to test."""
    _, _, holder, _ = _two_workers()
    first = holder.open(OP, "client-1")
    assert holder.is_current_lease(OP, first)

    second = holder.open(OP, "client-1")     # daemon reconnected
    assert second != first
    assert holder.is_current_lease(OP, second)
    assert not holder.is_current_lease(OP, first), \
        "the old stream must know it is a corpse before it evicts anyone"


def test_the_reconnect_can_land_on_a_different_worker():
    """Reconnects re-roll which worker holds the socket, so 'is this still the
    live lease' cannot be answered from local memory alone."""
    _, _, holder, other = _two_workers()
    first = holder.open(OP, "client-1")
    second = other.open(OP, "client-1")      # reconnected elsewhere

    assert not holder.is_current_lease(OP, first), \
        "the old holder must see that another worker now owns the lease"
    assert other.is_current_lease(OP, second)


def test_without_redis_the_single_worker_path_still_works():
    """Degradation must be graceful: no shared store is the dev setup, and it
    behaved correctly before any of this existed."""
    mod = _fresh_transport_module()
    mod._redis = lambda: None
    t = mod.PinSseTransport()
    token = t.open(OP, "client-1")

    assert t.resolve(token) == OP
    assert t.renew(token) == OP
    assert t.is_current_lease(OP, token)
