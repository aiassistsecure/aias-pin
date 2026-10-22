"""PIN SSE transport: persistent operator downlink, independent HTTP uplink."""
import asyncio
import json
import secrets
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

LEASE_SECONDS = 45


@dataclass
class OperatorSession:
    operator_id: str
    client_id: str
    lease_until: float
    events: asyncio.Queue = field(default_factory=asyncio.Queue)
    models: set = field(default_factory=set)


def _redis():
    """Lazy, and never fatal: the in-memory path must keep working alone."""
    try:
        from api.services.redis_storage import storage
        return storage.r
    except Exception:  # noqa: BLE001
        return None


def _lease_key(token: str) -> str:
    return f"aiconsult:pin:sse_lease:{token}"


def _token_key(operator_id: str) -> str:
    return f"aiconsult:pin:sse_token:{operator_id}"


class PinSseTransport:
    """Leases in Redis, event queues in memory.

    THE SPLIT MATTERS, AND GETTING IT WRONG COST A DAY.

    The DOWNLINK genuinely belongs to one worker: an `asyncio.Queue` cannot be
    read from another process, which is what `pin_operator_bus` exists to
    bridge. The UPLINKS — heartbeat, register, result, chunk — are ordinary
    HTTP requests, and the load balancer sprays them across every worker.

    Validating an uplink against a dict in one worker's memory therefore fails
    for (N-1)/N of them. In production that meant:

      * `/stream/heartbeat` 404'd on the wrong worker, so the
        `sadd(pin:operators:online, ...)` after the check never ran, the
        heartbeat reaper evicted the operator, and `pin_select_operator`
        filtered out an operator that was connected and serving.
      * `/stream/register` succeeded only when it happened to land on the
        right worker, which is why the model indexes drifted in the first
        place.

    So the lease — identity and expiry, the part every worker needs — lives in
    Redis under a TTL. The event queue stays local, because only the holder can
    use it.
    """

    def __init__(self):
        self.sessions: Dict[str, OperatorSession] = {}
        self.by_operator: Dict[str, str] = {}

    def open(self, operator_id: str, client_id: str) -> str:
        old = self.by_operator.get(operator_id)
        if old:
            self.sessions.pop(old, None)
        token = secrets.token_urlsafe(32)
        self.sessions[token] = OperatorSession(operator_id, client_id, time.time() + LEASE_SECONDS)
        self.by_operator[operator_id] = token
        r = _redis()
        if r is not None:
            try:
                r.setex(_lease_key(token), LEASE_SECONDS, operator_id)
                r.setex(_token_key(operator_id), LEASE_SECONDS, token)
            except Exception:  # noqa: BLE001 -- a live downlink beats bookkeeping
                pass
        return token

    def resolve(self, token: str) -> Optional[str]:
        """The operator behind this token, from ANY worker.

        Identity only — no queue, because a worker that is not holding the
        downlink cannot deliver events and should not pretend it can. This is
        what the uplink handlers need in order to authenticate a request that
        the balancer happened to send them.
        """
        session = self.get(token)
        if session:
            return session.operator_id
        r = _redis()
        if r is None:
            return None
        try:
            value = r.get(_lease_key(token))
        except Exception:  # noqa: BLE001
            return None
        if not value:
            return None
        return value.decode() if isinstance(value, bytes) else str(value)

    def renew(self, token: str, models=None) -> Optional[str]:
        """Extend the lease from any worker; returns the operator id.

        Renews BOTH the local session (when this is the holding worker) and
        the shared lease, so a heartbeat that lands anywhere keeps the downlink
        alive rather than letting it lapse under a worker that never sees it.
        """
        operator_id = self.resolve(token)
        if not operator_id:
            return None
        local = self.get(token)
        if local:
            local.lease_until = time.time() + LEASE_SECONDS
            if models:
                local.models = set(models)
        r = _redis()
        if r is not None:
            try:
                r.setex(_lease_key(token), LEASE_SECONDS, operator_id)
                r.setex(_token_key(operator_id), LEASE_SECONDS, token)
            except Exception:  # noqa: BLE001
                pass
        return operator_id

    def has_live_lease(self, operator_id: str) -> bool:
        """Is ANY worker currently holding an SSE downlink for this operator?

        `is_current_lease` answers "is THIS token still the lease", which is the
        question a teardown asks about itself. A teardown on the OTHER transport
        has no token to ask about — it needs to know whether the operator is
        still reachable at all before evicting it.

        Without this, `PinClientConnectionManager.disconnect` removed operators
        whose SSE downlink was alive and heartbeating: observed in production as
        `pin:operators:online -> {'members': []}` and a deleted heartbeat key
        while the daemon's every heartbeat answered 200. Same "the corpse must
        not evict the living" defect as the SSE teardown, one transport over.

        Shared lease first so the answer covers every worker, local sessions as
        the fallback when the store is unreachable — the same order `resolve`
        uses, and the safe direction: believing an operator is still held costs
        one stale entry that the heartbeat reaper sweeps in 60s, while wrongly
        believing it is gone costs every request until it reconnects.
        """
        r = _redis()
        if r is not None:
            try:
                if r.get(_token_key(operator_id)):
                    return True
            except Exception:  # noqa: BLE001 -- liveness must never raise
                pass
        return self.get(self.by_operator.get(operator_id, "")) is not None

    def is_current_lease(self, operator_id: str, token: str) -> bool:
        """Is `token` still the operator's live lease?

        Guards the teardown path. A reconnect revokes the previous lease, and
        the old generator's `finally` then ran an unconditional
        `srem(pin:operators:online, operator_id)` — evicting the operator that
        had just reconnected. The corpse removed the living, and the online set
        went empty while the daemon sat there heartbeating.
        """
        r = _redis()
        if r is not None:
            try:
                current = r.get(_token_key(operator_id))
                if current:
                    current = current.decode() if isinstance(current, bytes) else str(current)
                    return current == token
            except Exception:  # noqa: BLE001
                pass
        return self.by_operator.get(operator_id) == token

    def get(self, token: str) -> Optional[OperatorSession]:
        session = self.sessions.get(token)
        if not session or session.lease_until < time.time():
            if session:
                self.by_operator.pop(session.operator_id, None)
                self.sessions.pop(token, None)
            return None
        return session

    def heartbeat(self, token: str, models=None) -> Optional[OperatorSession]:
        session = self.get(token)
        if not session:
            return None
        session.lease_until = time.time() + LEASE_SECONDS
        if models:
            session.models = set(models)
        return session

    def queue(self, operator_id: str, event: dict) -> bool:
        token = self.by_operator.get(operator_id)
        session = self.get(token) if token else None
        if not session:
            return False
        session.events.put_nowait(event)
        return True

    def operator_ids(self) -> list:
        """Operators whose SSE lease this process is holding right now."""
        live = []
        for token, session in list(self.sessions.items()):
            if session.lease_until < time.time():
                continue
            live.append(session.operator_id)
        return live

    def close(self, token: str) -> Optional[OperatorSession]:
        session = self.sessions.pop(token, None)
        if not session:
            return None
        if self.by_operator.get(session.operator_id) == token:
            self.by_operator.pop(session.operator_id, None)
        return session


transport = PinSseTransport()
