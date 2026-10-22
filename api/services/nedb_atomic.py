"""
nedb_atomic — Lua-parity atomic guards for the AiAS→NEDB migration (slice 1).

Replaces the Redis Lua scripts with typed, testable Python guards over NEDB's
atomic transaction primitive (nedb-engine >= 2.6.2: POST /batch with per-op
`if_seq` compare-and-set preconditions, all-or-nothing).

The concurrency model, plainly:
  Redis Lua was atomic because Redis executes scripts on its single thread.
  NEDB tx is atomic because the daemon's Sequencer applies the whole
  transaction as ONE committer-thread intent. Same serialization property.
  Cross-worker safety (uvicorn --workers N) comes from `if_seq`: every doc we
  read-modify-write carries the version we read; if another worker touched it
  first, the tx 409s and we re-read + re-validate + retry. Exactly one winner
  per race — proven by the double-claim test in scripts/test_nedb_slice1.py.

Semantics contract (mirrored line-by-line from the Lua bodies, 2026-07-11):
  - Guard failures raise ValueError with the EXACT Lua error message, so the
    RedisStorage callers port mechanically.
  - Field updates are merge-then-put (Redis HSET merges; NEDB put replaces —
    we read the full doc, merge, and CAS so nothing concurrent is clobbered).
  - Derived state (status sets, key->id pointers, email indexes, user->seat
    pointers) is NOT maintained: it died in the slice-1 transform, replaced
    by engine eq indexes. Invite tokens live in `invite_tokens` with engine
    TTL (SETEX parity).
  - Every tx writes its audit entry IN the same transaction, `caused_by`-free
    for now but attributable via client="aias-atomic" in the op log.

Slice 1 guards: activate_license, invite_seat, claim_seat, revoke_seat.
Slice 2 will add: pin_job_complete, pin_withdraw, pin_add_credits.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Optional

import httpx

INVITE_TOKEN_TTL = 7 * 24 * 60 * 60  # mirror redis_storage.py:76

MAX_RETRIES = 16         # CAS retry budget before we give up loudly
RETRY_BACKOFF_S = 0.01   # base backoff; grows 1.5x per attempt, capped 0.2s

# Shared, pooled HTTP client for ALL nedbd calls. Thread-safe (httpx.Client is
# safe for concurrent use), reused across every NedbdClient instance AND across
# FastAPI's threadpool once hot routes run sync. Replaces per-call
# urllib.urlopen, which opened a fresh TCP(+TLS) connection every single call —
# under an N+1 that was dozens of handshakes per request, and blocked the event
# loop when called from async handlers. Keep-alive pooling collapses those into
# a handful of reused connections. Timeout preserved at 30s.
_HTTP = httpx.Client(
    timeout=httpx.Timeout(30.0),
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
)


def _backoff(attempt: int) -> float:
    return min(RETRY_BACKOFF_S * (1.5 ** attempt), 0.2)


class NedbConflictExhausted(RuntimeError):
    """A guard lost the CAS race MAX_RETRIES times in a row. At AiAS write
    volumes this indicates a stuck loop or a hot-doc pathology, not load."""


class NedbdClient:
    """Minimal nedbd HTTP client — the same surface the backfill exercises.

    The forthcoming NedbStorage adapter will absorb/share this class; guards
    only need get/query/tx.
    """

    def __init__(self, base: Optional[str] = None, token: Optional[str] = None,
                 db: Optional[str] = None):
        self.base = (base or os.getenv("NEDBD_URL", "http://localhost:7070")).rstrip("/")
        self.token = token or os.getenv("NEDBD_TOKEN")
        self.db = db or os.getenv("NEDB_DB", "aias")

    def _req(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        # Contract preserved exactly: 2xx -> parsed JSON body; >=400 -> parsed
        # error dict with "_status" set (callers key on _status == 409 for CAS
        # retries); transport failure (connect/timeout) raises, same as before
        # (httpx.HTTPError instead of urllib.error.URLError — no caller in the
        # storage path catches the urllib type; verified).
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        resp = _HTTP.request(
            method,
            f"{self.base}{path}",
            content=json.dumps(body).encode() if body is not None else None,
            headers=headers)
        if resp.status_code >= 400:
            try:
                parsed = resp.json()
                if not isinstance(parsed, dict):
                    parsed = {"error": str(parsed)[:300]}
            except Exception:
                parsed = {"error": resp.text[:300]}
            parsed["_status"] = resp.status_code
            return parsed
        return resp.json()

    def get_doc(self, coll: str, doc_id: str) -> Optional[dict]:
        out = self._req("POST", f"/v1/databases/{self.db}/query",
                        {"nql": f'FROM {coll} WHERE _id = "{doc_id}"'})
        rows = out.get("rows") or []
        return rows[0] if rows else None

    def query(self, nql: str) -> list[dict]:
        return self._req("POST", f"/v1/databases/{self.db}/query",
                         {"nql": nql}).get("rows", [])

    def tx(self, ops: list[dict], client: str = "aias-atomic") -> dict:
        """Atomic all-or-nothing transaction. Returns the response dict;
        callers check `_status == 409` + `error == "precondition_failed"`
        for CAS misses (retry) vs other errors (raise)."""
        return self._req("POST", f"/v1/databases/{self.db}/batch",
                         {"ops": ops, "client": client})


# ---------------------------------------------------------------------------
# tx op builders — tiny helpers so guards read like the Lua they replace
# ---------------------------------------------------------------------------

def _merge_put(coll: str, doc: dict, updates: dict) -> dict:
    """HSET parity: read doc + merge fields + CAS on the version we read."""
    merged = {k: v for k, v in doc.items() if k not in ("_seq",)}
    merged.update(updates)
    return {"op": "put", "coll": coll, "id": doc["_id"], "doc": merged,
            "if_seq": doc["_seq"]}


def _create(coll: str, doc_id: str, doc: dict,
            ttl_s: Optional[float] = None) -> dict:
    op: dict[str, Any] = {"op": "put", "coll": coll, "id": doc_id, "doc": doc}
    if ttl_s is not None:
        op["ttl_s"] = ttl_s
    return op


def _delete(coll: str, doc_id: str) -> dict:
    return {"op": "del", "coll": coll, "id": doc_id}


def _audit(event: str, data: dict) -> dict:
    """XADD parity: the audit entry rides INSIDE the same atomic tx."""
    eid = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    doc = {"key": "audit:stream", "rtype": "stream-entry", "entry_id": eid,
           "fields": {"event": event,
                      "timestamp": data.get("timestamp", ""),
                      **{k: str(v) for k, v in data.items()}},
           "src": "nedb-atomic"}
    return {"op": "put", "coll": "audit", "id": f"audit:stream:{eid}",
            "doc": doc}


def _run_guarded(client: NedbdClient, build_attempt) -> Any:
    """The retry loop every guard shares: read -> validate -> tx(if_seq) ->
    on CAS miss re-read and re-validate. Guard ValueErrors propagate
    immediately (a failed business rule is an answer, not a retry)."""
    for attempt in range(MAX_RETRIES):
        ops, result = build_attempt()
        out = client.tx(ops)
        if out.get("_status") == 409 and out.get("error") == "precondition_failed":
            time.sleep(_backoff(attempt))
            continue
        if out.get("_status", 200) != 200:
            raise RuntimeError(f"nedbd tx failed: {out}")
        return result
    raise NedbConflictExhausted(
        f"CAS retries exhausted after {MAX_RETRIES} attempts")


# ---------------------------------------------------------------------------
# The four slice-1 guards — each mirrors its Lua script line-by-line.
# ---------------------------------------------------------------------------

def activate_license(client: NedbdClient, license_id: str, user_id: str,
                     org_id: str, now: str, expires_at: str,
                     plan_code: str, seat_count: int) -> str:
    """LUA_ACTIVATE_LICENSE parity. Returns license_id.
    Raises ValueError('License is not available for activation')."""
    def attempt():
        lic = client.get_doc("licenses_v2", license_id)
        org = client.get_doc("orgs", org_id)
        usr = client.get_doc("users", user_id)  # native since slice 4
        if lic is None:
            raise ValueError("License is not available for activation")
        # -- the guard (Lua: HGET status ~= 'available') --------------------
        if lic.get("status") != "available":
            raise ValueError("License is not available for activation")
        ops = [
            _merge_put("licenses_v2", lic, {
                "status": "active", "organization_id": org_id,
                "activated_by": user_id, "activated_at": now,
                "expires_at": expires_at, "updated_at": now}),
        ]
        # (Lua SREM avail_set / SADD active_set / SADD org_licenses: derived
        #  state — replaced by the licenses_v2.status/.organization_id indexes)
        if org is not None:
            ops.append(_merge_put("orgs", org, {
                "active_license_id": license_id, "plan_code": plan_code,
                "seats_total": seat_count, "updated_at": now}))
        if usr is not None:
            ops.append(_merge_put("users", usr, {
                "plan": plan_code, "organization_id": org_id}))
        ops.append(_audit("license_activated", {
            "license_id": license_id, "user_id": user_id, "org_id": org_id,
            "timestamp": now}))
        return ops, license_id
    return _run_guarded(client, attempt)


def invite_seat(client: NedbdClient, seat_id: str, email: str, token: str,
                invited_by: str, now: str,
                ttl: int = INVITE_TOKEN_TTL) -> str:
    """LUA_INVITE_SEAT parity. Returns seat_id.
    Raises ValueError('Seat is not available for invitation')."""
    def attempt():
        seat = client.get_doc("seats", seat_id)
        if seat is None or seat.get("status") != "available":
            raise ValueError("Seat is not available for invitation")
        ops = [
            _merge_put("seats", seat, {
                "invitation_email": email, "invitation_token": token,
                "invited_by": invited_by, "status": "invited",
                "invited_at": now, "updated_at": now}),
            # Lua SETEX token -> engine-TTL doc (nedb-engine >= 2.6.3)
            _create("invite_tokens", token, {"seat_id": seat_id},
                    ttl_s=float(ttl)),
            # (Lua SADD email_set: derived — seats.invitation_email index)
            _audit("seat_invited", {
                "seat_id": seat_id, "email": email, "invited_by": invited_by,
                "org_id": seat.get("organization_id", ""), "timestamp": now}),
        ]
        return ops, seat_id
    return _run_guarded(client, attempt)


def claim_seat(client: NedbdClient, seat_id: str, token: str, user_id: str,
               now: str) -> tuple[str, str, str, str]:
    """LUA_CLAIM_SEAT parity. Returns (seat_id, org_id, invited_by, plan_code)
    — the same extras the Lua returned for _create_org_member.
    Raises ValueError('Seat is not available for claiming')."""
    def attempt():
        seat = client.get_doc("seats", seat_id)
        if seat is None or seat.get("status") != "invited":
            raise ValueError("Seat is not available for claiming")
        org_id = seat.get("organization_id", "")
        org = client.get_doc("orgs", org_id)
        usr = client.get_doc("users", user_id)  # native since slice 4
        plan_code = (org or {}).get("plan_code", "")
        invited_by = seat.get("invited_by", "")
        ops = [
            _merge_put("seats", seat, {
                "user_id": user_id, "status": "claimed",
                "claimed_at": now, "updated_at": now}),
            _delete("invite_tokens", token),          # Lua DEL token
            # (Lua SET user->seat pointer: derived — seats.user_id index)
        ]
        if org is not None:
            ops.append(_merge_put("orgs", org, {     # Lua HINCRBY +1
                "seats_allocated": int(org.get("seats_allocated") or 0) + 1,
                "updated_at": now}))
        if usr is not None:
            ops.append(_merge_put("users", usr, {
                "plan": plan_code, "organization_id": org_id}))
        ops.append(_audit("seat_claimed", {
            "seat_id": seat_id, "user_id": user_id, "org_id": org_id,
            "timestamp": now}))
        return ops, (seat_id, org_id, invited_by, plan_code)
    return _run_guarded(client, attempt)


def revoke_seat(client: NedbdClient, seat_id: str, now: str,
                revoked_by: str = "system") -> str:
    """LUA_REVOKE_SEAT parity (inverted guard: fails only when ALREADY
    revoked; conditional user/org cleanup when a user was assigned).
    Returns seat_id. Raises ValueError('Seat is already revoked')."""
    def attempt():
        seat = client.get_doc("seats", seat_id)
        if seat is None:
            raise ValueError("Seat is already revoked")
        if seat.get("status") == "revoked":
            raise ValueError("Seat is already revoked")
        user_id = seat.get("user_id") or ""
        org_id = seat.get("organization_id", "")
        ops = [_merge_put("seats", seat, {
            "status": "revoked", "revoked_at": now, "updated_at": now})]
        if user_id:                                    # Lua: if user assigned
            usr = client.get_doc("users", user_id)  # native since slice 4
            org = client.get_doc("orgs", org_id)
            if usr is not None:
                ops.append(_merge_put("users", usr, {
                    "plan": "free", "organization_id": ""}))
            if org is not None:                        # Lua HINCRBY -1
                ops.append(_merge_put("orgs", org, {
                    "seats_allocated": int(org.get("seats_allocated") or 0) - 1,
                    "updated_at": now}))
        if seat.get("invitation_token"):               # Lua DEL token if set
            ops.append(_delete("invite_tokens", seat["invitation_token"]))
        ops.append(_audit("seat_revoked", {
            "seat_id": seat_id, "user_id": user_id, "org_id": org_id,
            "revoked_by": revoked_by, "timestamp": now}))
        return ops, seat_id
    return _run_guarded(client, attempt)


# ---------------------------------------------------------------------------
# Slice 2 — the three PIN financial guards.
#
# Contract note: unlike slice 1 (ValueError parity), the PIN callers consume
# RESULT DICTS — {'success': False, 'error': 'INSUFFICIENT_BALANCE', ...} —
# so these guards return the exact dict shapes pin_complete_job_atomic /
# pin_create_withdrawal_atomic already map to. pin_add_credits returns the
# new balance (float), raising only on unexpected transport failure.
#
# Billing docs auto-create with Lua parity: HINCRBYFLOAT materializes a hash
# on first touch, so a missing billing doc reads as zero balances. Creation
# is race-safe via if_seq=-1 (create-once): two workers materializing the
# same billing doc conflict and one retries against the created doc.
# ---------------------------------------------------------------------------

_ZERO_USER_BILLING = {"credits_balance": 0.0, "credits_spent": 0.0,
                      "credits_purchased": 0.0, "total_spent": 0.0}
_ZERO_OPERATOR_BILLING = {"earnings_balance": 0.0, "earnings_total": 0.0,
                          "pending_withdrawals": 0.0, "jobs_completed": 0,
                          "tokens_processed": 0}
_ZERO_PROTOCOL_BILLING = {"total_fees": 0.0, "total_jobs": 0,
                          "total_tokens": 0}


def _billing_ops(coll: str, doc: Optional[dict], doc_id: str, zero: dict,
                 deltas: dict) -> dict:
    """One billing update op: CAS merge when the doc exists, create-once
    (if_seq=-1) seeded from zeros when it doesn't. `deltas` are ADDED to
    current values — HINCRBYFLOAT/HINCRBY parity."""
    base = dict(zero)
    if doc is not None:
        base.update({k: v for k, v in doc.items() if k not in ("_seq",)})
    merged = dict(base)
    for f, d in deltas.items():
        cur = base.get(f, zero.get(f, 0))
        try:
            cur = float(cur)
        except (TypeError, ValueError):
            cur = 0.0
        val = cur + d
        # ints stay ints (jobs_completed, tokens_processed, total_jobs...)
        if isinstance(zero.get(f), int) and not isinstance(zero.get(f), bool):
            val = int(val)
        merged[f] = val
    merged.setdefault("_id", doc_id)
    op = {"op": "put", "coll": coll, "id": doc_id, "doc": merged}
    op["if_seq"] = doc["_seq"] if doc is not None else -1
    return op


def pin_job_complete(client: NedbdClient, tx_id: str, job_id: str,
                     user_id: str, operator_id: str, total_cost: float,
                     operator_share: float, protocol_fee: float,
                     tokens: int, model: str, now: str) -> dict:
    """LUA_PIN_JOB_COMPLETE parity. Atomically: guard user credits ->
    settle user/operator/protocol billing -> record transaction -> audit.
    Returns {'success': True, 'tx_id', 'new_balance'} or
    {'success': False, 'error': 'INSUFFICIENT_CREDITS', 'balance', 'required'}.
    (Capped tx lists died in slice 2 transform: history is
    FROM pin_transactions WHERE user_id = ... ORDER BY timestamp DESC.)"""
    def attempt():
        usr = client.get_doc("pin_billing_users", user_id)
        opr = client.get_doc("pin_billing_operators", operator_id)
        pro = client.get_doc("pin_billing_protocol", "protocol")
        balance = float((usr or {}).get("credits_balance") or 0.0)
        # -- the guard (Lua: user_balance < total_cost) ----------------------
        if balance < total_cost:
            raise _GuardResult({"success": False,
                                "error": "INSUFFICIENT_CREDITS",
                                "balance": balance, "required": total_cost})
        ops = [
            _billing_ops("pin_billing_users", usr, user_id,
                         _ZERO_USER_BILLING,
                         {"credits_balance": -total_cost,
                          "credits_spent": total_cost}),
            _billing_ops("pin_billing_operators", opr, operator_id,
                         _ZERO_OPERATOR_BILLING,
                         {"earnings_balance": operator_share,
                          "earnings_total": operator_share,
                          "jobs_completed": 1,
                          "tokens_processed": tokens}),
            _billing_ops("pin_billing_protocol", pro, "protocol",
                         _ZERO_PROTOCOL_BILLING,
                         {"total_fees": protocol_fee,
                          "total_jobs": 1,
                          "total_tokens": tokens}),
            _create("pin_transactions", tx_id, {
                "id": tx_id, "job_id": job_id, "user_id": user_id,
                "operator_id": operator_id, "amount": total_cost,
                "operator_share": operator_share,
                "protocol_fee": protocol_fee, "tokens": tokens,
                "model": model, "timestamp": now, "status": "completed"}),
            _audit("pin_job_completed", {
                "tx_id": tx_id, "job_id": job_id, "user_id": user_id,
                "operator_id": operator_id, "amount": total_cost,
                "tokens": tokens, "timestamp": now}),
        ]
        return ops, {"success": True, "tx_id": tx_id,
                     "new_balance": balance - total_cost}
    return _run_money_guarded(client, attempt)


def pin_withdraw(client: NedbdClient, wd_id: str, operator_id: str,
                 amount: float, min_withdrawal: float, bsc_address: str,
                 now: str) -> dict:
    """LUA_PIN_WITHDRAW parity. Guard order preserved: INSUFFICIENT_BALANCE
    first, then BELOW_MINIMUM. Returns the caller's exact dict shapes.
    (pending set + operator list died: FROM pin_withdrawals WHERE
    status = "pending" / WHERE operator_id = ...)"""
    def attempt():
        opr = client.get_doc("pin_billing_operators", operator_id)
        balance = float((opr or {}).get("earnings_balance") or 0.0)
        if balance < amount:
            raise _GuardResult({"success": False,
                                "error": "INSUFFICIENT_BALANCE",
                                "balance": balance, "requested": amount})
        if amount < min_withdrawal:
            raise _GuardResult({"success": False, "error": "BELOW_MINIMUM",
                                "minimum": min_withdrawal,
                                "requested": amount})
        ops = [
            _billing_ops("pin_billing_operators", opr, operator_id,
                         _ZERO_OPERATOR_BILLING,
                         {"earnings_balance": -amount,
                          "pending_withdrawals": amount}),
            _create("pin_withdrawals", wd_id, {
                "id": wd_id, "operator_id": operator_id, "amount": amount,
                "bsc_address": bsc_address, "status": "pending",
                "created_at": now}),
            _audit("pin_withdrawal_requested", {
                "withdrawal_id": wd_id, "operator_id": operator_id,
                "amount": amount, "timestamp": now}),
        ]
        return ops, {"success": True, "withdrawal_id": wd_id,
                     "new_balance": balance - amount}
    return _run_money_guarded(client, attempt)


def pin_add_credits(client: NedbdClient, user_id: str, amount: float,
                    source: str, now: str) -> float:
    """LUA_PIN_ADD_CREDITS parity (no guard — pure settlement).
    Returns the new balance (float), exactly like the Lua's HINCRBYFLOAT."""
    def attempt():
        usr = client.get_doc("pin_billing_users", user_id)
        balance = float((usr or {}).get("credits_balance") or 0.0)
        new_balance = balance + amount
        ops = [
            _billing_ops("pin_billing_users", usr, user_id,
                         _ZERO_USER_BILLING,
                         {"credits_balance": amount,
                          "credits_purchased": amount}),
            _audit("pin_credits_added", {
                "user_id": user_id, "amount": amount, "source": source,
                "new_balance": new_balance, "timestamp": now}),
        ]
        return ops, new_balance
    return _run_money_guarded(client, attempt)


class _GuardResult(Exception):
    """Business-rule outcome carried as control flow: the PIN callers expect
    result DICTS for guard failures (not exceptions), so guards raise this
    internally and _run_money_guarded returns .result to the caller."""

    def __init__(self, result: dict):
        self.result = result
        super().__init__(str(result))


def _run_money_guarded(client: NedbdClient, build_attempt) -> Any:
    """Same CAS retry loop as _run_guarded, but guard failures return their
    dict (PIN contract) instead of raising."""
    for attempt in range(MAX_RETRIES):
        try:
            ops, result = build_attempt()
        except _GuardResult as g:
            return g.result
        out = client.tx(ops)
        if out.get("_status") == 409 and out.get("error") == "precondition_failed":
            time.sleep(_backoff(attempt))
            continue
        if out.get("_status", 200) != 200:
            raise RuntimeError(f"nedbd tx failed: {out}")
        return result
    raise NedbConflictExhausted(
        f"CAS retries exhausted after {MAX_RETRIES} attempts")
