"""
RedisOnNedb — the redis-py surface over NEDB's verbatim-shaped docs.

Purpose: the long-tail RedisStorage methods (CRM, pin-ops, training, blogs,
tools, billing…) are compositions of ~30 redis-py primitives. Rather than
hand-transcribing 361 method bodies (each a chance to drift), this shim
implements those primitives over the verbatim doc shape the backfill
preserves — so the ORIGINAL method bodies run unchanged against faithfully
shaped data. NedbStorage inherits RedisStorage with self.r = RedisOnNedb().

This is NOT wrap_redis-the-migration-strategy: every load-bearing domain is
already NATIVE (indexes, engine TTL, atomic tx guards) and overrides these
inherited bodies. The shim serves only the tail, and each triage-doc mini-PR
shrinks it. It is a bridge with a demolition schedule, not an architecture.

Shape contract (matches the backfill's verbatim fallback exactly):
  collection = key's first ':'-segment (or _unrouted)
  _id        = the FULL redis key
  hash  -> {key, rtype: "hash",  fields: {..}}     (all values str)
  string-> {key, rtype: "string", value: str}
  set   -> {key, rtype: "set",   members: [..] sorted}
  zset  -> {key, rtype: "zset",  members: [[member, score], ..]}
  list  -> {key, rtype: "list",  items: [..]}      (index 0 = head)
TTLs ride as engine ttl_s on write (expire/setex) and read as _expires_at.
decode_responses=True parity: everything in and out is str.

Concurrency: every read-modify-write CASes on the doc's _seq and retries —
the same discipline as the native adapter. Cross-worker safe.
"""
from __future__ import annotations

import fnmatch
import json as _json
import secrets as _secrets
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from api.services import nedb_atomic as na
from api.services.nedb_atomic import NedbdClient


def _norm(key_s: str) -> str:
    """Strip the Redis namespace (api/config.py:key() prepends it): inherited
    RedisStorage bodies pass namespaced keys; NEDB docs store unnamespaced."""
    from api.config import REDIS_NAMESPACE
    ns = REDIS_NAMESPACE + ":"
    return key_s[len(ns):] if key_s.startswith(ns) else key_s


def _coll_of(key_s: str) -> str:
    return key_s.split(":", 1)[0] if ":" in key_s else "_unrouted"


# Envelope/system fields. RULE: every "_"-prefixed key is engine metadata
# (_id, _seq, _expires_at, plus DAG-mode _coll/_hash and provenance
# _caused_by/_evidence/...) and must NEVER leak into hash reads — Mark's
# "Corrupt license record" showed _coll/_hash surfacing as data fields.
_META_KEYS = {"key", "rtype", "src", "value", "members", "items", "entry_id"}


def _is_meta(k: str) -> bool:
    return k.startswith("_") or k in _META_KEYS


def _as_hash(d: dict) -> Dict[str, str]:
    """Read any doc as a Redis hash. Verbatim docs expose their `fields`;
    NATIVE docs (fields promoted top-level, no wrapper) expose their
    top-level scalars. decode_responses parity: values stringified, nested
    structures skipped. This is what lets INHERITED RedisStorage bodies read
    natively-migrated collections (users, orgs, licenses_v2, ...) unchanged."""
    if "fields" in d:
        return dict(d.get("fields") or {})
    out: Dict[str, str] = {}
    for k, v in d.items():
        if _is_meta(k) or isinstance(v, (dict, list)):
            continue
        out[k] = v if isinstance(v, str) else (
            "true" if v is True else "false" if v is False else str(v))
    return out


class RedisOnNedb:
    def __init__(self, client: Optional[NedbdClient] = None):
        self.c = client or NedbdClient()

    # ── doc plumbing ─────────────────────────────────────────────────────────

    def _get(self, key_s: str) -> Optional[dict]:
        key_s = _norm(key_s)
        coll = _coll_of(key_s)
        # Primary: verbatim docs are keyed by the FULL redis key.
        d = self.c.get_doc(coll, key_s)
        # Fallback: NATIVE entity docs (users, orgs, seats, messages, ...) are
        # keyed by the BARE id (coll:id shape), not the full key. Let INHERITED
        # READ bodies (e.g. verify_password's hgetall(users:{id})) find them.
        # NOTE: writes to native collections must be handled by native
        # OVERRIDES — a shim write lands a divergent verbatim doc at the full
        # key. Read-safe here; write-safety is a cutover audit item.
        if d is None and ":" in key_s:
            d = self.c.get_doc(coll, key_s.split(":", 1)[1])
        if d is None:
            return None
        exp = d.get("_expires_at")
        if exp is not None and time.time() > float(exp):
            return None  # defensive expiry, same as the native adapter
        return d

    def _put(self, key_s: str, doc: dict, if_seq, ttl_s: Optional[float] = None):
        key_s = _norm(key_s)
        return self._put_doc(_coll_of(key_s), key_s, doc, if_seq, ttl_s)

    def _put_doc(self, coll: str, doc_id: str, doc: dict, if_seq,
                 ttl_s: Optional[float] = None):
        op: Dict[str, Any] = {"op": "put", "coll": coll, "id": doc_id,
                              "doc": doc, "if_seq": if_seq}
        if ttl_s is not None:
            op["ttl_s"] = ttl_s
        return self.c.tx([op], client="aias-shim")

    def _rmw(self, key_s: str, rtype: str, empty: dict, mutate,
             ttl_s: Optional[float] = None):
        """Read-modify-write with CAS retry. `mutate(doc)` edits in place and
        returns the op's return value.

        NATIVE-AWARE WRITES (closes the divergent-partial-doc class Mark hit
        as 'Corrupt license record'): when the target is a NATIVE entity doc
        (found at the bare id, no verbatim envelope), hash mutations are
        applied to its TOP-LEVEL fields and written back AT THE BARE ID —
        never a second doc at the full key, never a fields-subdict hybrid."""
        key_s = _norm(key_s)
        coll = _coll_of(key_s)
        bare = key_s.split(":", 1)[1] if ":" in key_s else key_s
        for attempt in range(na.MAX_RETRIES):
            cur = self.c.get_doc(coll, key_s)
            write_id = key_s
            if cur is None and bare != key_s:
                cur = self.c.get_doc(coll, bare)
                if cur is not None:
                    write_id = bare
            if cur is not None:
                exp = cur.get("_expires_at")
                if exp is not None and time.time() > float(exp):
                    cur = None
                    write_id = key_s
            native = (cur is not None and "rtype" not in cur
                      and "fields" not in cur)
            if cur is None:
                doc = {"key": key_s, "rtype": rtype, **empty, "src": "aias-shim"}
                if_seq = -1
            else:
                doc = {k: v for k, v in cur.items() if k != "_seq"}
                if_seq = cur["_seq"]
            if native and rtype == "hash":
                # present the native doc's data fields as a `fields` view,
                # run the ORIGINAL mutation, fold the result back top-level
                view = {k: (v if isinstance(v, str) else
                            ("true" if v is True else
                             "false" if v is False else str(v)))
                        for k, v in doc.items()
                        if not _is_meta(k) and not isinstance(v, (dict, list))}
                proxy = {"fields": dict(view)}
                rv = mutate(proxy)
                merged = proxy.get("fields") or {}
                for k in view:                       # apply deletions (hdel)
                    if k not in merged and k in doc:
                        del doc[k]
                doc.update(merged)                   # apply sets/increments
            else:
                rv = mutate(doc)
            out = self._put_doc(coll, write_id, doc, if_seq, ttl_s)
            if out.get("_status") == 409 and out.get("error") == "precondition_failed":
                time.sleep(na._backoff(attempt))
                continue
            if out.get("_status", 200) != 200:
                raise RuntimeError(f"nedbd write failed: {out}")
            return rv
        raise na.NedbConflictExhausted(f"shim CAS exhausted on {key_s}")

    # ── strings / KV ─────────────────────────────────────────────────────────

    def get(self, key_s: str) -> Optional[str]:
        d = self._get(key_s)
        if d is not None:
            return d.get("value")
        # Killed derived pointers (slice transforms dropped them in favour of
        # eq indexes) must still resolve for INHERITED bodies that GET them
        # (e.g. verify_password reads users:email:{e}).
        return self._resolve_dead_pointer(_norm(key_s))

    def _resolve_dead_pointer(self, key_s: str) -> Optional[str]:
        parts = key_s.split(":")

        def q1(coll, field, val):
            if '"' in val:
                return None
            rows = self.c.query(f'FROM {coll} WHERE {field} = "{val}"')
            return rows[0].get("_id") if rows else None

        if len(parts) >= 3 and parts[0] == "users" and parts[1] == "email":
            # pointer value was always the NORMALIZED email (both create_user
            # and register_user wrote it normalized) -> users.email_normalized
            email = ":".join(parts[2:])
            hit = q1("users", "email_normalized", email)
            if hit:
                return hit
            # LEGACY: user docs migrated BEFORE the email_normalized field
            # existed (Mark's mainnet run predates the login fix). Fall back
            # to scanning users and comparing normalized emails — correct at
            # any scale that matters, and re-running the idempotent backfill
            # upgrades docs so this path stops being taken.
            for d in self.c.query("FROM users"):
                if (d.get("email") or "").lower().strip() == email:
                    return d.get("_id")
            return None
        if len(parts) >= 3 and parts[0] == "licenses_v2" and parts[1] == "key":
            return q1("licenses_v2", "key", ":".join(parts[2:]))
        if len(parts) >= 3 and parts[0] == "licenses" and parts[1] == "key":
            return q1("licenses", "key", ":".join(parts[2:]))
        if len(parts) == 3 and parts[0] == "users" and parts[2] == "seat":
            rows = self.c.query(f'FROM seats WHERE user_id = "{parts[1]}"')
            claimed = [r for r in rows if r.get("status") == "claimed"]
            pick = claimed or rows
            return pick[0].get("_id") if pick else None
        if len(parts) == 4 and parts[0] == "envs" and parts[1] == "slug":
            if '"' in parts[2] + parts[3]:
                return None
            rows = self.c.query(
                f'FROM envs WHERE license_id = "{parts[2]}" '
                f'AND slug = "{parts[3]}"')
            return rows[0].get("_id") if rows else None
        return None

    def set(self, key_s: str, value, ex: Optional[int] = None, nx: bool = False):
        if not nx:
            return self._rmw(key_s, "string", {"value": ""},
                             lambda doc: doc.__setitem__("value", str(value)) or True,
                             ttl_s=float(ex) if ex else None)
        # nx=True must be a single atomic "claim if absent", not a check then
        # a separate write — two workers can both pass a pre-read check and
        # both believe they won (redis SET NX is one atomic command; this
        # shim's own docstring promises "cross-worker safe" CAS for every
        # read-modify-write, so the pre-read version was violating its own
        # contract). The existence check now runs INSIDE the mutate callback,
        # on the SAME freshly-read doc _rmw is about to CAS-write: if another
        # worker's write lands first, _rmw's if_seq retry re-reads and
        # re-runs this callback against the NEW state, so a loser always
        # correctly sees the winner's value and refuses on a later attempt.
        # IMPORTANT: trust _rmw's OWN return value (whatever mutate returned
        # on the attempt whose CAS write actually committed) — do NOT track
        # "did I decide to claim" via a side-channel across retries. An
        # earlier draft of this fix used a `won=[]` list appended inside the
        # callback; a losing worker's FIRST attempt can optimistically decide
        # to claim before its write loses the race, then correctly refuse on
        # a later retry — but the stale True from the failed first attempt
        # was still sitting in the list, so the loser was reported as a
        # winner anyway. Caught by scripts/test_shim_atomic_nx.py, which
        # forces a real two-thread race rather than trusting a single-path
        # walkthrough.
        def _claim(doc):
            if doc.get("value"):
                return False
            doc["value"] = str(value)
            return True

        return True if self._rmw(
            key_s, "string", {"value": ""}, _claim, ttl_s=float(ex) if ex else None,
        ) else None

    def setex(self, key_s: str, ttl: int, value) -> bool:
        return bool(self.set(key_s, value, ex=int(ttl)))

    def incr(self, key_s: str, amount: int = 1) -> int:
        return self.incrby(key_s, amount)

    def incrby(self, key_s: str, amount: int = 1) -> int:
        box: List[int] = []

        def mut(doc):
            new = int(float(doc.get("value") or 0)) + int(amount)
            doc["value"] = str(new)
            box.append(new)
        self._rmw(key_s, "string", {"value": "0"}, mut)
        return box[-1]  # last attempt = the one that committed (CAS retries)

    def decr(self, key_s: str, amount: int = 1) -> int:
        return self.incrby(key_s, -int(amount))

    def decrby(self, key_s: str, amount: int = 1) -> int:
        return self.incrby(key_s, -int(amount))

    def delete(self, *keys: str) -> int:
        n = 0
        for k in keys:
            if not k:
                continue
            k = _norm(k)
            coll = _coll_of(k)
            did = k
            if self.c.get_doc(coll, did) is None and ":" in k:
                bare = k.split(":", 1)[1]
                if self.c.get_doc(coll, bare) is not None:
                    did = bare
                else:
                    continue
            elif self.c.get_doc(coll, did) is None:
                continue
            self.c.tx([{"op": "del", "coll": coll, "id": did}],
                      client="aias-shim")
            n += 1
        return n

    def exists(self, *keys: str) -> int:
        return sum(1 for k in keys if self._get(k) is not None)

    def expire(self, key_s: str, ttl: int) -> bool:
        cur = self._get(key_s)
        if cur is None:
            return False
        doc = {k: v for k, v in cur.items() if k != "_seq"}
        out = self._put(key_s, doc, cur["_seq"], ttl_s=float(ttl))
        return out.get("_status", 200) == 200

    def pttl(self, key_s: str) -> int:
        d = self._get(key_s)
        if d is None:
            return -2
        exp = d.get("_expires_at")
        if exp is None:
            return -1
        return max(0, int((float(exp) - time.time()) * 1000))

    def ttl(self, key_s: str) -> int:
        p = self.pttl(key_s)
        return p if p < 0 else p // 1000

    # ── hashes ───────────────────────────────────────────────────────────────

    def hget(self, key_s: str, field: str) -> Optional[str]:
        d = self._get(key_s)
        return None if d is None else _as_hash(d).get(str(field))

    def hgetall(self, key_s: str) -> Dict[str, str]:
        d = self._get(key_s)
        return _as_hash(d) if d else {}

    def hkeys(self, key_s: str) -> List[str]:
        d = self._get(key_s)
        return list(_as_hash(d).keys()) if d else []

    def hvals(self, key_s: str) -> List[str]:
        d = self._get(key_s)
        return list(_as_hash(d).values()) if d else []

    def hset(self, key_s: str, field=None, value=None, mapping=None) -> int:
        updates: Dict[str, str] = {}
        if mapping:
            updates.update({str(k): str(v) for k, v in mapping.items()})
        if field is not None:
            updates[str(field)] = str(value)

        def mut(doc):
            f = dict(doc.get("fields") or {})
            added = sum(1 for k in updates if k not in f)
            f.update(updates)
            doc["fields"] = f
            return added
        return self._rmw(key_s, "hash", {"fields": {}}, mut)

    def hdel(self, key_s: str, *fields: str) -> int:
        def mut(doc):
            f = dict(doc.get("fields") or {})
            n = 0
            for fd in fields:
                if str(fd) in f:
                    del f[str(fd)]
                    n += 1
            doc["fields"] = f
            return n
        if self._get(key_s) is None:
            return 0
        return self._rmw(key_s, "hash", {"fields": {}}, mut)

    def hexists(self, key_s: str, field: str) -> bool:
        return self.hget(key_s, field) is not None

    def hsetnx(self, key_s: str, field: str, value) -> int:
        """Create-once field set (KMS TMK/DEK wrap depends on this never
        clobbering). _rmw CASes on _seq (if_seq=-1 when the doc is absent),
        so two racing writers produce exactly one winner."""
        box: List[int] = []

        def mut(doc):
            f = dict(doc.get("fields") or {})
            if str(field) in f:
                box.append(0)
            else:
                f[str(field)] = str(value)
                doc["fields"] = f
                box.append(1)
        self._rmw(key_s, "hash", {"fields": {}}, mut)
        return box[-1]

    def hincrby(self, key_s: str, field: str, amount: int = 1) -> int:
        box: List[int] = []

        def mut(doc):
            f = dict(doc.get("fields") or {})
            new = int(float(f.get(str(field)) or 0)) + int(amount)
            f[str(field)] = str(new)
            doc["fields"] = f
            box.append(new)
        self._rmw(key_s, "hash", {"fields": {}}, mut)
        return box[-1]

    def hincrbyfloat(self, key_s: str, field: str, amount: float) -> float:
        box: List[float] = []

        def mut(doc):
            f = dict(doc.get("fields") or {})
            new = float(f.get(str(field)) or 0.0) + float(amount)
            f[str(field)] = repr(new) if new != int(new) else str(new)
            doc["fields"] = f
            box.append(new)
        self._rmw(key_s, "hash", {"fields": {}}, mut)
        return box[-1]

    # ── sets ─────────────────────────────────────────────────────────────────

    def sadd(self, key_s: str, *members) -> int:
        def mut(doc):
            m = set(doc.get("members") or [])
            n = len({str(x) for x in members} - m)
            m |= {str(x) for x in members}
            doc["members"] = sorted(m)
            return n
        return self._rmw(key_s, "set", {"members": []}, mut)

    def srem(self, key_s: str, *members) -> int:
        if self._get(key_s) is None:
            return 0

        def mut(doc):
            m = set(doc.get("members") or [])
            n = len(m & {str(x) for x in members})
            m -= {str(x) for x in members}
            doc["members"] = sorted(m)
            return n
        return self._rmw(key_s, "set", {"members": []}, mut)

    def smembers(self, key_s: str) -> set:
        d = self._get(key_s)
        if d is not None:
            return set(d.get("members") or [])
        return self._resolve_dead_set(_norm(key_s)) or set()

    def _resolve_dead_set(self, key_s: str) -> Optional[set]:
        """Killed derived sets (slice transforms replaced them with eq
        indexes) resolve for INHERITED bodies that smembers them — e.g.
        get_workspaces_by_client_id reads workspaces:client:{cid} ('No
        conversations yet' was this returning empty)."""
        parts = key_s.split(":")

        def ids(coll, field, val):
            if '"' in val:
                return set()
            return {r["_id"] for r in self.c.query(
                f'FROM {coll} WHERE {field} = "{val}"') if r.get("_id")}

        if parts[0] == "workspaces" and len(parts) >= 2:
            if len(parts) >= 3 and parts[1] == "user":
                return ids("workspaces", "owner_id", ":".join(parts[2:]))
            if len(parts) >= 3 and parts[1] == "org":
                return ids("workspaces", "organization_id", ":".join(parts[2:]))
            if len(parts) >= 3 and parts[1] == "client":
                return ids("workspaces", "client_id", ":".join(parts[2:]))
            if len(parts) == 2 and parts[1] == "active":
                return ids("workspaces", "status", "active")
        if parts[0] == "envs" and len(parts) == 3:
            if parts[2] == "workspaces":
                return ids("workspaces", "environment_id", parts[1])
            if parts[2] == "members":
                return ids("env_members", "environment_id", parts[1])
        if parts[0] == "orgs" and len(parts) == 3 and parts[2] == "members":
            return ids("org_members", "organization_id", parts[1])
        if parts[0] == "users" and len(parts) == 3:
            if parts[2] == "memberships":
                return ids("org_members", "user_id", parts[1])
            if parts[2] == "env_memberships":
                return ids("env_members", "user_id", parts[1])
        if parts[0] == "licenses_v2" and len(parts) >= 3:
            if parts[1] == "status":
                return ids("licenses_v2", "status", ":".join(parts[2:]))
            if parts[1] == "org":
                return ids("licenses_v2", "organization_id", ":".join(parts[2:]))
        if parts[0] == "licenses" and len(parts) >= 3 and parts[1] == "status":
            return ids("licenses", "status", ":".join(parts[2:]))
        if parts[0] == "seats" and len(parts) >= 3 and parts[1] == "email":
            return ids("seats", "invitation_email", ":".join(parts[2:]))
        if key_s == "pin:withdrawals:pending":
            return ids("pin_withdrawals", "status", "pending")
        if key_s == "drafts:pending":
            return {r["_id"] for r in self.c.query("FROM drafts")
                    if r.get("_id") and "rtype" not in r}
        if parts[0] == "drafts" and len(parts) >= 3 and parts[1] == "ws":
            return ids("drafts", "workspace_id", ":".join(parts[2:]))
        return None

    def sismember(self, key_s: str, member) -> bool:
        return str(member) in self.smembers(key_s)   # dead-set aware

    def scard(self, key_s: str) -> int:
        return len(self.smembers(key_s))             # dead-set aware

    def sinter(self, *keys: str) -> set:
        sets = [self.smembers(k) for k in keys]
        return set.intersection(*sets) if sets else set()

    # ── lists (index 0 = head) ───────────────────────────────────────────────

    def lpush(self, key_s: str, *values) -> int:
        def mut(doc):
            items = list(doc.get("items") or [])
            for v in values:
                items.insert(0, str(v))
            doc["items"] = items
            return len(items)
        return self._rmw(key_s, "list", {"items": []}, mut)

    def rpush(self, key_s: str, *values) -> int:
        def mut(doc):
            items = list(doc.get("items") or [])
            items.extend(str(v) for v in values)
            doc["items"] = items
            return len(items)
        return self._rmw(key_s, "list", {"items": []}, mut)

    def lrange(self, key_s: str, start: int, end: int) -> List[str]:
        d = self._get(key_s)
        items = list(d.get("items") or []) if d else []
        if end == -1:
            return items[start:]
        return items[start:end + 1]

    def ltrim(self, key_s: str, start: int, end: int) -> bool:
        def mut(doc):
            items = list(doc.get("items") or [])
            doc["items"] = items[start:] if end == -1 else items[start:end + 1]
            return True
        if self._get(key_s) is None:
            return True
        return self._rmw(key_s, "list", {"items": []}, mut)

    def lpop(self, key_s: str) -> Optional[str]:
        """Pop from the HEAD. redis returns nil on a missing or empty list.

        `pin_operator_bus.await_reply` has always called this: it is how a
        dispatched job's reply is collected. The shim shipped `lpush`/`rpush`/
        `lrange`/`llen` and not this, so on the NEDB backend every cross-worker
        reply sat in its list until the TTL swept it while the caller waited out
        the full timeout and reported "operator disconnected".
        """
        if self._get(key_s) is None:
            return None
        box: List[Optional[str]] = [None]

        def mut(doc):
            items = list(doc.get("items") or [])
            if items:
                box[0] = items.pop(0)
            doc["items"] = items
            return box[0]
        return self._rmw(key_s, "list", {"items": []}, mut)

    def rpop(self, key_s: str) -> Optional[str]:
        """Pop from the TAIL. Included with lpop so the list surface is whole —
        a half-implemented primitive set is what produced the bug above."""
        if self._get(key_s) is None:
            return None
        box: List[Optional[str]] = [None]

        def mut(doc):
            items = list(doc.get("items") or [])
            if items:
                box[0] = items.pop()
            doc["items"] = items
            return box[0]
        return self._rmw(key_s, "list", {"items": []}, mut)

    def llen(self, key_s: str) -> int:
        d = self._get(key_s)
        return len(d.get("items") or []) if d else 0

    def lrem(self, key_s: str, count: int, value) -> int:
        """Full redis semantics: count=0 remove all occurrences, count>0
        remove first N from head, count<0 remove last N from tail."""
        if self._get(key_s) is None:
            return 0
        val = str(value)

        def mut(doc):
            items = list(doc.get("items") or [])
            if count == 0:
                kept = [x for x in items if x != val]
                removed = len(items) - len(kept)
            elif count > 0:
                kept, removed = [], 0
                for x in items:
                    if x == val and removed < count:
                        removed += 1
                    else:
                        kept.append(x)
            else:
                kept_rev, removed = [], 0
                for x in reversed(items):
                    if x == val and removed < -count:
                        removed += 1
                    else:
                        kept_rev.append(x)
                kept = list(reversed(kept_rev))
            doc["items"] = kept
            return removed
        return self._rmw(key_s, "list", {"items": []}, mut)

    # ── sorted sets ──────────────────────────────────────────────────────────

    def zadd(self, key_s: str, mapping: Dict[str, float]) -> int:
        def mut(doc):
            pairs = {m: s for m, s in (doc.get("members") or [])}
            n = sum(1 for m in mapping if str(m) not in pairs)
            for m, s in mapping.items():
                pairs[str(m)] = float(s)
            doc["members"] = sorted(pairs.items(), key=lambda kv: (kv[1], kv[0]))
            doc["members"] = [[m, s] for m, s in doc["members"]]
            return n
        return self._rmw(key_s, "zset", {"members": []}, mut)

    def zrem(self, key_s: str, *members) -> int:
        if self._get(key_s) is None:
            return 0

        def mut(doc):
            pairs = [(m, s) for m, s in (doc.get("members") or [])]
            drop = {str(x) for x in members}
            kept = [[m, s] for m, s in pairs if m not in drop]
            n = len(pairs) - len(kept)
            doc["members"] = kept
            return n
        return self._rmw(key_s, "zset", {"members": []}, mut)

    def _zpairs(self, key_s: str) -> List[Tuple[str, float]]:
        """All zset reads flow through here: a live doc's members, else
        DEAD-ZSET RESOLUTION. Slice 3 killed the messages:ws:{ws} timeline
        zset (ORDER BY created_at replaced it); inherited readers that still
        zrange it get the same answer synthesized from the entity docs, with
        scores derived from created_at — exactly what the writer used to zadd.
        Ascending score order, ties broken by member (redis parity)."""
        d = self._get(key_s)
        if d is not None:
            return [(m, float(s)) for m, s in (d.get("members") or [])]
        return self._dead_zset_pairs(_norm(key_s)) or []

    def _dead_zset_pairs(self, key_s: str) -> Optional[List[Tuple[str, float]]]:
        from datetime import datetime
        parts = key_s.split(":")
        if len(parts) == 3 and parts[0] == "messages" and parts[1] == "ws":
            rows = self.c.query(
                f'FROM messages WHERE workspace_id = "{parts[2]}"')
            pairs: List[Tuple[str, float]] = []
            for r in rows:
                if "rtype" in r or "id" not in r:
                    continue  # verbatim strays are not timeline entities
                try:
                    ts = datetime.fromisoformat(r.get("created_at")).timestamp()
                except (TypeError, ValueError):
                    ts = 0.0
                pairs.append((r["id"], ts))
            pairs.sort(key=lambda p: (p[1], p[0]))
            return pairs
        return None

    def zrange(self, key_s: str, start: int, end: int,
               withscores: bool = False, desc: bool = False):
        pairs = self._zpairs(key_s)
        if desc:
            pairs = list(reversed(pairs))
        sl = pairs[start:] if end == -1 else pairs[start:end + 1]
        return [(m, s) for m, s in sl] if withscores else [m for m, _ in sl]

    def zrevrange(self, key_s: str, start: int, end: int,
                  withscores: bool = False):
        pairs = list(reversed(self._zpairs(key_s)))
        sl = pairs[start:] if end == -1 else pairs[start:end + 1]
        return [(m, s) for m, s in sl] if withscores else [m for m, _ in sl]

    def zscore(self, key_s: str, member) -> Optional[float]:
        for m, s in self._zpairs(key_s):
            if m == str(member):
                return float(s)
        return None

    def zcard(self, key_s: str) -> int:
        return len(self._zpairs(key_s))

    @staticmethod
    def _zbound(v, default: float) -> Tuple[float, bool]:
        """Parse a redis score bound: float, "-inf"/"+inf", or "(x" exclusive.
        Returns (bound, exclusive)."""
        if isinstance(v, str):
            s = v.strip()
            if s in ("-inf", "+inf", "inf"):
                return (float("-inf") if s == "-inf" else float("inf")), False
            if s.startswith("("):
                return float(s[1:]), True
        return float(v if v is not None else default), False

    def zrangebyscore(self, key_s: str, min, max,
                      withscores: bool = False, start: Optional[int] = None,
                      num: Optional[int] = None):
        pairs = self._zpairs(key_s)
        lo, lo_x = self._zbound(min, float("-inf"))
        hi, hi_x = self._zbound(max, float("inf"))
        sel = [(m, s) for m, s in pairs
               if (s > lo if lo_x else s >= lo) and (s < hi if hi_x else s <= hi)]
        if start is not None:
            sel = sel[start:(start + num) if num is not None else None]
        return [(m, s) for m, s in sel] if withscores else [m for m, _ in sel]

    def zremrangebyrank(self, key_s: str, start: int, stop: int) -> int:
        """Trim by rank — ascending score, inclusive both ends, negatives
        counted from the tail (redis parity).

        Missing until now, and web_search.py:330 calls it on every search to
        cap its audit zset at the newest 10000 (`0, -10001`). The AttributeError
        surfaced as `success:false, "Search failed: 'RedisOnNedb' object has no
        attribute 'zremrangebyrank'"` — i.e. platform web search returned a 200
        carrying nothing, for every query, on the nedb backend.

        A synthesized dead zset has no doc to write back to, so the `_get`
        guard makes removal a no-op there rather than materializing one —
        same early return `zrem` uses.
        """
        if self._get(key_s) is None:
            return 0

        def mut(doc):
            pairs = [(m, s) for m, s in (doc.get("members") or [])]
            n = len(pairs)
            if n == 0:
                return 0
            lo = start + n if start < 0 else start
            hi = stop + n if stop < 0 else stop
            lo, hi = max(lo, 0), min(hi, n - 1)
            if lo > hi:
                return 0
            doc["members"] = [[m, s] for i, (m, s) in enumerate(pairs)
                              if i < lo or i > hi]
            return hi - lo + 1
        return self._rmw(key_s, "zset", {"members": []}, mut)

    def zremrangebyscore(self, key_s: str, min, max) -> int:
        """Trim by score window, honouring "(x" exclusive and ±inf bounds.

        Also missing, and also on a live path: redis_storage.record_groq_usage
        and get_groq_usage_count both drop out-of-window entries with this
        before zcard'ing what remains. Without it the free-tier meter raises
        instead of counting.
        """
        if self._get(key_s) is None:
            return 0

        def mut(doc):
            pairs = [(m, s) for m, s in (doc.get("members") or [])]
            lo, lo_x = self._zbound(min, float("-inf"))
            hi, hi_x = self._zbound(max, float("inf"))

            def in_window(s: float) -> bool:
                return ((s > lo if lo_x else s >= lo)
                        and (s < hi if hi_x else s <= hi))
            kept = [[m, s] for m, s in pairs if not in_window(s)]
            n = len(pairs) - len(kept)
            doc["members"] = kept
            return n
        return self._rmw(key_s, "zset", {"members": []}, mut)

    # ── scanning ─────────────────────────────────────────────────────────────

    def _keys_matching(self, pattern: str) -> List[str]:
        """Pattern's first ':'-segment picks the collection (patterns in this
        codebase always share the entity prefix, e.g. "licenses:key:*")."""
        from api.config import REDIS_NAMESPACE
        ns = REDIS_NAMESPACE + ":"
        namespaced = pattern.startswith(ns)
        npat = _norm(pattern)                       # unnamespaced for matching
        prefix = npat.split(":", 1)[0]
        if any(ch in prefix for ch in "*?["):
            raise NotImplementedError(
                "cross-prefix scan patterns are not supported by the shim")
        rows = self.c.query(f"FROM {prefix}")
        out = []
        for r in rows:
            k = r.get("key")
            if not k:
                # NATIVE entity doc: no verbatim envelope, keyed by bare id.
                # Synthesize its redis-form key (coll:id) so scan patterns
                # written for Redis keys (e.g. "users:*") match it — the
                # list_all_users class of inherited bodies depends on this.
                rid = r.get("_id")
                if rid is None:
                    continue
                rid = str(rid)
                k = rid if rid.startswith(prefix + ":") else f"{prefix}:{rid}"
            if not fnmatch.fnmatchcase(k, npat):
                continue
            # Return keys namespaced exactly as real redis-py would (the
            # caller scanned a namespaced pattern -> gets namespaced keys),
            # so inherited bodies that string-manipulate the returned key
            # behave identically to Redis.
            out.append(ns + k if namespaced else k)
        return out

    def scan_iter(self, match: str = "*", count: int = 100) -> Iterator[str]:
        yield from self._keys_matching(match)

    def scan(self, cursor: int = 0, match: str = "*",
             count: int = 100) -> Tuple[int, List[str]]:
        return 0, self._keys_matching(match)

    def keys(self, pattern: str = "*") -> List[str]:
        return self._keys_matching(pattern)

    # ── misc parity ──────────────────────────────────────────────────────────

    def type(self, key_s: str) -> str:
        d = self._get(key_s)
        if d is None:
            return "none"
        rt = d.get("rtype")
        if rt:
            return "stream" if rt == "stream-entry" else rt
        # NATIVE docs carry no envelope: infer the redis-equivalent type so
        # inherited bodies' `type(k) == "hash"` guards pass
        if "value" in d:
            return "string"
        if "members" in d:
            return "set" if d.get("members") and not isinstance(
                d["members"][0], list) else "zset"
        if "items" in d:
            return "list"
        return "hash"

    def ping(self) -> bool:
        # NedbdClient has no health(); hit the daemon's /health directly.
        # Raise on failure (redis-py ping raises too) so callers' except
        # blocks report "error: ..." instead of silently claiming healthy.
        #
        # EMBEDDED MODE has no daemon and therefore no _req:
        # EmbeddedNedbdClient is a get_doc/query/tx drop-in by design, so
        # reaching for a private HTTP helper broke it — /api/health reported
        # `engine: "error: 'EmbeddedNedbdClient' object has no attribute
        # '_req'"`. Prefer a public health() when the client offers one, and
        # keep the HTTP private only as the fallback it always was.
        health = getattr(self.c, "health", None)
        if callable(health):
            out = health()
        elif hasattr(self.c, "_req"):
            out = self.c._req("GET", "/health")
        else:
            raise ConnectionError(
                f"{type(self.c).__name__} exposes neither health() nor _req()")
        if out.get("_status", 200) != 200:
            raise ConnectionError(f"nedbd health check failed: {out}")
        return True

    def xadd(self, key_s: str, fields: Dict[str, str],
             maxlen: Optional[int] = None) -> str:
        """Audit-stream parity: one doc per entry (same shape the backfill
        and the guards' _audit() write). maxlen is ignored — NEDB retains
        history by design; trimming was a Redis memory concession."""
        import uuid as _uuid
        key_s = _norm(key_s)
        eid = f"{int(time.time() * 1000)}-{_uuid.uuid4().hex[:8]}"
        doc = {"key": key_s, "rtype": "stream-entry", "entry_id": eid,
               "fields": {str(k): str(v) for k, v in fields.items()},
               "src": "aias-shim"}
        self.c.tx([{"op": "put", "coll": _coll_of(key_s),
                    "id": f"{key_s}:{eid}", "doc": doc}], client="aias-shim")
        return eid

    def xrange(self, key_s: str, min: str = "-", max: str = "+",
               count: Optional[int] = None) -> List[Tuple[str, Dict[str, str]]]:
        key_s = _norm(key_s)
        rows = self.c.query(f'FROM {_coll_of(key_s)} WHERE key = "{key_s}"')
        entries = sorted(((r["entry_id"], dict(r.get("fields") or {}))
                          for r in rows if r.get("rtype") == "stream-entry"),
                         key=lambda e: e[0])
        return entries[:count] if count else entries

    def xlen(self, key_s: str) -> int:
        key_s = _norm(key_s)
        rows = self.c.query(f'FROM {_coll_of(key_s)} WHERE key = "{key_s}"')
        return sum(1 for r in rows if r.get("rtype") == "stream-entry")

    def register_script(self, script: str):
        def _refuse(*a, **k):
            raise NotImplementedError(
                "Lua scripts don't run on NEDB — the atomic paths are "
                "overridden with guard-backed methods (api/services/"
                "nedb_atomic.py). If you hit this, a legacy non-atomic "
                "caller needs its override.")
        return _refuse

    # ── pub/sub ──────────────────────────────────────────────────────────────
    #
    # THE BUG THIS EXISTS FOR
    #
    #     [PIN BUS] serve loop failed for op_a32a252d-292:
    #         'RedisOnNedb' object has no attribute 'pubsub'
    #
    # `pin_operator_bus` is built on four redis primitives — pubsub(),
    # publish(), pubsub_numsub() and lpop(). This shim implemented NONE of
    # them, so on the NEDB backend the whole cross-worker bus was inert:
    # serve() raised on the first line (the only loud one), dispatch() and
    # holders() had their exceptions swallowed and returned 0, and await_reply
    # returned None. `holders() == 0` is what the chat path reads as "no worker
    # is holding this operator", so it excluded operators that were connected
    # and healthy.
    #
    # It shipped green because tests/test_pin_operator_bus.py drives the bus
    # against a FakeRedis that implements all four. The double was strictly
    # more capable than production, so the suite proved a round trip that
    # could never happen on this backend.
    #
    # HOW PUB/SUB WORKS WITHOUT PUB/SUB
    #
    # Redis fan-out is ephemeral; the store is durable. So a subscriber is
    # modelled explicitly instead of implicitly:
    #
    #     pubsub:subs:{channel} -> SET of subscriber tokens
    #     pubsub:live:{token}   -> string under TTL   (the subscriber's lease)
    #     pubsub:inbox:{token}  -> LIST of messages
    #
    # publish() fans out to each live subscriber's inbox and reaps tokens whose
    # lease has lapsed, so a worker that died mid-subscribe stops being counted
    # without anyone running a cleanup pass. pubsub_numsub() counts live leases,
    # which keeps holders() measured-at-the-moment-of-asking rather than
    # remembered — the property the bus docstring relies on.
    #
    # HONEST COST: a publish is one store write per live subscriber. Dispatch
    # and reply are low-volume and this is free in practice. The STREAM leg
    # publishes per token, so a cross-worker stream becomes one write per token
    # per consumer. That is real, and it is still strictly better than a bus
    # that silently refuses every operator. A single-worker gateway never
    # touches this path at all, because the local branch handles it.

    _PUBSUB_NS = "aiconsult:pubsub:"
    _PUBSUB_LEASE_SECONDS = 60

    def _ps_subs_key(self, channel: str) -> str:
        return f"{self._PUBSUB_NS}subs:{channel}"

    def _ps_live_key(self, token: str) -> str:
        return f"{self._PUBSUB_NS}live:{token}"

    def _ps_inbox_key(self, token: str) -> str:
        return f"{self._PUBSUB_NS}inbox:{token}"

    def _ps_live_tokens(self, channel: str) -> List[str]:
        """Subscriber tokens whose lease is still valid, reaping the rest."""
        live = []
        for token in sorted(self.smembers(self._ps_subs_key(channel))):
            if self.get(self._ps_live_key(token)) is None:
                # Lease lapsed: the worker is gone. Reap so it stops being
                # counted as a holder and its inbox stops accumulating.
                self.srem(self._ps_subs_key(channel), token)
                self.delete(self._ps_inbox_key(token))
                continue
            live.append(token)
        return live

    def publish(self, channel: str, message) -> int:
        """Deliver to every live subscriber. Returns the number delivered to,
        which is what `dispatch()` reports as "workers holding this operator"."""
        payload = message if isinstance(message, str) else str(message)
        delivered = 0
        for token in self._ps_live_tokens(channel):
            self.rpush(self._ps_inbox_key(token),
                       _json.dumps({"type": "message", "channel": channel,
                                    "data": payload}))
            delivered += 1
        return delivered

    def pubsub_numsub(self, *channels: str) -> List[Tuple[str, int]]:
        return [(ch, len(self._ps_live_tokens(ch))) for ch in channels]

    def pubsub(self, ignore_subscribe_messages: bool = False) -> "_NedbPubSub":
        return _NedbPubSub(self, ignore_subscribe_messages)

    def pipeline(self, transaction: bool = True):
        """Minimal pipeline: queue redis-py calls, replay sequentially on
        execute(). NOTE: redis-py pipelines ARE MULTI/EXEC-transactional by
        default; the shim replays op-by-op instead, so multi-key batches are
        not atomic as a unit. Every op still CASes individually (_rmw), so
        there are no lost updates — and the callers (webhooks, subscription
        sweeps, usage counters) are idempotent batch writers that self-heal
        on the next pass. `transaction` is accepted for signature parity
        (kms_service passes transaction=False) and ignored."""
        shim = self

        class _Pipe:
            def __init__(self):
                self._ops: List[Tuple[str, tuple, dict]] = []

            def __getattr__(self, name):
                def q(*a, **k):
                    self._ops.append((name, a, k))
                    return self
                return q

            def execute(self):
                return [getattr(shim, n)(*a, **k) for n, a, k in self._ops]

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False
        return _Pipe()


class _NedbPubSub:
    """A redis-py PubSub, modelled explicitly over the store.

    Only the surface `pin_operator_bus` actually uses is implemented —
    subscribe / get_message / unsubscribe / close / __enter__ / __exit__ —
    because a primitive that exists but misbehaves is worse than one that is
    absent: the absent one raises where you can see it, which is how the bug
    this fixes was finally found.

    THE LEASE IS THE SUBSCRIPTION. Redis knows a subscriber died because the
    socket closed; nothing here has a socket. So each subscriber holds a TTL
    lease it refreshes while it polls, and `publish` reaps any lease that has
    lapsed. A worker killed with -9 stops counting as a holder within
    `_PUBSUB_LEASE_SECONDS` without any cleanup pass — which matters, because
    `holders()` deciding an operator is reachable when it is not turns a fast
    "no operator" into a full inference timeout.
    """

    # Refreshing on every poll would be one store write per 100 ms per
    # subscriber. The lease only has to be refreshed comfortably inside its
    # own window.
    _REFRESH_EVERY_SECONDS = 15.0

    def __init__(self, shim: "RedisOnNedb", ignore_subscribe_messages: bool = False):
        self._shim = shim
        self._ignore_subscribe = ignore_subscribe_messages
        self._channels: Dict[str, str] = {}   # channel -> token
        self._last_refresh = 0.0
        self._closed = False

    def subscribe(self, *channels: str) -> None:
        for channel in channels:
            if channel in self._channels:
                continue
            token = _secrets.token_urlsafe(16)
            self._channels[channel] = token
            # Lease FIRST, then membership: a publisher that reads the set
            # between the two writes must never see a token with no lease, or
            # it would reap a subscriber that is in the middle of arriving.
            self._shim.setex(self._shim._ps_live_key(token),
                             self._shim._PUBSUB_LEASE_SECONDS, "1")
            self._shim.sadd(self._shim._ps_subs_key(channel), token)
        self._last_refresh = time.time()

    def _refresh_leases(self) -> None:
        now = time.time()
        if now - self._last_refresh < self._REFRESH_EVERY_SECONDS:
            return
        for token in self._channels.values():
            self._shim.setex(self._shim._ps_live_key(token),
                             self._shim._PUBSUB_LEASE_SECONDS, "1")
        self._last_refresh = now

    def get_message(self, timeout: float = 0, ignore_subscribe_messages: bool = False):
        """One pending message, or None. Never blocks.

        `timeout` is accepted for signature parity and ignored: the caller
        (`pin_operator_bus.serve`) already polls with `timeout=0` and does its
        own `await asyncio.sleep(POLL_INTERVAL_SECONDS)`. Honouring it with a
        synchronous sleep would stall the worker's event loop for the whole
        window — the exact failure the bus docstring rejects BLPOP for.
        """
        if self._closed:
            return None
        self._refresh_leases()
        for channel, token in self._channels.items():
            raw = self._shim.lpop(self._shim._ps_inbox_key(token))
            if raw is None:
                continue
            try:
                return _json.loads(raw)
            except (TypeError, ValueError):
                # A message we cannot parse is still a message consumed; the
                # alternative is an inbox permanently wedged behind one bad row.
                return {"type": "message", "channel": channel, "data": raw}
        return None

    def unsubscribe(self, *channels: str) -> None:
        targets = list(channels) if channels else list(self._channels)
        for channel in targets:
            token = self._channels.pop(channel, None)
            if not token:
                continue
            self._shim.srem(self._shim._ps_subs_key(channel), token)
            self._shim.delete(self._shim._ps_live_key(token),
                              self._shim._ps_inbox_key(token))

    def close(self) -> None:
        # Idempotent: `serve()` closes in a finally that can run after an
        # exception already unwound part of the way.
        if self._closed:
            return
        self.unsubscribe()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
