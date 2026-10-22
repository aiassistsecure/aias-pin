import os
import redis
from typing import Optional

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/12")
REDIS_NAMESPACE = os.getenv("REDIS_NAMESPACE", "aiconsult")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-secret-change-in-production")
SESSION_EXPIRE_SECONDS = 60 * 60 * 24 * 7  # 7 days

_redis_client: Optional[redis.Redis] = None

def get_redis():
    """The client seam. Honors AIAS_STORAGE_BACKEND exactly like the storage
    singleton (bottom of redis_storage.py): "redis" (default) returns the
    real redis-py client; "nedb" returns RedisOnNedb, so every direct caller
    (routes/services/workers that bypass `storage`) rides the same backend —
    no split brain on cutover. Lazy import avoids a config<->shim cycle."""
    global _redis_client
    if _redis_client is None:
        if os.getenv("AIAS_STORAGE_BACKEND", "redis").strip().lower() == "nedb":
            from api.services.nedb_redis_shim import RedisOnNedb
            # RIDE THE STORAGE SINGLETON'S TRANSPORT (2026-07-21, the split-
            # store incident): a bare RedisOnNedb() defaults to NedbdClient()
            # — the HTTP daemon client — even when NEDB_MODE=embedded. For a
            # day, every get_redis() write (subscriptions, counters, audit
            # streams, seats) flowed to the daemon's database while native
            # writes and ALL reads used the local embedded dir: production
            # state split across two machines, and a reconcile "extended"
            # a subscription nobody could read. Binding to storage.nedb
            # makes the shim share the one engine handle — same store, same
            # LOCK, read-your-write by construction.
            from api.services.redis_storage import storage as _storage
            _redis_client = RedisOnNedb(_storage.nedb)
        else:
            _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client

def key(suffix: str) -> str:
    return f"{REDIS_NAMESPACE}:{suffix}"
