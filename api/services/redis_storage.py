"""PIN storage — extracted from api/services/redis_storage.py (10,723 lines).

Source: aiassistsecure/aias @ march_2026 (58a16643993188898ca8b51a29a9ffa771e64755).
Contains, verbatim: the three PIN Lua scripts (atomic job completion,
withdrawal, credit grant), the RedisStorage class's PIN section
(operators, nodes, reputation, billing, job lifecycle, model index),
and the three auth lookups the PIN routes call (get_user,
get_session_user, get_api_key_by_secret). Everything else in the
original class — users, workspaces, billing, licensing — is a different
subsystem and is not part of this slice.

Documented packaging edits (see EXTRACTION_MAP.md): the import block is
narrowed to the names below; _register_lua_scripts registers only the
PIN scripts; the module-level `storage` singleton is created directly
instead of selecting NedbStorage when AIAS_STORAGE_BACKEND=nedb
(the NEDB bridge is a separate distillation).
"""
import hashlib
import json
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from api.config import SESSION_EXPIRE_SECONDS, get_redis, key
from api.models.schemas import (
    OperatorRegion, OperatorStatus, PinBillingOperator, PinBillingUser,
    PinJob, PinJobStatus, PinNetworkStatus, PinOperator, PinReputation,
    PinTransaction, PinWithdrawal, PlanType, User, UserRole,
)


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def gen_id() -> str:
    return str(uuid.uuid4())


LUA_PIN_JOB_COMPLETE = """
local user_billing_key = KEYS[1]
local operator_billing_key = KEYS[2]
local protocol_billing_key = KEYS[3]
local tx_hash_key = KEYS[4]
local user_tx_list = KEYS[5]
local operator_tx_list = KEYS[6]
local audit_stream = KEYS[7]

local tx_id = ARGV[1]
local job_id = ARGV[2]
local user_id = ARGV[3]
local operator_id = ARGV[4]
local total_cost = tonumber(ARGV[5])
local operator_share = tonumber(ARGV[6])
local protocol_fee = tonumber(ARGV[7])
local tokens = ARGV[8]
local model = ARGV[9]
local now = ARGV[10]

-- Check user has sufficient credits
local user_balance = tonumber(redis.call('HGET', user_billing_key, 'credits_balance') or '0')
if user_balance < total_cost then
    return {'error', 'INSUFFICIENT_CREDITS', tostring(user_balance), tostring(total_cost)}
end

-- Deduct user credits atomically
redis.call('HINCRBYFLOAT', user_billing_key, 'credits_balance', -total_cost)
redis.call('HINCRBYFLOAT', user_billing_key, 'credits_spent', total_cost)

-- Add operator earnings
redis.call('HINCRBYFLOAT', operator_billing_key, 'earnings_balance', operator_share)
redis.call('HINCRBYFLOAT', operator_billing_key, 'earnings_total', operator_share)
redis.call('HINCRBY', operator_billing_key, 'jobs_completed', 1)
redis.call('HINCRBY', operator_billing_key, 'tokens_processed', tonumber(tokens))

-- Add protocol fee
redis.call('HINCRBYFLOAT', protocol_billing_key, 'total_fees', protocol_fee)
redis.call('HINCRBY', protocol_billing_key, 'total_jobs', 1)
redis.call('HINCRBY', protocol_billing_key, 'total_tokens', tonumber(tokens))

-- Record transaction
redis.call('HSET', tx_hash_key,
    'id', tx_id,
    'job_id', job_id,
    'user_id', user_id,
    'operator_id', operator_id,
    'amount', tostring(total_cost),
    'operator_share', tostring(operator_share),
    'protocol_fee', tostring(protocol_fee),
    'tokens', tokens,
    'model', model,
    'timestamp', now,
    'status', 'completed')

-- Add to user and operator transaction lists
redis.call('LPUSH', user_tx_list, tx_id)
redis.call('LTRIM', user_tx_list, 0, 999)
redis.call('LPUSH', operator_tx_list, tx_id)
redis.call('LTRIM', operator_tx_list, 0, 999)

-- Audit log
redis.call('XADD', audit_stream, '*',
    'event', 'pin_job_completed',
    'tx_id', tx_id,
    'job_id', job_id,
    'user_id', user_id,
    'operator_id', operator_id,
    'amount', tostring(total_cost),
    'tokens', tokens,
    'timestamp', now)

return {'ok', tx_id, tostring(user_balance - total_cost)}
"""


LUA_PIN_WITHDRAW = """
local operator_billing_key = KEYS[1]
local withdrawal_hash_key = KEYS[2]
local pending_withdrawals_set = KEYS[3]
local audit_stream = KEYS[4]
local operator_withdrawals_list = KEYS[5]

local withdrawal_id = ARGV[1]
local operator_id = ARGV[2]
local amount = tonumber(ARGV[3])
local min_withdrawal = tonumber(ARGV[4])
local bsc_address = ARGV[5]
local now = ARGV[6]

-- Check operator has sufficient balance
local earnings_balance = tonumber(redis.call('HGET', operator_billing_key, 'earnings_balance') or '0')
if earnings_balance < amount then
    return {'error', 'INSUFFICIENT_BALANCE', tostring(earnings_balance), tostring(amount)}
end

-- Check minimum withdrawal
if amount < min_withdrawal then
    return {'error', 'BELOW_MINIMUM', tostring(min_withdrawal), tostring(amount)}
end

-- Deduct from earnings balance atomically
redis.call('HINCRBYFLOAT', operator_billing_key, 'earnings_balance', -amount)
redis.call('HINCRBYFLOAT', operator_billing_key, 'pending_withdrawals', amount)

-- Create withdrawal record
redis.call('HSET', withdrawal_hash_key,
    'id', withdrawal_id,
    'operator_id', operator_id,
    'amount', tostring(amount),
    'bsc_address', bsc_address,
    'status', 'pending',
    'created_at', now)

-- Add to pending set and operator list
redis.call('SADD', pending_withdrawals_set, withdrawal_id)
redis.call('LPUSH', operator_withdrawals_list, withdrawal_id)
redis.call('LTRIM', operator_withdrawals_list, 0, 99)

-- Audit log
redis.call('XADD', audit_stream, '*',
    'event', 'pin_withdrawal_requested',
    'withdrawal_id', withdrawal_id,
    'operator_id', operator_id,
    'amount', tostring(amount),
    'timestamp', now)

return {'ok', withdrawal_id, tostring(earnings_balance - amount)}
"""


LUA_PIN_ADD_CREDITS = """
local user_billing_key = KEYS[1]
local audit_stream = KEYS[2]

local user_id = ARGV[1]
local amount = tonumber(ARGV[2])
local source = ARGV[3]
local now = ARGV[4]

-- Add credits
local new_balance = redis.call('HINCRBYFLOAT', user_billing_key, 'credits_balance', amount)
redis.call('HINCRBYFLOAT', user_billing_key, 'credits_purchased', amount)

-- Audit log
redis.call('XADD', audit_stream, '*',
    'event', 'pin_credits_added',
    'user_id', user_id,
    'amount', tostring(amount),
    'source', source,
    'new_balance', tostring(new_balance),
    'timestamp', now)

return {'ok', tostring(new_balance)}
"""


class RedisStorage:
    def __init__(self):
        self.r = get_redis()
        self._register_lua_scripts()

    def _register_lua_scripts(self):
        """Register Lua scripts for atomic operations."""
        # PIN financial operations
        self._lua_pin_job_complete = self.r.register_script(LUA_PIN_JOB_COMPLETE)
        self._lua_pin_withdraw = self.r.register_script(LUA_PIN_WITHDRAW)
        self._lua_pin_add_credits = self.r.register_script(LUA_PIN_ADD_CREDITS)

    def get_user(self, user_id: str) -> Optional[User]:
        data = self.r.hgetall(key(f"users:{user_id}"))
        if not data:
            return None
        return User(
            id=data["id"],
            email=data["email"],
            display_name=data["display_name"],
            role=UserRole(data["role"]),
            plan=PlanType(data.get("plan", "free")),
            organization_id=data.get("organization_id") or None,
            active_environment_id=data.get("active_environment_id") or None,
            is_active=data.get("is_active") == "true",
            email_verified=data.get("email_verified") == "true",
            created_at=data["created_at"]
        )

    def get_session_user(self, token: str) -> Optional[User]:
        data = self.r.hgetall(key(f"sessions:{token}"))
        if not data:
            return None
        return self.get_user(data["user_id"])

    def get_api_key_by_secret(self, secret: str) -> Optional[tuple]:
        """
        Validate API key - checks both standard (aai_) and extended (aai_pub_, aai_srv_) keys.
        Returns (api_key, user) tuple if valid, None otherwise.
        """
        secret_hash = hashlib.sha256(secret.encode()).hexdigest()
        
        # First check standard API keys (aai_)
        key_id = self.r.get(key(f"api_keys:hash:{secret_hash}"))
        if key_id:
            data = self.r.hgetall(key(f"api_keys:{key_id}"))
            if data and data.get("is_active") == "true":
                self.r.hset(key(f"api_keys:{key_id}"), "last_used_at", now_iso())
                
                user = self.get_user(data["user_id"])
                if user and user.is_active:
                    api_key = ApiKey(
                        id=data["id"],
                        user_id=data["user_id"],
                        key_prefix=data["key_prefix"],
                        name=data["name"],
                        is_active=True,
                        last_used_at=now_iso(),
                        created_at=data["created_at"]
                    )
                    return (api_key, user)
        
        # Then check extended API keys (aai_pub_, aai_srv_, or extended aai_)
        ext_key_id = self.r.get(key(f"api_keys_ext:hash:{secret_hash}"))
        if ext_key_id:
            return self.get_api_key_extended_by_secret(secret)
        
        return None

    def pin_create_operator(self, user_id: str, name: str, endpoint: Optional[str], models: list,
                           capacity: int, pricing_per_1k_tokens: float, region: str,
                           api_key_hash: str, client_id: str = None, api_key_encrypted: str = None,
                           connection_mode: str = "websocket", payout_address: str = None) -> "PinOperator":
        """
        Create a PIN operator.
        
        connection_mode:
            - "websocket" (default, secure): Operator runs Clientd daemon, all inference via WebSocket
            - "http" (legacy): AiAS calls operator's public endpoint directly
        """
        from api.models.schemas import PinOperator, OperatorStatus, OperatorRegion
        operator_id = f"op_{gen_id()[:12]}"
        now = now_iso()
        
        data = {
            "id": operator_id,
            "user_id": user_id,
            "name": name,
            "endpoint": endpoint or "",
            "api_key_hash": api_key_hash,
            "api_key_encrypted": api_key_encrypted or "",
            "client_id": client_id or "",
            "status": "active",
            "models": json.dumps(models),
            "capacity": str(capacity),
            "current_load": "0",
            "pricing_per_1k_tokens": str(pricing_per_1k_tokens),
            "region": region,
            "connection_mode": connection_mode,
            "payout_address": payout_address or "",
            "created_at": now,
            "updated_at": now
        }
        
        self.r.hset(key(f"pin:operators:{operator_id}"), mapping=data)
        self.r.sadd(key("pin:operators:all"), operator_id)
        self.r.sadd(key("pin:operators:active"), operator_id)
        
        if connection_mode == "websocket":
            pass
        else:
            self.r.sadd(key("pin:operators:online"), operator_id)
            import time as time_module
            self.r.setex(key(f"pin:heartbeats:{operator_id}"), 300, str(time_module.time()))
        
        for model in models:
            self.r.sadd(key(f"pin:models:{model}"), operator_id)
            self.r.sadd(key("pin:models:available"), model)
        
        self._pin_init_reputation(operator_id)
        self._pin_init_operator_billing(operator_id)
        
        return PinOperator(
            id=operator_id,
            user_id=user_id,
            name=name,
            endpoint=endpoint,
            api_key_hash=api_key_hash,
            status=OperatorStatus.ACTIVE,
            models=models,
            capacity=capacity,
            current_load=0,
            pricing_per_1k_tokens=pricing_per_1k_tokens,
            region=OperatorRegion(region),
            connection_mode=connection_mode,
            payout_address=payout_address,
            created_at=now,
            updated_at=now
        )

    def pin_get_operator(self, operator_id: str) -> Optional["PinOperator"]:
        from api.models.schemas import PinOperator, OperatorStatus, OperatorRegion
        data = self.r.hgetall(key(f"pin:operators:{operator_id}"))
        if not data:
            return None
        endpoint = data.get("endpoint", "")
        payout_addr = data.get("payout_address", "")
        
        # Check node-level pricing first, fallback to operator-level
        node_price = None
        node_ids = self.r.smembers(key(f"pin:operator:{operator_id}:nodes"))
        for nid in node_ids:
            np = self.r.hget(key(f"pin:nodes:{nid}"), "price_per_thousand_tokens")
            if np:
                node_price = float(np)
                break
        effective_price = node_price if node_price is not None else float(data.get("pricing_per_1k_tokens", 0.5))
        
        return PinOperator(
            id=data["id"],
            user_id=data["user_id"],
            name=data["name"],
            endpoint=endpoint if endpoint else None,
            api_key_hash=data.get("api_key_hash"),
            status=OperatorStatus(data.get("status", "pending")),
            models=json.loads(data.get("models", "[]")),
            capacity=int(data.get("capacity", 10)),
            current_load=int(data.get("current_load", 0)),
            pricing_per_1k_tokens=effective_price,
            region=OperatorRegion(data.get("region", "global")),
            connection_mode=data.get("connection_mode", "websocket"),
            payout_address=payout_addr if payout_addr else None,
            created_at=data["created_at"],
            updated_at=data["updated_at"]
        )

    def pin_get_operator_by_api_key(self, api_key: str) -> Optional["PinOperator"]:
        import hashlib
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        operator_ids = self.r.smembers(key("pin:operators:all"))
        for op_id in operator_ids:
            data = self.r.hgetall(key(f"pin:operators:{op_id}"))
            if data and data.get("api_key_hash") == key_hash:
                return self.pin_get_operator(op_id)
        return None

    def pin_get_operator_by_endpoint(self, endpoint: str) -> Optional["PinOperator"]:
        operator_ids = self.r.smembers(key("pin:operators:all"))
        for op_id in operator_ids:
            data = self.r.hgetall(key(f"pin:operators:{op_id}"))
            if data and data.get("endpoint") == endpoint:
                return self.pin_get_operator(op_id)
        return None

    def pin_get_operator_by_user(self, user_id: str) -> Optional["PinOperator"]:
        """Get operator by user_id for session-authenticated access."""
        operator_ids = self.r.smembers(key("pin:operators:all"))
        for op_id in operator_ids:
            data = self.r.hgetall(key(f"pin:operators:{op_id}"))
            if data and data.get("user_id") == user_id:
                return self.pin_get_operator(op_id)
        return None

    def pin_list_operators(self, active_only: bool = True) -> List["PinOperator"]:
        if active_only:
            operator_ids = self.r.smembers(key("pin:operators:active"))
        else:
            operator_ids = self.r.smembers(key("pin:operators:all"))
        return [op for op_id in operator_ids if (op := self.pin_get_operator(op_id))]

    def pin_update_operator_status(self, operator_id: str, status: str) -> None:
        self.r.hset(key(f"pin:operators:{operator_id}"), "status", status)
        self.r.hset(key(f"pin:operators:{operator_id}"), "updated_at", now_iso())
        if status == "active":
            self.r.sadd(key("pin:operators:active"), operator_id)
        else:
            self.r.srem(key("pin:operators:active"), operator_id)

    def pin_record_heartbeat(self, operator_id: str, current_load: int, capacity: int,
                            models_available: list) -> None:
        import time as time_module
        now = now_iso()
        now_ts = str(time_module.time())
        self.r.setex(key(f"pin:heartbeats:{operator_id}"), 60, now_ts)
        self.r.sadd(key("pin:operators:online"), operator_id)
        self.r.hset(key(f"pin:operators:{operator_id}"), mapping={
            "current_load": str(current_load),
            "capacity": str(capacity),
            "models": json.dumps(models_available),
            "updated_at": now
        })

    def pin_is_operator_online(self, operator_id: str) -> bool:
        return self.r.exists(key(f"pin:heartbeats:{operator_id}")) > 0

    def _normalize_endpoint(self, endpoint: str) -> str:
        """Normalize endpoint to consistent format for uniqueness check."""
        import re
        from urllib.parse import urlparse
        
        endpoint = endpoint.strip().lower().rstrip("/")
        parsed = urlparse(endpoint)
        host = parsed.hostname or ""
        port = parsed.port
        
        if not port:
            port = 443 if parsed.scheme == "https" else 80
        
        return f"{host}:{port}"

    def _get_endpoint_hash(self, endpoint: str) -> str:
        """Generate hash for endpoint uniqueness check."""
        normalized = self._normalize_endpoint(endpoint)
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def pin_check_endpoint_exists(self, endpoint: str, exclude_node_id: str = None, 
                                    exclude_operator_id: str = None) -> Optional[dict]:
        """Check if endpoint already registered. Returns existing node info if found."""
        endpoint_hash = self._get_endpoint_hash(endpoint)
        
        node_ids = self.r.smembers(key("pin:nodes:all"))
        for nid in node_ids:
            if exclude_node_id and nid == exclude_node_id:
                continue
            data = self.r.hgetall(key(f"pin:nodes:{nid}"))
            if data:
                if exclude_operator_id and data.get("operator_id") == exclude_operator_id:
                    continue
                existing_hash = self._get_endpoint_hash(data.get("endpoint", ""))
                if existing_hash == endpoint_hash:
                    return {
                        "node_id": nid,
                        "alias": data.get("alias", ""),
                        "operator_id": data.get("operator_id", ""),
                        "endpoint": data.get("endpoint", "")
                    }
        
        operator_ids = self.r.smembers(key("pin:operators:all"))
        for op_id in operator_ids:
            if exclude_operator_id and op_id == exclude_operator_id:
                continue
            op_data = self.r.hgetall(key(f"pin:operators:{op_id}"))
            if op_data:
                existing_hash = self._get_endpoint_hash(op_data.get("endpoint", ""))
                if existing_hash == endpoint_hash:
                    return {
                        "node_id": None,
                        "operator_id": op_id,
                        "alias": op_data.get("name", ""),
                        "endpoint": op_data.get("endpoint", ""),
                        "is_operator": True
                    }
        
        return None

    def _pin_index_node_models(self, node_id: str, operator_id: str, models: list) -> None:
        """Register `models` in EVERY index the selector might consult.

        There were two, written by different paths and never reconciled:

            pin:models:{model}       -> OPERATOR ids   (operator registration, WS)
            pin:node_models:{model}  -> NODE ids       (node create/update)
            pin:models:available     -> model ids      (the advertised catalog)

        The node paths wrote only the node index, so a long-running daemon —
        which after its first registration only ever takes the UPDATE path —
        ended up registered exclusively where pin_service._operator_serves does
        not look. Observed in production: the gateway answering "No operators
        available for model: muse-local:latest" in the same minute the operator
        logged "Completed successfully (1571+107 tokens)".

        Writing all three is cheap, idempotent, and removes the class of bug
        rather than the instance. Availability is still gated by heartbeats and
        the online set; this only makes a registration findable.
        """
        for model in models or []:
            if not model:
                continue
            try:
                self.r.sadd(key(f"pin:node_models:{model}"), node_id)
                self.r.sadd(key(f"pin:models:{model}"), operator_id)
                self.r.sadd(key("pin:models:available"), model)
            except Exception as exc:  # noqa: BLE001 -- indexing must not fail a registration
                print(f"[PIN STORAGE] model index write failed for {model}: {exc}")

    def pin_create_node(self, operator_id: str, alias: str, endpoint: str, 
                       models: list, capacity: int, region: str) -> dict:
        node_id = f"node_{gen_id()[:12]}"
        now = now_iso()
        
        node_data = {
            "id": node_id,
            "operator_id": operator_id,
            "alias": alias,
            "endpoint": endpoint,
            "models": json.dumps(models),
            "capacity": str(capacity),
            "current_load": "0",
            "region": region,
            "status": "active",
            "is_primary": "false",
            "created_at": now,
            "updated_at": now,
            "last_seen": ""
        }
        
        self.r.hset(key(f"pin:nodes:{node_id}"), mapping=node_data)
        self.r.sadd(key(f"pin:operator:{operator_id}:nodes"), node_id)
        self.r.sadd(key("pin:nodes:all"), node_id)
        self.r.sadd(key("pin:nodes:active"), node_id)
        
        self._pin_index_node_models(node_id, operator_id, models)
        
        return {
            "node_id": node_id,
            "alias": alias,
            "endpoint": endpoint,
            "models": models,
            "capacity": capacity,
            "region": region,
            "status": "active"
        }

    def pin_find_or_create_node(self, operator_id: str, alias: str, models: list, 
                                 capacity: int = 10, region: str = "global") -> dict:
        """Find existing node by alias for operator, or create with unique naming."""
        existing_nodes = self.pin_list_operator_nodes(operator_id)
        
        for node in existing_nodes:
            if node.get("alias") == alias:
                self.r.hset(key(f"pin:nodes:{node['id']}"), mapping={
                    "models": json.dumps(models),
                    "capacity": str(capacity),
                    "region": region,
                    "updated_at": now_iso(),
                    "last_seen": now_iso()
                })
                self._pin_index_node_models(node['id'], operator_id, models)
                node["models"] = models
                node["capacity"] = capacity
                node["region"] = region
                return {"node": node, "created": False}
        
        base_alias = alias
        existing_aliases = {n.get("alias", "") for n in existing_nodes}
        
        if alias in existing_aliases:
            suffix_ord = ord('a')
            while f"{base_alias}-{chr(suffix_ord)}" in existing_aliases:
                suffix_ord += 1
                if suffix_ord > ord('z'):
                    suffix_ord = ord('a')
                    base_alias = f"{alias}_{len(existing_aliases)}"
            alias = f"{base_alias}-{chr(suffix_ord)}"
        
        new_node = self.pin_create_node(
            operator_id=operator_id,
            alias=alias,
            endpoint="websocket",
            models=models,
            capacity=capacity,
            region=region
        )
        return {"node": new_node, "created": True}

    def pin_get_node(self, node_id: str) -> Optional[dict]:
        data = self.r.hgetall(key(f"pin:nodes:{node_id}"))
        if not data:
            return None
        is_online = self.r.exists(key(f"pin:node_heartbeats:{node_id}")) > 0
        node_price = data.get("price_per_thousand_tokens")
        return {
            "id": data["id"],
            "operator_id": data["operator_id"],
            "alias": data["alias"],
            "endpoint": data["endpoint"],
            "client_id": data.get("client_id", ""),
            "models": json.loads(data.get("models", "[]")),
            "capacity": int(data.get("capacity", 10)),
            "current_load": int(data.get("current_load", 0)),
            "region": data.get("region", "global"),
            "status": data.get("status", "active"),
            "is_primary": data.get("is_primary", "false") == "true",
            "online": is_online,
            "created_at": data["created_at"],
            "updated_at": data["updated_at"],
            "last_seen": data.get("last_seen", ""),
            "price_per_thousand_tokens": float(node_price) if node_price else None
        }

    def pin_list_operator_nodes(self, operator_id: str) -> list:
        node_ids = self.r.smembers(key(f"pin:operator:{operator_id}:nodes"))
        nodes = []
        for nid in node_ids:
            node = self.pin_get_node(nid)
            if node:
                nodes.append(node)
        return sorted(nodes, key=lambda n: (not n.get("is_primary", False), n.get("created_at", "")))

    def pin_update_node(self, node_id: str, updates: dict) -> Optional[dict]:
        if not self.r.exists(key(f"pin:nodes:{node_id}")):
            return None
        
        allowed_fields = ["alias", "endpoint", "capacity", "region", "status", "is_primary"]
        update_data = {k: str(v) if k != "is_primary" else ("true" if v else "false") 
                       for k, v in updates.items() if k in allowed_fields}
        update_data["updated_at"] = now_iso()
        
        if "models" in updates:
            old_models = json.loads(self.r.hget(key(f"pin:nodes:{node_id}"), "models") or "[]")
            for m in old_models:
                self.r.srem(key(f"pin:node_models:{m}"), node_id)
            owner = self.r.hget(key(f"pin:nodes:{node_id}"), "operator_id")
            if isinstance(owner, bytes):
                owner = owner.decode()
            self._pin_index_node_models(node_id, owner or "", updates["models"])
            update_data["models"] = json.dumps(updates["models"])
        
        self.r.hset(key(f"pin:nodes:{node_id}"), mapping=update_data)
        return self.pin_get_node(node_id)

    def pin_delete_node(self, node_id: str) -> bool:
        data = self.r.hgetall(key(f"pin:nodes:{node_id}"))
        if not data:
            return False
        
        operator_id = data.get("operator_id")
        models = json.loads(data.get("models", "[]"))
        
        for m in models:
            self.r.srem(key(f"pin:node_models:{m}"), node_id)
        
        self.r.srem(key(f"pin:operator:{operator_id}:nodes"), node_id)
        self.r.srem(key("pin:nodes:all"), node_id)
        self.r.srem(key("pin:nodes:active"), node_id)
        self.r.srem(key("pin:nodes:online"), node_id)
        self.r.delete(key(f"pin:nodes:{node_id}"))
        self.r.delete(key(f"pin:node_heartbeats:{node_id}"))
        
        return True

    def pin_node_heartbeat(self, node_id: str, current_load: int, capacity: int, 
                          models: list) -> None:
        import time as time_module
        now = now_iso()
        self.r.setex(key(f"pin:node_heartbeats:{node_id}"), 300, str(time_module.time()))
        self.r.sadd(key("pin:nodes:online"), node_id)
        self.r.hset(key(f"pin:nodes:{node_id}"), mapping={
            "current_load": str(current_load),
            "capacity": str(capacity),
            "models": json.dumps(models),
            "last_seen": now,
            "updated_at": now
        })

    def pin_get_node_by_client_id(self, client_id: str) -> Optional[dict]:
        node_ids = self.r.smembers(key("pin:nodes:all"))
        for nid in node_ids:
            data = self.r.hgetall(key(f"pin:nodes:{nid}"))
            if data and data.get("client_id") == client_id:
                return self.pin_get_node(nid)
        return None

    def pin_regenerate_node_credentials(self, node_id: str) -> Optional[dict]:
        if not self.r.exists(key(f"pin:nodes:{node_id}")):
            return None
        
        raw_key = f"pin_nk_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        client_id = f"pin_ncid_{secrets.token_urlsafe(16)}"
        
        self.r.hset(key(f"pin:nodes:{node_id}"), mapping={
            "api_key_hash": key_hash,
            "client_id": client_id,
            "updated_at": now_iso()
        })
        
        return {
            "node_id": node_id,
            "client_id": client_id,
            "api_key": raw_key
        }

    def pin_set_primary_node(self, operator_id: str, node_id: str) -> bool:
        node_ids = self.r.smembers(key(f"pin:operator:{operator_id}:nodes"))
        for nid in node_ids:
            self.r.hset(key(f"pin:nodes:{nid}"), "is_primary", "false")
        
        if node_id in node_ids:
            self.r.hset(key(f"pin:nodes:{node_id}"), "is_primary", "true")
            return True
        return False

    def pin_get_operator_node_stats(self, operator_id: str) -> dict:
        nodes = self.pin_list_operator_nodes(operator_id)
        total_capacity = sum(n.get("capacity", 0) for n in nodes)
        total_load = sum(n.get("current_load", 0) for n in nodes)
        online_count = sum(1 for n in nodes if n.get("online", False))
        all_models = set()
        for n in nodes:
            all_models.update(n.get("models", []))
        
        return {
            "total_nodes": len(nodes),
            "online_nodes": online_count,
            "offline_nodes": len(nodes) - online_count,
            "total_capacity": total_capacity,
            "total_load": total_load,
            "utilization": (total_load / total_capacity * 100) if total_capacity > 0 else 0,
            "models": list(all_models)
        }

    def _pin_init_reputation(self, operator_id: str) -> None:
        data = {
            "operator_id": operator_id,
            "total_requests": "0",
            "successful_requests": "0",
            "failed_requests": "0",
            "total_latency_ms": "0",
            "avg_latency_ms": "0.0",
            "success_rate": "1.0",
            "score": "0.5",
            "last_updated": now_iso()
        }
        self.r.hset(key(f"pin:reputation:{operator_id}"), mapping=data)

    def pin_get_reputation(self, operator_id: str) -> Optional["PinReputation"]:
        from api.models.schemas import PinReputation
        data = self.r.hgetall(key(f"pin:reputation:{operator_id}"))
        if not data:
            return None
        return PinReputation(
            operator_id=operator_id,
            total_requests=int(data.get("total_requests", 0)),
            successful_requests=int(data.get("successful_requests", 0)),
            failed_requests=int(data.get("failed_requests", 0)),
            total_latency_ms=int(data.get("total_latency_ms", 0)),
            avg_latency_ms=float(data.get("avg_latency_ms", 0.0)),
            success_rate=float(data.get("success_rate", 1.0)),
            score=float(data.get("score", 0.5)),
            last_updated=data.get("last_updated", now_iso())
        )

    def pin_update_reputation(self, operator_id: str, success: bool, latency_ms: int = 0) -> None:
        rep_key = key(f"pin:reputation:{operator_id}")
        self.r.hincrby(rep_key, "total_requests", 1)
        if success:
            self.r.hincrby(rep_key, "successful_requests", 1)
            self.r.hincrby(rep_key, "total_latency_ms", latency_ms)
        else:
            self.r.hincrby(rep_key, "failed_requests", 1)
        
        data = self.r.hgetall(rep_key)
        total = int(data.get("total_requests", 1))
        successful = int(data.get("successful_requests", 0))
        total_latency = int(data.get("total_latency_ms", 0))
        
        success_rate = successful / total if total > 0 else 1.0
        avg_latency = total_latency / successful if successful > 0 else 0.0
        latency_score = max(0, 1 - (avg_latency / 10000))
        score = (0.7 * success_rate) + (0.3 * latency_score)
        
        self.r.hset(rep_key, mapping={
            "success_rate": str(success_rate),
            "avg_latency_ms": str(avg_latency),
            "score": str(score),
            "last_updated": now_iso()
        })

    def _pin_init_operator_billing(self, operator_id: str) -> None:
        data = {
            "operator_id": operator_id,
            "earnings_balance": "0.0",
            "total_earned": "0.0",
            "total_paid_out": "0.0",
            "last_payout": ""
        }
        self.r.hset(key(f"pin:billing:operator:{operator_id}"), mapping=data)

    def pin_get_operator_billing(self, operator_id: str) -> Optional["PinBillingOperator"]:
        from api.models.schemas import PinBillingOperator
        data = self.r.hgetall(key(f"pin:billing:operator:{operator_id}"))
        if not data:
            return None
        return PinBillingOperator(
            operator_id=operator_id,
            earnings_balance=float(data.get("earnings_balance", 0.0)),
            total_earned=float(data.get("total_earned", 0.0)),
            total_paid_out=float(data.get("total_paid_out", 0.0)),
            last_payout=data.get("last_payout") or None
        )

    def pin_get_operator_earnings_period(self, operator_id: str, hours: int) -> float:
        """Calculate operator earnings for a given time period (in hours)."""
        from datetime import datetime, timedelta
        
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        total_earnings = 0.0
        
        # Get all transaction IDs for this operator
        tx_ids = self.r.lrange(key(f"pin:transactions:operator:{operator_id}"), 0, -1)
        
        for tx_id in tx_ids:
            tx_data = self.r.hgetall(key(f"pin:transactions:{tx_id}"))
            if not tx_data:
                continue
            
            # Parse timestamp and check if within period
            timestamp_str = tx_data.get("timestamp", "")
            if not timestamp_str:
                continue
            
            try:
                # Handle ISO format timestamp
                tx_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00").replace("+00:00", ""))
                if tx_time >= cutoff:
                    operator_share = float(tx_data.get("operator_share", 0))
                    total_earnings += operator_share
            except (ValueError, TypeError):
                continue
        
        return total_earnings

    def pin_get_user_billing(self, user_id: str) -> Optional["PinBillingUser"]:
        from api.models.schemas import PinBillingUser
        data = self.r.hgetall(key(f"pin:billing:user:{user_id}"))
        if not data:
            self.r.hset(key(f"pin:billing:user:{user_id}"), mapping={
                "user_id": user_id,
                "credits_balance": "0.0",
                "total_spent": "0.0",
                "last_transaction": ""
            })
            return PinBillingUser(user_id=user_id, credits_balance=0.0, total_spent=0.0)
        return PinBillingUser(
            user_id=user_id,
            credits_balance=float(data.get("credits_balance", 0.0)),
            total_spent=float(data.get("total_spent", 0.0)),
            last_transaction=data.get("last_transaction") or None
        )

    def pin_add_user_credits(self, user_id: str, amount: float, source: str = "manual") -> float:
        """Atomically add credits to user account. Returns new balance."""
        result = self._lua_pin_add_credits(
            keys=[
                key(f"pin:billing:user:{user_id}"),
                key("audit:stream")
            ],
            args=[user_id, str(amount), source, now_iso()]
        )
        if result[0] == 'ok':
            return float(result[1])
        raise ValueError(f"Failed to add credits: {result}")

    @staticmethod
    def _as_text(value) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)

    def _pin_model_aliases(self, model: str) -> list:
        """`name` and `name:latest` are the same Ollama model.

        The two registration paths do not always report the same spelling, so a
        node registered as `muse-local` is invisible to a request for
        `muse-local:latest` unless both are tried.
        """
        m = (model or "").strip()
        if not m:
            return []
        base = m[: -len(":latest")] if m.endswith(":latest") else m
        if base == m and ":" in m:
            return [m]
        return list(dict.fromkeys([m, base, f"{base}:latest"]))

    def _pin_operators_for_model(self, model: str) -> set:
        """Operators that serve `model`, according to EVERY index that records it.

        Widening the read is deliberately preferred over trusting any single
        index: they disagree in production today, and a selector that believes
        one of them is a selector that hides a working operator.
        """
        found = set()
        for name in self._pin_model_aliases(model):
            # 1. operator-level index — the only one this used to read.
            for op in (self.r.smembers(key(f"pin:models:{name}")) or set()):
                found.add(self._as_text(op))

            # 2. node-level index, followed back to its owner. This is the arm
            #    that was missing, and the one the steady-state update path
            #    writes.
            for nid in (self.r.smembers(key(f"pin:node_models:{name}")) or set()):
                try:
                    node = self.r.hgetall(key(f"pin:nodes:{self._as_text(nid)}")) or {}
                except Exception:  # noqa: BLE001
                    continue
                owner = node.get("operator_id") or node.get(b"operator_id")
                if owner:
                    found.add(self._as_text(owner))

        # 3. THE NODE ROWS THEMSELVES — the only record that is written on
        #    every registration and is therefore never behind.
        #
        #    Both indexes above are DERIVED: something has to remember to
        #    `sadd` them, and in production on 2026-08-28 nothing had, for an
        #    operator whose node row read
        #
        #        models: [muse-local:latest, deepseek-r1:32b, tts:chatterbox-turbo]
        #        updated_at: 16:42 (minutes earlier)
        #
        #    while both index sets held nothing for it. The gateway answered
        #    "No operators available for model: muse-local:latest" about a
        #    model the operator was serving in that same minute — the third
        #    time this failure has been diagnosed, each previous time by
        #    widening a read one index further.
        #
        #    `pin_service._operator_serves` already consults node rows; this
        #    function — the one that actually PICKS — did not, so the checker
        #    stayed wider than the chooser and the chooser is what decides.
        #    Only reached when the indexes found nothing, so the steady-state
        #    cost is unchanged.
        if not found:
            found |= self._pin_operators_by_node_scan(model)
        return found

    def _pin_operators_by_node_scan(self, model: str) -> set:
        """Operators whose OWN node rows claim `model`, index or no index.

        The last-resort arm of `_pin_operators_for_model`. Scans the online
        set rather than every operator ever registered: an operator that is
        not online cannot serve this request anyway, so the scan is bounded by
        concurrent operators, not by history.

        Repairs the indexes it just proved wrong. A read path that silently
        compensates for a broken write path hides the breakage forever and
        pays the scan on every request; writing back means the NEXT request
        takes arm 1 and the drift is self-healing rather than permanent.
        """
        import json as _json

        wanted = set(self._pin_model_aliases(model))
        if not wanted:
            return set()

        found = set()
        try:
            online = self.r.smembers(key("pin:operators:online")) or set()
        except Exception:  # noqa: BLE001 -- selection must never raise
            return set()

        for raw_op in online:
            op_id = self._as_text(raw_op)
            try:
                nodes = self.pin_list_operator_nodes(op_id) or []
            except Exception:  # noqa: BLE001
                continue
            for node in nodes:
                models = node.get("models") if isinstance(node, dict) else None
                if isinstance(models, str):
                    try:
                        models = _json.loads(models)
                    except (ValueError, TypeError):
                        models = []
                hit = next(
                    (self._as_text(m) for m in (models or [])
                     if self._as_text(m) in wanted),
                    None,
                )
                if hit is None:
                    continue
                found.add(op_id)
                # Write back under the spelling the node actually reported,
                # so arm 1 answers next time.
                try:
                    self.r.sadd(key(f"pin:models:{hit}"), op_id)
                    self.r.sadd(key("pin:models:available"), hit)
                    print(f"[PIN SELECT] repaired model index for {op_id}: {hit} "
                          f"(found via node row, indexes were empty)")
                except Exception:  # noqa: BLE001 -- repair is best-effort
                    pass
                break
        return found

    def pin_select_operator(self, model: str, exclude: list = None, max_price: float = None) -> Optional["PinOperator"]:
        """
        Select best operator for a model based on:
        1. Quality tier (verified > standard > slow > pending, failed excluded)
        2. Reputation score as tiebreaker within same tier
        3. Optional max_price filter (per 1K tokens)
        """
        exclude = exclude or []

        # EVERY INDEX, NOT ONE. `pin_service._operator_serves` was widened to
        # read pin:models, pin:node_models (followed back to the owner) and the
        # node rows themselves, with `name`/`name:latest` treated as one model.
        # This function — the one that actually PICKS — was left reading a single
        # index with an exact string, so the chooser was narrower than the
        # checker. A daemon whose models were last written by a node path (every
        # steady-state `[NODE] UPDATED`) is registered where the checker looks
        # and where the selector does not, and the gateway then answers "No
        # operators available" for a model an operator is serving.
        candidates = self._pin_operators_for_model(model)
        online = {self._as_text(op) for op in (self.r.smembers(key("pin:operators:online")) or set())}
        valid = [op for op in candidates if op in online and op not in exclude]
        
        # No fallback to random operators - only route to nodes that actually have the model
        if not valid:
            return None
        
        # Quality tier priority: verified=4, standard=3, slow=2, pending=1, failed=0 (excluded)
        tier_priority = {"verified": 4, "standard": 3, "slow": 2, "pending": 1, "failed": 0}
        
        scored = []
        for op_id in valid:
            # Get quality tier
            op_data = self.r.hgetall(key(f"pin:operators:{op_id}"))
            quality_tier = op_data.get("quality_tier", "pending") if op_data else "pending"
            
            # Exclude failed operators from production traffic
            if quality_tier == "failed":
                continue
            
            # Apply max_price filter if specified
            # Check node-level pricing first, fallback to operator-level
            if max_price is not None and op_data:
                try:
                    # Try to get node-level pricing first
                    node_ids = self.r.smembers(key(f"pin:operator:{op_id}:nodes"))
                    node_price = None
                    for nid in node_ids:
                        np = self.r.hget(key(f"pin:nodes:{nid}"), "price_per_thousand_tokens")
                        if np:
                            node_price = float(np)
                            break
                    # Fallback to operator-level pricing if no node price
                    effective_price = node_price if node_price is not None else float(op_data.get("pricing_per_1k_tokens", 0.5))
                    if effective_price > max_price:
                        continue
                except:
                    pass
            
            tier_score = tier_priority.get(quality_tier, 1)
            
            # Get reputation as tiebreaker
            rep = self.pin_get_reputation(op_id)
            rep_score = rep.score if rep else 0.5
            
            # Combined score: tier priority * 10 + reputation (0-1)
            # This ensures tier always takes precedence
            combined_score = (tier_score * 10) + rep_score
            scored.append((op_id, combined_score, quality_tier))
        
        if not scored:
            return None
        
        scored.sort(key=lambda x: x[1], reverse=True)
        return self.pin_get_operator(scored[0][0]) if scored else None

    def pin_create_job(self, user_id: str, operator_id: str, model: str) -> "PinJob":
        from api.models.schemas import PinJob, PinJobStatus
        job_id = gen_id()
        now = now_iso()
        data = {
            "id": job_id,
            "user_id": user_id,
            "operator_id": operator_id,
            "model": model,
            "status": "assigned",
            "input_tokens": "0",
            "output_tokens": "0",
            "total_tokens": "0",
            "credits_charged": "0.0",
            "latency_ms": "0",
            "error": "",
            "created_at": now,
            "started_at": now,
            "completed_at": ""
        }
        self.r.hset(key(f"pin:jobs:{job_id}"), mapping=data)
        self.r.lpush(key("pin:jobs:recent"), job_id)
        self.r.ltrim(key("pin:jobs:recent"), 0, 999)
        return PinJob(
            id=job_id, user_id=user_id, operator_id=operator_id, model=model,
            status=PinJobStatus.ASSIGNED, created_at=now, started_at=now
        )

    def pin_complete_job(self, job_id: str, input_tokens: int, output_tokens: int,
                        total_tokens: int, credits_charged: float, latency_ms: int) -> None:
        """Update job status (non-financial update only). Use pin_complete_job_atomic for billing."""
        now = now_iso()
        self.r.hset(key(f"pin:jobs:{job_id}"), mapping={
            "status": "completed",
            "input_tokens": str(input_tokens),
            "output_tokens": str(output_tokens),
            "total_tokens": str(total_tokens),
            "credits_charged": str(credits_charged),
            "latency_ms": str(latency_ms),
            "completed_at": now
        })

    def pin_get_protocol_fee_percent(self) -> float:
        """Get the current protocol fee percentage (0.0 to 1.0). Default is 0.10 (10%)."""
        fee = self.r.get(key("pin:config:protocol_fee_percent"))
        if fee:
            return float(fee)
        return 0.10

    def pin_set_protocol_fee_percent(self, percent: float) -> None:
        """Set the protocol fee percentage (0.0 to 1.0). Must be between 0 and 0.5 (50%)."""
        if percent < 0 or percent > 0.5:
            raise ValueError("Protocol fee must be between 0% and 50%")
        self.r.set(key("pin:config:protocol_fee_percent"), str(percent))

    def pin_complete_job_atomic(self, job_id: str, user_id: str, operator_id: str,
                                total_tokens: int, model: str, pricing_per_1k: float,
                                input_tokens: int, output_tokens: int, latency_ms: int) -> dict:
        """Atomically complete a job with full financial settlement.
        Returns: {'success': True, 'tx_id': str, 'new_balance': float} or {'success': False, 'error': str}
        """
        protocol_fee_percent = self.pin_get_protocol_fee_percent()
        operator_share_percent = 1.0 - protocol_fee_percent
        
        total_cost = (total_tokens / 1000.0) * pricing_per_1k
        operator_share = total_cost * operator_share_percent
        protocol_fee = total_cost * protocol_fee_percent
        
        tx_id = gen_id()
        now = now_iso()
        
        result = self._lua_pin_job_complete(
            keys=[
                key(f"pin:billing:user:{user_id}"),
                key(f"pin:billing:operator:{operator_id}"),
                key("pin:billing:protocol"),
                key(f"pin:transactions:{tx_id}"),
                key(f"pin:transactions:user:{user_id}"),
                key(f"pin:transactions:operator:{operator_id}"),
                key("audit:stream")
            ],
            args=[
                tx_id, job_id, user_id, operator_id,
                str(total_cost), str(operator_share), str(protocol_fee),
                str(total_tokens), model, now
            ]
        )
        
        if result[0] == 'ok':
            # Update job record (non-financial)
            self.r.hset(key(f"pin:jobs:{job_id}"), mapping={
                "status": "completed",
                "input_tokens": str(input_tokens),
                "output_tokens": str(output_tokens),
                "total_tokens": str(total_tokens),
                "credits_charged": str(total_cost),
                "latency_ms": str(latency_ms),
                "completed_at": now,
                "tx_id": tx_id
            })
            
            # Update global metrics
            self.r.hincrby(key("pin:metrics:global"), "total_requests", 1)
            self.r.hincrby(key("pin:metrics:global"), "total_tokens", total_tokens)
            
            return {
                'success': True,
                'tx_id': result[1],
                'new_balance': float(result[2]),
                'total_cost': total_cost,
                'operator_share': operator_share,
                'protocol_fee': protocol_fee
            }
        else:
            error_type = result[1]
            if error_type == 'INSUFFICIENT_CREDITS':
                return {
                    'success': False,
                    'error': 'INSUFFICIENT_CREDITS',
                    'balance': float(result[2]),
                    'required': float(result[3])
                }
            return {'success': False, 'error': str(result)}

    def pin_fail_job(self, job_id: str, error: str) -> None:
        self.r.hset(key(f"pin:jobs:{job_id}"), mapping={
            "status": "failed",
            "error": error,
            "completed_at": now_iso()
        })

    def pin_record_transaction(self, job_id: str, user_id: str, operator_id: str,
                              amount: float, operator_share: float, protocol_fee: float,
                              tokens: int, model: str) -> None:
        tx_id = gen_id()
        now = now_iso()
        data = {
            "id": tx_id,
            "job_id": job_id,
            "user_id": user_id,
            "operator_id": operator_id,
            "amount": str(amount),
            "operator_share": str(operator_share),
            "protocol_fee": str(protocol_fee),
            "tokens": str(tokens),
            "model": model,
            "timestamp": now
        }
        self.r.hset(key(f"pin:transactions:{tx_id}"), mapping=data)
        self.r.lpush(key(f"pin:transactions:operator:{operator_id}"), tx_id)
        self.r.ltrim(key(f"pin:transactions:operator:{operator_id}"), 0, 999)
        
        self.r.hincrbyfloat(key(f"pin:billing:operator:{operator_id}"), "earnings_balance", operator_share)
        self.r.hincrbyfloat(key(f"pin:billing:operator:{operator_id}"), "total_earned", operator_share)
        
        self.r.hincrby(key("pin:metrics:global"), "total_requests", 1)
        self.r.hincrby(key("pin:metrics:global"), "total_tokens", tokens)

    def pin_create_withdrawal_atomic(self, operator_id: str, amount: float, 
                                      payout_email: str, min_withdrawal: float = 50.0) -> dict:
        """Atomically create a withdrawal request with balance check.
        payout_email is now the BSC address for USDT payouts.
        Returns: {'success': True, 'withdrawal_id': str, 'new_balance': float} or {'success': False, 'error': str}
        """
        wd_id = f"wd_{gen_id()[:12]}"
        now = now_iso()
        
        result = self._lua_pin_withdraw(
            keys=[
                key(f"pin:billing:operator:{operator_id}"),
                key(f"pin:withdrawals:{wd_id}"),
                key("pin:withdrawals:pending"),
                key("audit:stream"),
                key(f"pin:withdrawals:operator:{operator_id}")
            ],
            args=[wd_id, operator_id, str(amount), str(min_withdrawal), payout_email, now]
        )
        
        if result[0] == 'ok':
            return {
                'success': True,
                'withdrawal_id': result[1],
                'new_balance': float(result[2])
            }
        else:
            error_type = result[1]
            if error_type == 'INSUFFICIENT_BALANCE':
                return {
                    'success': False,
                    'error': 'INSUFFICIENT_BALANCE',
                    'balance': float(result[2]),
                    'requested': float(result[3])
                }
            elif error_type == 'BELOW_MINIMUM':
                return {
                    'success': False,
                    'error': 'BELOW_MINIMUM',
                    'minimum': float(result[2]),
                    'requested': float(result[3])
                }
            return {'success': False, 'error': str(result)}

    def pin_create_withdrawal(self, operator_id: str, amount: float, method: str) -> "PinWithdrawal":
        """Legacy non-atomic withdrawal. Prefer pin_create_withdrawal_atomic."""
        from api.models.schemas import PinWithdrawal
        wd_id = f"wd_{gen_id()[:12]}"
        now = now_iso()
        data = {
            "id": wd_id,
            "operator_id": operator_id,
            "amount": str(amount),
            "method": method,
            "status": "processing",
            "created_at": now,
            "completed_at": ""
        }
        self.r.hset(key(f"pin:withdrawals:{wd_id}"), mapping=data)
        self.r.hincrbyfloat(key(f"pin:billing:operator:{operator_id}"), "earnings_balance", -amount)
        return PinWithdrawal(id=wd_id, operator_id=operator_id, amount=amount,
                            method=method, status="processing", created_at=now)

    def pin_get_network_status(self) -> "PinNetworkStatus":
        from api.models.schemas import PinNetworkStatus
        all_ops = self.r.scard(key("pin:operators:all"))
        online_ops = self.r.scard(key("pin:operators:online"))
        # Same filtered view as pin_list_available_models: two endpoints
        # disagreeing about what the network offers is its own bug class.
        models = self.pin_list_available_models()
        
        metrics = self.r.hgetall(key("pin:metrics:global"))
        total_requests = int(metrics.get("total_requests", 0))
        total_tokens = int(metrics.get("total_tokens", 0))
        
        total_capacity = 0
        for op_id in self.r.smembers(key("pin:operators:online")):
            op = self.pin_get_operator(op_id)
            if op:
                total_capacity += op.capacity
        
        return PinNetworkStatus(
            status="healthy" if online_ops > 0 else "degraded",
            total_operators=all_ops,
            online_operators=online_ops,
            total_capacity=total_capacity,
            models=models,
            requests_24h=total_requests,
            tokens_24h=total_tokens,
            avg_latency_ms=0.0
        )

    def pin_list_all_advertised_models(self) -> List[str]:
        """The raw historical ledger: every model any operator EVER advertised.

        Kept reachable deliberately. `pin:models:available` is written by 7
        call sites and removed by NONE, so it is a permanent record rather than
        a catalog -- useful for debugging and for the interview/tier provenance
        that hangs off it, and nothing here prunes it.
        """
        return list(self.r.smembers(key("pin:models:available")))

    def _pin_live_models(self) -> List[str]:
        """Models an ONLINE operator actually serves right now.

        THE BUG THIS EXISTS FOR

        Nothing ever evicts a model. `pin:models:available` has 7 `sadd` sites
        and zero removers; `pin:models:{model}` has 14 and zero. Going offline,
        failing an interview, being reaped for a stale heartbeat, or having the
        model deleted from the box all leave both indexes untouched. Only
        `pin:node_models:{model}` has any lifecycle (pin_update_node,
        pin_delete_node).

        Observed 2026-09-07: /network/models advertised 48 models while the one
        online operator served 4. The list still carried a CPU node's entire
        ollama library, two models evicted from the GPU box weeks earlier, and
        a stale cloudflare-tunnel hostname. Any consumer reading that as a menu
        picks a model nobody serves and gets "No operators available" -- which
        is precisely the 503 that cost an afternoon to diagnose.

        Derived from NODE ROWS rather than either model index, because the node
        row's `models` field is rewritten on every registration and update and
        is therefore the only record that is never behind (the same reasoning
        `_pin_operators_for_model`'s last arm already relies on).
        """
        import json as _json

        live = set()
        for op_id in (self.r.smembers(key("pin:operators:online")) or set()):
            op_id = self._as_text(op_id)
            node_ids = self.r.smembers(key(f"pin:operator:{op_id}:nodes")) or set()
            for nid in node_ids:
                # PER NODE, NOT PER OPERATOR. Guarding the whole operator meant
                # one unparseable `models` field erased every model that
                # operator served -- turning a single bad row into a network
                # that appears to offer nothing.
                #
                # The node hash is read directly rather than through
                # pin_get_node, which requires a full row (it raises on a
                # missing created_at). A catalog read must not depend on
                # fields it does not use.
                try:
                    nid = self._as_text(nid)
                    raw = (self.r.hgetall(key(f"pin:nodes:{nid}")) or {}).get("models")
                    if not raw:
                        continue
                    models = raw if isinstance(raw, list) else _json.loads(raw)
                    for m in models:
                        if m:
                            live.add(str(m))
                except Exception as exc:  # noqa: BLE001 -- a catalog must not raise
                    print(f"[PIN STORAGE] live-model scan skipped node "
                          f"{nid} of {op_id}: {exc}")
        return sorted(live)

    def pin_list_available_models(self) -> List[str]:
        """The models a caller can actually reach, filtered at READ time.

        FILTERED, NOT PRUNED. The stored sets are left byte-for-byte alone:
        they are history, other code reads them, and a read path that quietly
        deletes what it disagrees with is unrecoverable if the disagreement is
        wrong. Correcting the answer costs nothing and is reversible.

        EMPTY FALLS BACK TO THE FULL LEDGER. If the live scan yields nothing --
        a stale online set, an unreadable node row -- returning [] would break
        every consumer that reads this as its menu. A catalog that is too long
        is a nuisance; an empty one is an outage, so the failure direction is
        deliberate.
        """
        live = self._pin_live_models()
        if live:
            return live
        return list(self.r.smembers(key("pin:models:available")))

    def pin_evict_stale_models(self, dry_run: bool = False) -> dict:
        """Delete models from the stored catalog that no online node serves.

        THE MISSING LIFECYCLE. `pin:models:available` has seven `sadd` sites
        and, until this, zero removers; `pin:models:{model}` had fourteen and
        zero. An operator that goes offline, fails its interview, is reaped for
        a stale heartbeat, or simply has the model deleted from its box leaves
        both untouched. The set is a lifetime ledger wearing a catalog's name.

        Filtering it at read time (`pin_list_available_models`) corrects the
        answer, and that was the right first move. It does not correct the
        DATA: `api/utils/pin_models.py` resolves requested ids against this
        pool, so every stale entry remains a live route onto a model that has
        not existed for weeks.

        WHAT COUNTS AS SERVED is the node rows of online operators — the same
        source the read path uses, and the only record rewritten on every
        registration and update. Anything else is behind by construction.

        REFUSES ON AN EMPTY SCAN. This is the whole safety property. A stale
        online set, a Redis blip, or every operator restarting at once yields
        an empty live scan; pruning against that would delete the entire
        catalog, and a delete has no fallback the way a read does. The read
        path can afford to answer with history when the scan comes up empty.
        A writer cannot, so it declines and says why.

        Idempotent: a second run finds nothing to do. `dry_run` reports the
        same answer without touching anything, so the removal can be inspected
        before it happens.
        """
        live = set(self._pin_live_models())
        if not live:
            return {
                "evicted": [],
                "kept": sorted(self.r.smembers(key("pin:models:available")) or set()),
                "refused": (
                    "No online operator reported any model, so there is nothing "
                    "to prune against. Removing the catalog on the strength of an "
                    "empty scan would empty it outright; a stale online set is the "
                    "likelier explanation than a network serving nothing."
                ),
            }

        stored = {
            self._as_text(m)
            for m in (self.r.smembers(key("pin:models:available")) or set())
        }
        stale = sorted(stored - live)

        if not dry_run:
            for model in stale:
                # Both indexes, or the per-model operator set outlives the
                # catalog entry and `_pin_operators_for_model` keeps finding
                # operators for a model the catalog no longer lists.
                self.r.srem(key("pin:models:available"), model)
                self.r.delete(key(f"pin:models:{model}"))

        return {
            "evicted": stale,
            "kept": sorted(live),
            "dry_run": dry_run,
        }



storage = RedisStorage()
