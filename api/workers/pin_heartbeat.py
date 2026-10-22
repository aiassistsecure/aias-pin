"""
PIN Heartbeat Worker
Background task that monitors operator heartbeats, runs health checks,
and marks stale operators offline.
"""

import asyncio
import time
import json
from datetime import datetime, timedelta
from typing import Set, Optional

import sys
sys.path.insert(0, '.')

import httpx
from api.services.redis_storage import storage, key

HEARTBEAT_TTL_SECONDS = 60
CHECK_INTERVAL_SECONDS = 15
STALE_THRESHOLD_SECONDS = 60
HEALTH_CHECK_INTERVAL_SECONDS = 30
HEALTH_CHECK_TIMEOUT_SECONDS = 10


class PinHeartbeatWorker:
    def __init__(self):
        self.running = False
        self.checked_count = 0
        self.evicted_count = 0
        self.health_checks_run = 0
        self.health_checks_failed = 0
        self.last_health_check = 0.0
    
    def get_all_active_operators(self) -> list:
        """Get all active operators for health checks."""
        operators = []
        active_ids = storage.r.smembers(key("pin:operators:active"))
        for op_id in active_ids or []:
            op_data = storage.r.hgetall(key(f"pin:operators:{op_id}"))
            if op_data:
                operators.append({
                    "id": op_id,
                    "endpoint": op_data.get("endpoint", ""),
                    "models": json.loads(op_data.get("models", "[]"))
                })
        return operators
    
    def is_operator_websocket_connected(self, operator_id: str) -> bool:
        """Check if operator is connected via WebSocket (online set + recent heartbeat)."""
        is_online = storage.r.sismember(key("pin:operators:online"), operator_id)
        if not is_online:
            return False
        last_heartbeat = self.get_heartbeat_timestamp(operator_id)
        if last_heartbeat == 0.0:
            return True
        age_seconds = time.time() - last_heartbeat
        return age_seconds < STALE_THRESHOLD_SECONDS
    
    async def health_check_operator(self, operator: dict) -> bool:
        """Check operator health. WebSocket-connected operators are healthy by connection status."""
        operator_id = operator.get("id", "")
        endpoint = operator.get("endpoint", "")
        models = operator.get("models", [])
        
        if self.is_operator_websocket_connected(operator_id):
            storage.r.hset(key(f"pin:health:{operator_id}"), mapping={
                "last_check": datetime.utcnow().isoformat(),
                "status": "healthy",
                "method": "websocket"
            })
            return True
        
        if not endpoint or not models:
            return False
        
        if not endpoint.startswith("http"):
            return False
        
        try:
            async with httpx.AsyncClient(timeout=HEALTH_CHECK_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"{endpoint}/v1/chat/completions",
                    json={
                        "model": models[0],
                        "messages": [{"role": "user", "content": "ping"}],
                        "max_tokens": 5
                    },
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    latency_ms = int(response.elapsed.total_seconds() * 1000)
                    storage.r.hset(key(f"pin:health:{operator_id}"), mapping={
                        "last_check": datetime.utcnow().isoformat(),
                        "status": "healthy",
                        "method": "http",
                        "latency_ms": str(latency_ms)
                    })
                    return True
                else:
                    storage.r.hset(key(f"pin:health:{operator_id}"), mapping={
                        "last_check": datetime.utcnow().isoformat(),
                        "status": "unhealthy",
                        "method": "http",
                        "error": f"status_{response.status_code}"
                    })
                    return False
                    
        except httpx.TimeoutException:
            storage.r.hset(key(f"pin:health:{operator_id}"), mapping={
                "last_check": datetime.utcnow().isoformat(),
                "status": "timeout",
                "method": "http",
                "error": "request_timeout"
            })
            return False
        except Exception as e:
            storage.r.hset(key(f"pin:health:{operator_id}"), mapping={
                "last_check": datetime.utcnow().isoformat(),
                "status": "error",
                "method": "http",
                "error": str(e)[:100]
            })
            return False
    
    async def run_health_checks(self) -> tuple:
        """Run health checks on all active operators. Returns (passed, failed)."""
        operators = self.get_all_active_operators()
        passed = 0
        failed = 0
        
        for operator in operators:
            result = await self.health_check_operator(operator)
            self.health_checks_run += 1
            if result:
                passed += 1
            else:
                failed += 1
                self.health_checks_failed += 1
        
        return passed, failed
    
    def get_online_operators(self) -> set:
        """Get set of operator IDs currently marked as online."""
        result = storage.r.smembers(key("pin:operators:online"))
        return set(result) if result else set()
    
    def get_heartbeat_timestamp(self, operator_id: str) -> float:
        """Get the last heartbeat timestamp for an operator."""
        ts = storage.r.get(key(f"pin:heartbeats:{operator_id}"))
        if ts:
            try:
                return float(str(ts))
            except (ValueError, TypeError):
                return 0.0
        return 0.0
    
    def mark_operator_offline(self, operator_id: str) -> None:
        """Mark an operator as offline due to stale heartbeat."""
        storage.r.srem(key("pin:operators:online"), operator_id)
        storage.r.hincrby(key("pin:metrics:global"), "evictions", 1)
        
        storage.r.hset(key(f"pin:operators:{operator_id}"), mapping={
            "last_seen": datetime.utcnow().isoformat(),
            "offline_reason": "heartbeat_timeout"
        })
        
        print(f"[PIN] Operator {operator_id} marked offline (heartbeat timeout)")
        self.evicted_count += 1
    
    def check_all_operators(self) -> int:
        """Check all online operators for stale heartbeats. Returns count of evicted."""
        online_operators = self.get_online_operators()
        now = time.time()
        evicted = 0
        
        for operator_id in online_operators:
            last_heartbeat = self.get_heartbeat_timestamp(operator_id)
            
            if last_heartbeat == 0.0:
                continue
            
            age_seconds = now - last_heartbeat
            
            if age_seconds > STALE_THRESHOLD_SECONDS:
                self.mark_operator_offline(operator_id)
                evicted += 1
        
        self.checked_count += len(online_operators)
        return evicted
    
    async def run(self):
        """Main worker loop."""
        self.running = True
        print(f"[PIN] Heartbeat worker started (check interval: {CHECK_INTERVAL_SECONDS}s, health check interval: {HEALTH_CHECK_INTERVAL_SECONDS}s)")
        
        loop_count = 0
        
        while self.running:
            try:
                evicted = self.check_all_operators()
                
                if evicted > 0:
                    print(f"[PIN] Evicted {evicted} stale operators")
                
                now = time.time()
                if now - self.last_health_check >= HEALTH_CHECK_INTERVAL_SECONDS:
                    self.last_health_check = now
                    passed, failed = await self.run_health_checks()
                    if passed > 0 or failed > 0:
                        print(f"[PIN] Health checks: {passed} passed, {failed} failed")
                
                loop_count += 1
                
            except Exception as e:
                print(f"[PIN] Heartbeat worker error: {e}")
            
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
    
    def stop(self):
        """Stop the worker."""
        self.running = False
        print(f"[PIN] Heartbeat worker stopped. Checked: {self.checked_count}, Evicted: {self.evicted_count}, Health checks: {self.health_checks_run}")


worker = PinHeartbeatWorker()


async def start_heartbeat_worker():
    """Start the heartbeat worker as a background task."""
    await worker.run()


def run_heartbeat_worker_sync():
    """Run the heartbeat worker synchronously (for standalone mode)."""
    asyncio.run(worker.run())


if __name__ == "__main__":
    print("[PIN] Starting heartbeat worker in standalone mode...")
    run_heartbeat_worker_sync()
