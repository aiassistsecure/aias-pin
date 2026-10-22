import json
import time
import secrets
import hashlib
import hmac
import httpx
import asyncio
import base64
import os
import random
from datetime import datetime
from typing import Optional, List, Dict
from fastapi import APIRouter, HTTPException, Depends, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response, StreamingResponse

from api.services import pin_operator_bus as bus
from cryptography.fernet import Fernet
from api.utils.pin_auth import (
    hash_api_secret,
    signature_from_hash,
    signature_from_secret,
    verify_signature,
)
from api.services.pin_sse_transport import transport as sse_transport

PIN_ENCRYPTION_KEY = "4ihWS7IX8gFJMY6N42c1UD7YN9K5VE9RUlnNUu430Ow="
_fernet = Fernet(PIN_ENCRYPTION_KEY.encode())

def encrypt_api_key(raw_key: str) -> str:
    return _fernet.encrypt(raw_key.encode()).decode()

def decrypt_api_key(encrypted_key: str) -> str:
    return _fernet.decrypt(encrypted_key.encode()).decode()

from api.models.schemas import (
    PinOperator, PinOperatorCreate, PinOperatorUpdate,
    PinOperatorRegistrationResponse, PinReputation, PinHeartbeat, PinHeartbeatResponse,
    PinJob, PinJobStatus, PinBillingUser, PinBillingOperator, PinTransaction,
    PinWithdrawalRequest, PinWithdrawal, PinWalletUpdate, PinOperatorEarnings, PinOperatorStatus,
    PinNetworkStatus, PinOperatorPublic, PinChatCompletionRequest,
    PinChatCompletionResponse, PinChatCompletionMetadata,
    OperatorStatus, OperatorRegion,
    ChatCompletionChoice, ChatCompletionUsage, ChatMessage, User
)
from api.services.redis_storage import storage

SESSION_COOKIE_NAME = "session_id"

def get_current_user_optional(request: Request) -> Optional[User]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        user = storage.get_session_user(token)
        if user:
            return user
    
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer aai_"):
        api_key_token = auth_header[7:]
        result = storage.get_api_key_by_secret(api_key_token)
        if result:
            return result[1]
    
    return None

router = APIRouter(prefix="/api/v1/pin", tags=["pin"])

PROTOCOL_FEE_PERCENT = 0.10
HEARTBEAT_TTL_SECONDS = 60
OPERATOR_TIMEOUT_SECONDS = 30

# Interview System Constants
INTERVIEW_TIMEOUT_SECONDS = 180
INTERVIEW_RETRY_COOLDOWN_HOURS = 1
INTERVIEW_MAX_ATTEMPTS_PER_DAY = 3

# Benchmark models required for interview - operator must have at least one
# These are 7B+ class models that establish real hardware capability
# How long a node waits after a failed interview before it may retry.
# Was 300s, which meant a config fix or a rebuild cost five minutes of
# staring at a socket that hangs up on arrival. The cooldown exists to stop
# reconnect loops, and 30s does that without punishing an operator who is
# actively iterating.
INTERVIEW_COOLDOWN_SECONDS = 30

# Models whose interview scores are comparable ACROSS nodes, because many
# nodes serve them. Preferred when a node has one — never REQUIRED. A node
# serving only its own custom models (muse-local, granite, gpt-oss, whatever
# ships next month) is interviewed on the first model it actually serves.
# muse-local:latest was briefly hardcoded here to stop that exact failure;
# the first-model fallback is the general fix, so first-party names no longer
# belong in a global list.
BENCHMARK_MODELS = [
    "llama3:8b", "llama3.1:8b", "llama3.2:8b", "llama3.3:8b",
    "mistral:7b", "mistral:latest",
    "qwen2:7b", "qwen2.5:7b",
    "gemma2:7b", "gemma:7b",
]

def find_benchmark_model(node_models: List[str]) -> Optional[str]:
    """Find a benchmark model from the node's available models."""
    if not node_models:
        return None
    for model in node_models:
        model_lower = model.lower()
        for benchmark in BENCHMARK_MODELS:
            if benchmark.lower() in model_lower or model_lower.startswith(benchmark.split(":")[0]):
                return model
    return None

# Quality Tier Thresholds (defaults, can be overridden via admin)
DEFAULT_TIER_THRESHOLDS = {
    "verified_accuracy": 90,
    "verified_tokens_per_sec": 20,
    "standard_accuracy": 70,
    "standard_tokens_per_sec": 10,
    "min_tokens_per_sec": 10,  # Below this = failed regardless of accuracy - quality floor
}

# OpenAI-mode thresholds - stricter requirements for vLLM/TGI/LocalAI operators
OPENAI_MODE_TIER_THRESHOLDS = {
    "verified_accuracy": 95,
    "verified_tokens_per_sec": 30,
    "standard_accuracy": 85,
    "standard_tokens_per_sec": 20,
    "min_tokens_per_sec": 15,
}

def get_tier_thresholds() -> dict:
    """Get tier thresholds from Redis or return defaults."""
    cached = storage.r.hgetall("aiconsult:pin:config:thresholds")
    if cached:
        return {
            "verified_accuracy": float(cached.get("verified_accuracy", DEFAULT_TIER_THRESHOLDS["verified_accuracy"])),
            "verified_tokens_per_sec": float(cached.get("verified_tokens_per_sec", DEFAULT_TIER_THRESHOLDS["verified_tokens_per_sec"])),
            "standard_accuracy": float(cached.get("standard_accuracy", DEFAULT_TIER_THRESHOLDS["standard_accuracy"])),
            "standard_tokens_per_sec": float(cached.get("standard_tokens_per_sec", DEFAULT_TIER_THRESHOLDS["standard_tokens_per_sec"])),
            "min_tokens_per_sec": float(cached.get("min_tokens_per_sec", DEFAULT_TIER_THRESHOLDS["min_tokens_per_sec"])),
        }
    return DEFAULT_TIER_THRESHOLDS.copy()

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# =============================================================================
# PIN Interview System - Quality Assurance for Operators
# =============================================================================

INTERVIEW_PROMPTS_POOL = [
    # Factual accuracy tests (known answers)
    {
        "id": "fact_capital_france",
        "prompt": "What is the capital of France? Answer with just the city name.",
        "expected_contains": ["Paris"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "factual"
    },
    {
        "id": "fact_water_formula",
        "prompt": "What is the chemical formula for water? Answer with just the formula.",
        "expected_contains": ["H2O"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "factual"
    },
    {
        "id": "fact_planets",
        "prompt": "How many planets are in our solar system? Answer with just the number.",
        "expected_contains": ["8"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "factual"
    },
    {
        "id": "fact_python_creator",
        "prompt": "Who created the Python programming language? Answer with just the name.",
        "expected_contains": ["Guido", "van Rossum"],
        "expected_exact": None,
        "max_tokens": 20,
        "category": "factual"
    },
    # Instruction following tests
    {
        "id": "instruct_count",
        "prompt": "Count from 1 to 5, with each number on a new line.",
        "expected_contains": ["1", "2", "3", "4", "5"],
        "expected_exact": None,
        "max_tokens": 30,
        "category": "instruction"
    },
    {
        "id": "instruct_reverse",
        "prompt": "Reverse the word 'hello'. Answer with just the reversed word.",
        "expected_contains": ["olleh"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "instruction"
    },
    {
        "id": "instruct_uppercase",
        "prompt": "Convert 'test' to uppercase. Answer with just the result.",
        "expected_contains": ["TEST"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "instruction"
    },
    # Simple math
    {
        "id": "math_add",
        "prompt": "What is 7 + 8? Answer with just the number.",
        "expected_contains": ["15"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "math"
    },
    {
        "id": "math_multiply",
        "prompt": "What is 6 x 7? Answer with just the number.",
        "expected_contains": ["42"],
        "expected_exact": None,
        "max_tokens": 10,
        "category": "math"
    },
    # Coherence test (should produce sensible output)
    {
        "id": "coherence_greeting",
        "prompt": "Say hello in a friendly way.",
        "expected_contains": None,
        "expected_exact": None,
        "max_tokens": 50,
        "category": "coherence"
    }
]

# OpenAI-mode interview prompts - harder multi-step reasoning tests
OPENAI_MODE_PROMPTS_POOL = [
    # Multi-step reasoning
    {
        "id": "reason_age",
        "prompt": "If Sarah is 3 times as old as Tom, and Tom is 8 years old, how old is Sarah? Show your reasoning briefly, then give the final answer.",
        "expected_contains": ["24"],
        "expected_exact": None,
        "max_tokens": 100,
        "category": "reasoning"
    },
    {
        "id": "reason_sequence",
        "prompt": "What is the next number in this sequence: 2, 6, 12, 20, 30, ? Explain why.",
        "expected_contains": ["42"],
        "expected_exact": None,
        "max_tokens": 100,
        "category": "reasoning"
    },
    {
        "id": "reason_logic",
        "prompt": "All roses are flowers. Some flowers fade quickly. Can we conclude that some roses fade quickly? Answer Yes or No and explain briefly.",
        "expected_contains": ["No"],
        "expected_exact": None,
        "max_tokens": 100,
        "category": "reasoning"
    },
    # Code generation
    {
        "id": "code_fibonacci",
        "prompt": "Write a Python function called 'fib' that returns the nth Fibonacci number. Just the function, no explanation.",
        "expected_contains": ["def fib", "return"],
        "expected_exact": None,
        "max_tokens": 150,
        "category": "code"
    },
    {
        "id": "code_reverse",
        "prompt": "Write a one-line Python expression to reverse a list called 'items'. Just the expression.",
        "expected_contains": ["[::-1]"],
        "expected_exact": None,
        "max_tokens": 50,
        "category": "code"
    },
    # Complex factual
    {
        "id": "fact_physics",
        "prompt": "What is the speed of light in meters per second? Give the approximate value.",
        "expected_contains": ["3", "10"],
        "expected_exact": None,
        "max_tokens": 30,
        "category": "factual"
    },
    {
        "id": "fact_math_constant",
        "prompt": "What is the value of Euler's number (e) to 2 decimal places?",
        "expected_contains": ["2.71", "2.72"],
        "expected_exact": None,
        "max_tokens": 20,
        "category": "factual"
    },
    # Instruction following (stricter)
    {
        "id": "instruct_json",
        "prompt": "Return a JSON object with keys 'name' and 'age' where name is 'Alice' and age is 30. Just the JSON, nothing else.",
        "expected_contains": ['"name"', '"age"', "Alice", "30"],
        "expected_exact": None,
        "max_tokens": 50,
        "category": "instruction"
    },
    {
        "id": "instruct_list",
        "prompt": "List exactly 3 programming languages, one per line, in alphabetical order. Start with a language beginning with 'C'.",
        "expected_contains": ["C"],
        "expected_exact": None,
        "max_tokens": 50,
        "category": "instruction"
    },
    # Complex math
    {
        "id": "math_percentage",
        "prompt": "If a product costs $80 after a 20% discount, what was the original price? Show calculation.",
        "expected_contains": ["100"],
        "expected_exact": None,
        "max_tokens": 100,
        "category": "math"
    },
    {
        "id": "math_algebra",
        "prompt": "Solve for x: 3x + 7 = 22. Show your steps.",
        "expected_contains": ["5"],
        "expected_exact": None,
        "max_tokens": 80,
        "category": "math"
    }
]


def generate_interview_prompts(count: int = 5, api_mode: str = "ollama") -> List[dict]:
    """Generate randomized interview prompts from the pool based on API mode."""
    if api_mode == "openai":
        # OpenAI mode gets harder prompts with more variety
        prompt_pool = OPENAI_MODE_PROMPTS_POOL
        categories = ["reasoning", "code", "factual", "instruction", "math"]
        # OpenAI mode gets 7 prompts by default for more rigorous testing
        count = max(count, 7)
    else:
        prompt_pool = INTERVIEW_PROMPTS_POOL
        categories = ["factual", "instruction", "math"]
    
    selected = []
    
    # Ensure at least one from each important category
    for cat in categories:
        pool = [p for p in prompt_pool if p["category"] == cat]
        if pool:
            selected.append(random.choice(pool))
    
    # Fill remaining slots randomly
    remaining = [p for p in prompt_pool if p not in selected]
    while len(selected) < count and remaining:
        choice = random.choice(remaining)
        selected.append(choice)
        remaining.remove(choice)
    
    random.shuffle(selected)
    return selected


def calculate_quality_tier(results: List[dict], api_mode: str = "ollama") -> dict:
    """
    Calculate operator quality tier from interview results.
    Returns tier info with metrics.
    
    api_mode: "ollama" uses standard thresholds, "openai" uses stricter thresholds
    """
    if not results:
        return {"tier": "failed", "accuracy": 0, "avg_tokens_per_sec": 0, "reason": "No results"}
    
    # Select prompt pool and thresholds based on API mode
    if api_mode == "openai":
        prompt_pool = OPENAI_MODE_PROMPTS_POOL + INTERVIEW_PROMPTS_POOL  # Check both pools
        thresholds = OPENAI_MODE_TIER_THRESHOLDS
    else:
        prompt_pool = INTERVIEW_PROMPTS_POOL
        thresholds = get_tier_thresholds()
    
    total_prompts = len(results)
    correct = 0
    total_tokens_per_sec = 0
    total_ttft = 0
    valid_speed_samples = 0
    
    for r in results:
        if r.get("error"):
            continue
        
        prompt_id = r.get("prompt_id", "")
        response = r.get("response", "").strip()
        tokens_generated = r.get("tokens_generated", 0)
        total_ms = r.get("total_ms", 0)
        ttft_ms = r.get("ttft_ms", 0)
        
        # Find original prompt to check expected values
        original = next((p for p in prompt_pool if p["id"] == prompt_id), None)
        if not original:
            continue
        
        # Check accuracy - always require ALL expected substrings
        is_correct = False
        if original.get("expected_contains"):
            is_correct = all(exp.lower() in response.lower() for exp in original["expected_contains"])
        elif original.get("expected_exact"):
            is_correct = response.lower().strip() == original["expected_exact"].lower().strip()
        else:
            # Coherence test - just check it's not empty gibberish
            is_correct = len(response) > 3 and any(c.isalpha() for c in response)
        
        if is_correct:
            correct += 1
        
        # Calculate speed metrics
        if total_ms > 0 and tokens_generated > 0:
            tps = (tokens_generated / total_ms) * 1000
            total_tokens_per_sec += tps
            total_ttft += ttft_ms
            valid_speed_samples += 1
    
    accuracy = (correct / total_prompts) * 100 if total_prompts > 0 else 0
    avg_tps = total_tokens_per_sec / valid_speed_samples if valid_speed_samples > 0 else 0
    avg_ttft = total_ttft / valid_speed_samples if valid_speed_samples > 0 else 0
    
    # Determine tier - check minimum speed first
    mode_label = "(OpenAI mode)" if api_mode == "openai" else ""
    if avg_tps < thresholds["min_tokens_per_sec"]:
        tier = "failed"
        reason = f"Speed too low ({avg_tps:.1f} tok/s, minimum {thresholds['min_tokens_per_sec']:.0f} required) {mode_label}"
    elif accuracy < thresholds["standard_accuracy"]:
        tier = "failed"
        reason = f"Accuracy too low ({accuracy:.0f}%, minimum {thresholds['standard_accuracy']:.0f}% required) {mode_label}"
    elif accuracy >= thresholds["verified_accuracy"] and avg_tps >= thresholds["verified_tokens_per_sec"]:
        tier = "verified"
        reason = f"High accuracy ({accuracy:.0f}%) and speed ({avg_tps:.1f} tok/s) {mode_label}"
    elif accuracy >= thresholds["standard_accuracy"] and avg_tps >= thresholds["standard_tokens_per_sec"]:
        tier = "standard"
        reason = f"Good accuracy ({accuracy:.0f}%) and adequate speed ({avg_tps:.1f} tok/s) {mode_label}"
    else:
        tier = "slow"
        reason = f"Good accuracy ({accuracy:.0f}%) but slow ({avg_tps:.1f} tok/s) {mode_label}"
    
    return {
        "tier": tier,
        "accuracy": round(accuracy, 1),
        "avg_tokens_per_sec": round(avg_tps, 2),
        "avg_ttft_ms": round(avg_ttft, 0),
        "correct": correct,
        "total": total_prompts,
        "reason": reason.strip(),
        "api_mode": api_mode
    }




def get_current_user_required(request: Request) -> User:
    user = get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


from pydantic import BaseModel as PydanticBaseModel


class OperatorMeResponse(PydanticBaseModel):
    has_operator: bool
    operator_id: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    endpoint: Optional[str] = None
    models: Optional[List[str]] = None
    region: Optional[str] = None
    pricing_per_1k_tokens: Optional[float] = None


@router.get("/operator/me", response_model=OperatorMeResponse)
async def get_my_operator(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        return OperatorMeResponse(has_operator=False)
    return OperatorMeResponse(
        has_operator=True,
        operator_id=operator.id,
        name=operator.name,
        status=operator.status.value,
        endpoint=operator.endpoint,
        models=operator.models,
        region=operator.region.value,
        pricing_per_1k_tokens=operator.pricing_per_1k_tokens
    )


@router.get("/operator/me/nodes")
async def get_my_nodes(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    nodes = storage.pin_list_operator_nodes(operator.id)
    stats = storage.pin_get_operator_node_stats(operator.id)
    return {"operator_id": operator.id, "nodes": nodes, "stats": stats}


@router.post("/operator/me/nodes")
async def create_my_node(
    data: "NodeCreateRequest",
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    if not data.alias or len(data.alias) < 2:
        raise HTTPException(status_code=400, detail="Node alias must be at least 2 characters")
    if not data.endpoint.startswith("http"):
        raise HTTPException(status_code=400, detail="Invalid endpoint URL")
    if data.capacity < 1 or data.capacity > 10000:
        raise HTTPException(status_code=400, detail="Capacity must be between 1 and 10000")
    if not data.models:
        raise HTTPException(status_code=400, detail="At least one model required")
    
    existing = storage.pin_check_endpoint_exists(data.endpoint, exclude_operator_id=operator.id)
    if existing:
        if existing.get("operator_id") == operator.id:
            raise HTTPException(status_code=409, detail=f"You already have a node '{existing['alias']}' with this endpoint.")
        else:
            raise HTTPException(status_code=409, detail="This endpoint is already registered by another operator.")
    
    node = storage.pin_create_node(
        operator_id=operator.id,
        alias=data.alias,
        endpoint=data.endpoint,
        models=data.models,
        capacity=data.capacity,
        region=data.region
    )
    return {"success": True, "node": node, "message": "Node created successfully."}


@router.get("/operator/me/health")
async def get_my_health(request: Request, user: User = Depends(get_current_user_required)):
    """Get current health check status for operator."""
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    health_key = f"aiconsult:pin:health:{operator.id}"
    health_data = storage.r.hgetall(health_key)
    is_online = storage.pin_is_operator_online(operator.id)
    return {
        "operator_id": operator.id,
        "online": is_online,
        "health_status": health_data.get("status", "unknown"),
        "health_method": health_data.get("method", "unknown"),
        "last_check": health_data.get("last_check"),
        "latency_ms": int(health_data.get("latency_ms", 0)) if health_data.get("latency_ms") else None,
        "error": health_data.get("error")
    }


@router.post("/operator/me/reputation/reset")
async def reset_my_reputation(request: Request, user: User = Depends(get_current_user_required)):
    """Reset reputation metrics to defaults (for new operators or fresh start)."""
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    rep_key = f"aiconsult:pin:reputation:{operator.id}"
    storage.r.hset(rep_key, mapping={
        "score": "1.0",
        "total_requests": "0",
        "success_rate": "1.0",
        "avg_latency_ms": "0"
    })
    return {"success": True, "message": "Reputation reset to defaults"}


@router.get("/operator/me/status")
async def get_my_status(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    reputation = storage.pin_get_reputation(operator.id)
    is_online = storage.pin_is_operator_online(operator.id)
    billing = storage.pin_get_operator_billing(operator.id)
    health_key = f"aiconsult:pin:health:{operator.id}"
    health_data = storage.r.hgetall(health_key)
    health_status = health_data.get("status", "unknown")
    health_score = 1.0 if health_status == "healthy" else 0.0 if health_status in ["unhealthy", "timeout", "error"] else 0.5
    rep_score = reputation.score if reputation else health_score
    if is_online and health_status == "healthy" and rep_score < 0.5:
        rep_score = max(rep_score, 0.8)
    return PinOperatorStatus(
        operator_id=operator.id,
        status=operator.status,
        online=is_online,
        reputation_score=rep_score,
        total_requests=reputation.total_requests if reputation else 0,
        success_rate=reputation.success_rate if reputation else 1.0,
        avg_latency_ms=reputation.avg_latency_ms if reputation else 0.0,
        earnings_balance=billing.earnings_balance if billing else 0.0,
        total_earned=billing.total_earned if billing else 0.0
    )


@router.get("/operator/me/earnings")
async def get_my_earnings(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    billing = storage.pin_get_operator_billing(operator.id)
    earnings_24h = storage.pin_get_operator_earnings_period(operator.id, 24)
    earnings_7d = storage.pin_get_operator_earnings_period(operator.id, 168)
    return PinOperatorEarnings(
        balance=billing.earnings_balance if billing else 0.0,
        pending_payout=0.0,
        total_earned=billing.total_earned if billing else 0.0,
        total_paid_out=billing.total_paid_out if billing else 0.0,
        earnings_24h=earnings_24h,
        earnings_7d=earnings_7d
    )


@router.get("/operator/me/interview")
async def get_my_interview_status(request: Request, user: User = Depends(get_current_user_required)):
    """Get current interview/quality tier status for operator."""
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    operator_key = f"aiconsult:pin:operators:{operator.id}"
    interview_key = f"aiconsult:pin:interview:{operator.id}"
    
    op_data = storage.r.hgetall(operator_key)
    interview_data = storage.r.hgetall(interview_key)
    
    # Check operator-level tier first
    tier = op_data.get("quality_tier", "pending")
    accuracy = float(op_data.get("interview_accuracy", 0))
    tokens_per_sec = float(op_data.get("interview_tokens_per_sec", 0))
    ttft_ms = float(op_data.get("interview_ttft_ms", 0))
    last_interview = op_data.get("last_interview")
    interview_model = op_data.get("interview_model")
    
    # If still pending, check node-level tier data (interview results stored on node)
    if tier == "pending":
        nodes = storage.pin_list_operator_nodes(operator.id)
        for node in nodes:
            node_id = node.get("id") if isinstance(node, dict) else node.id
            node_key = f"aiconsult:pin:nodes:{node_id}"
            node_data = storage.r.hgetall(node_key)
            node_tier = node_data.get("quality_tier")
            if node_tier and node_tier != "pending":
                tier = node_tier
                accuracy = float(node_data.get("interview_accuracy", 0))
                tokens_per_sec = float(node_data.get("interview_tokens_per_sec", 0))
                ttft_ms = float(node_data.get("interview_ttft_ms", 0))
                last_interview = node_data.get("last_interview")
                interview_model = node_data.get("interview_model")
                # Copy to operator level for consistency
                storage.r.hset(operator_key, mapping={
                    "quality_tier": tier,
                    "interview_accuracy": str(accuracy),
                    "interview_tokens_per_sec": str(tokens_per_sec),
                    "interview_ttft_ms": str(ttft_ms),
                    "last_interview": last_interview or "",
                    "interview_model": interview_model or ""
                })
                break
    
    attempts = int(interview_data.get("attempts", 0))
    last_attempt = interview_data.get("last_attempt")
    
    # Get current thresholds for descriptions
    thresholds = get_tier_thresholds()
    
    # Check if eligible for re-interview
    can_retry = True
    retry_message = None
    
    if tier == "failed" and attempts >= INTERVIEW_MAX_ATTEMPTS_PER_DAY:
        can_retry = False
        retry_message = f"Maximum {INTERVIEW_MAX_ATTEMPTS_PER_DAY} attempts per day reached"
    elif last_attempt:
        try:
            last_ts = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
            cooldown_end = last_ts.timestamp() + (INTERVIEW_RETRY_COOLDOWN_HOURS * 3600)
            if time.time() < cooldown_end:
                can_retry = False
                remaining = int(cooldown_end - time.time())
                retry_message = f"Cooldown: retry available in {remaining // 60} minutes"
        except:
            pass
    
    return {
        "operator_id": operator.id,
        "quality_tier": tier,
        "accuracy": accuracy,
        "tokens_per_sec": tokens_per_sec,
        "ttft_ms": ttft_ms,
        "last_interview": last_interview,
        "interview_model": interview_model,
        "attempts_today": attempts,
        "can_retry": can_retry,
        "retry_message": retry_message,
        "tier_descriptions": {
            "verified": f"High accuracy ({thresholds['verified_accuracy']:.0f}%+) and speed ({thresholds['verified_tokens_per_sec']:.0f}+ tok/s) - highest routing priority",
            "standard": f"Good accuracy ({thresholds['standard_accuracy']:.0f}%+) and adequate speed ({thresholds['standard_tokens_per_sec']:.0f}+ tok/s) - normal priority",
            "slow": f"Good accuracy but slow speed - low priority (budget tier)",
            "failed": f"Below minimum requirements ({thresholds['min_tokens_per_sec']:.0f} tok/s or {thresholds['standard_accuracy']:.0f}% accuracy) - blocked from production traffic",
            "pending": "Interview not yet completed"
        },
        "thresholds": thresholds
    }


@router.post("/operator/me/interview/request")
async def request_reinterview(request: Request, user: User = Depends(get_current_user_required)):
    """Request a new interview to improve quality tier."""
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    interview_key = f"aiconsult:pin:interview:{operator.id}"
    operator_key = f"aiconsult:pin:operators:{operator.id}"
    
    interview_data = storage.r.hgetall(interview_key)
    attempts = int(interview_data.get("attempts", 0))
    last_attempt = interview_data.get("last_attempt")
    
    # Check cooldown
    if last_attempt:
        try:
            last_ts = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
            cooldown_end = last_ts.timestamp() + (INTERVIEW_RETRY_COOLDOWN_HOURS * 3600)
            if time.time() < cooldown_end:
                remaining = int(cooldown_end - time.time())
                raise HTTPException(
                    status_code=429,
                    detail=f"Cooldown period. Retry available in {remaining // 60} minutes."
                )
        except HTTPException:
            raise
        except:
            pass
    
    # Check daily attempts
    if attempts >= INTERVIEW_MAX_ATTEMPTS_PER_DAY:
        raise HTTPException(
            status_code=429,
            detail=f"Maximum {INTERVIEW_MAX_ATTEMPTS_PER_DAY} attempts per day reached. Try again tomorrow."
        )
    
    # Check if operator is online
    if not pin_ws_manager.is_operator_online(operator.id):
        raise HTTPException(
            status_code=400,
            detail="Your daemon must be connected to request an interview. Start your daemon first."
        )
    
    # Get models and api_mode from operator's nodes
    models_json = storage.r.hget(operator_key, "models")
    models = json.loads(models_json) if models_json else []
    
    # Check operator's nodes for api_mode (use first node's mode)
    nodes = storage.pin_get_operator_nodes(operator.id)
    node_api_mode = "ollama"
    target_node_id = None
    if nodes:
        for node in nodes:
            node_data = storage.r.hgetall(f"aiconsult:pin:nodes:{node.get('id')}")
            if node_data:
                node_api_mode = node_data.get("api_mode", "ollama")
                target_node_id = node.get("id")
                node_models = json.loads(node_data.get("models", "[]")) if node_data.get("models") else []
                if node_models:
                    models = node_models
                break
    
    if not models:
        raise HTTPException(
            status_code=400,
            detail="No models registered. Connect your daemon and wait for models to be reported."
        )
    
    # Select interview model based on API mode
    if node_api_mode == "openai":
        # OpenAI mode: use first model, no benchmark requirement
        interview_model = models[0]
    else:
        # Ollama mode: prefer a benchmark model when one exists (scores stay
        # comparable across nodes), but its absence is not fatal. Operators
        # run their own models and those change all the time; requiring one
        # of four frozen names meant every custom fleet had to hand-pin
        # interviewModel in config or fail. First advertised model is the
        # honest fallback: interview what the node actually serves.
        interview_model = find_benchmark_model(models) or models[0]
    
    # Generate prompts based on API mode
    prompts = generate_interview_prompts(5, api_mode=node_api_mode)
    interview_id = f"interview_{secrets.token_urlsafe(16)}"
    
    # Store api_mode for result handling
    if target_node_id:
        pin_ws_manager.node_interviews[interview_id] = target_node_id
    pin_ws_manager.interview_api_modes[interview_id] = node_api_mode
    
    ws = pin_ws_manager.get_websocket_for_operator(operator.id)
    if ws:
        await ws.send_json({
            "type": "INTERVIEW_REQUEST",
            "interview_id": interview_id,
            "node_id": target_node_id,
            "model": interview_model,
            "api_mode": node_api_mode,
            "prompts": [
                {
                    "id": p["id"],
                    "prompt": p["prompt"],
                    "max_tokens": p["max_tokens"]
                } for p in prompts
            ],
            "timeout_ms": INTERVIEW_TIMEOUT_SECONDS * 1000
        })
        
        return {
            "success": True,
            "interview_id": interview_id,
            "model": interview_model,
            "prompts_count": len(prompts),
            "message": "Interview request sent to your daemon. Results will be processed automatically."
        }
    else:
        raise HTTPException(
            status_code=503,
            detail="WebSocket connection lost. Reconnect your daemon and try again."
        )


@router.get("/operator/me/wallet")
async def get_my_wallet(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    # Check billing key first (legacy/manual updates)
    billing_key = f"aiconsult:pin:billing:operators:{operator.id}"
    wallet_address = storage.r.hget(billing_key, "wallet_address")
    
    # Fall back to operator's payout_address (set during registration or daemon UPDATE_WALLET)
    if not wallet_address:
        operator_key = f"aiconsult:pin:operators:{operator.id}"
        wallet_address = storage.r.hget(operator_key, "payout_address")
    
    return {"operator_id": operator.id, "bsc_address": wallet_address}


@router.put("/operator/me/wallet")
async def update_my_wallet(
    data: PinWalletUpdate,
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    # Store in both locations for compatibility
    billing_key = f"aiconsult:pin:billing:operators:{operator.id}"
    operator_key = f"aiconsult:pin:operators:{operator.id}"
    storage.r.hset(billing_key, "wallet_address", data.wallet_address)
    storage.r.hset(operator_key, "payout_address", data.wallet_address)
    storage.r.hset(operator_key, "updated_at", now_iso())
    
    return {"success": True, "bsc_address": data.wallet_address}


class PricingUpdate(PydanticBaseModel):
    pricing_per_1k_tokens: float


@router.get("/operator/me/pricing")
async def get_my_pricing(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    return {"operator_id": operator.id, "pricing_per_1k_tokens": operator.pricing_per_1k_tokens}


@router.put("/operator/me/pricing")
async def update_my_pricing(
    data: PricingUpdate,
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    if data.pricing_per_1k_tokens < 0:
        raise HTTPException(status_code=400, detail="Price cannot be negative")
    if data.pricing_per_1k_tokens > 100:
        raise HTTPException(status_code=400, detail="Price cannot exceed $100 per 1K tokens")
    
    operator_key = f"aiconsult:pin:operators:{operator.id}"
    storage.r.hset(operator_key, "pricing_per_1k_tokens", str(data.pricing_per_1k_tokens))
    storage.r.hset(operator_key, "updated_at", now_iso())
    
    return {"success": True, "pricing_per_1k_tokens": data.pricing_per_1k_tokens}


@router.get("/operator/me/withdrawals")
async def get_my_withdrawals(request: Request, user: User = Depends(get_current_user_required)):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    withdrawals_raw = storage.r.lrange(f"aiconsult:pin:withdrawals:{operator.id}", 0, -1)
    withdrawals = [json.loads(w) for w in withdrawals_raw]
    return {"operator_id": operator.id, "withdrawals": withdrawals}


@router.post("/operator/me/withdrawals")
async def request_my_withdrawal(
    data: PinWithdrawalRequest,
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    billing = storage.pin_get_operator_billing(operator.id)
    if not billing or billing.earnings_balance < data.amount:
        raise HTTPException(status_code=400, detail="Insufficient balance")
    if data.amount < 10.0:
        raise HTTPException(status_code=400, detail="Minimum withdrawal is $10")
    billing_key = f"aiconsult:pin:billing:operators:{operator.id}"
    wallet_address = storage.r.hget(billing_key, "wallet_address")
    if not wallet_address:
        raise HTTPException(status_code=400, detail="Set wallet address first")
    
    result = storage.pin_create_withdrawal_atomic(operator.id, data.amount, "usdt_bsc")
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.patch("/operator/me/nodes/{node_id}")
async def update_my_node(
    node_id: str,
    data: "NodeUpdateRequest",
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    node = storage.pin_get_node(node_id)
    if not node or node["operator_id"] != operator.id:
        raise HTTPException(status_code=404, detail="Node not found")
    
    updates = {k: v for k, v in data.dict().items() if v is not None}
    updated_node = storage.pin_update_node(node_id, updates)
    return {"success": True, "node": updated_node}


@router.delete("/operator/me/nodes/{node_id}")
async def delete_my_node(
    node_id: str,
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    node = storage.pin_get_node(node_id)
    if not node or node["operator_id"] != operator.id:
        raise HTTPException(status_code=404, detail="Node not found")
    
    storage.pin_delete_node(node_id)
    return {"success": True, "message": "Node deleted"}


@router.post("/operator/me/nodes/{node_id}/regenerate-credentials")
async def regenerate_my_node_credentials(
    node_id: str,
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    node = storage.pin_get_node(node_id)
    if not node or node["operator_id"] != operator.id:
        raise HTTPException(status_code=404, detail="Node not found")
    
    result = storage.pin_regenerate_node_credentials(node_id)
    if not result:
        raise HTTPException(status_code=500, detail="Failed to regenerate credentials")
    return {"success": True, "client_id": result["client_id"], "api_key": result["api_key"]}


@router.get("/operator/me/api-key")
async def get_my_operator_api_key(
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    encrypted_key = storage.r.hget(f"aiconsult:pin:operators:{operator.id}", "api_key_encrypted")
    if not encrypted_key:
        raise HTTPException(status_code=404, detail="API key not found - please regenerate")
    
    try:
        api_key = decrypt_api_key(encrypted_key)
        # client_id is what pin-clientd actually signs with; after a rotation
        # it is NOT the operator id, so the UI has to show the stored value.
        client_id = _pin_text(
            storage.r.hget(f"aiconsult:pin:operators:{operator.id}", "client_id")
        ) or operator.id
        return {"operator_id": operator.id, "client_id": client_id,
                "api_key": api_key}
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to decrypt API key - please regenerate")


@router.post("/operator/me/regenerate-api-key")
async def regenerate_my_operator_api_key(
    request: Request,
    user: User = Depends(get_current_user_required)
):
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")
    
    raw_key, key_hash = generate_operator_api_key()
    api_key_encrypted = encrypt_api_key(raw_key)
    storage.r.hset(f"aiconsult:pin:operators:{operator.id}", mapping={
        "api_key_hash": key_hash,
        "api_key_encrypted": api_key_encrypted
    })
    
    return {
        "success": True,
        "operator_id": operator.id,
        "api_key": raw_key,
        "message": "API key regenerated successfully."
    }


@router.post("/operator/me/rotate-client-id")
async def rotate_my_operator_client_id(
    request: Request,
    user: User = Depends(get_current_user_required)
):
    """Issue a brand-new client_id AND api_secret for this operator.

    Regenerating the API key alone does not shake off a stale daemon: it keeps
    dialing in under the SAME client_id, fails AUTH on a loop, and -- because
    interview verdicts and offline evictions are recorded against the operator
    -- can leave the legitimate operator tombstoned as `failed`. Rotating the
    identity itself means the ghost has nothing left to attach to.

    Old credentials are invalidated immediately and any socket currently open
    for this operator is closed, so the next connection must use the new pair.
    """
    operator = storage.pin_get_operator_by_user(user.id)
    if not operator:
        raise HTTPException(status_code=404, detail="No operator account found")

    operator_key = f"aiconsult:pin:operators:{operator.id}"
    old_data = storage.r.hgetall(operator_key) or {}
    old_client_id = _pin_text(
        old_data.get(b"client_id") or old_data.get("client_id"))

    new_client_id = generate_client_id()
    raw_key, key_hash = generate_operator_api_key()

    updates = {
        "client_id": new_client_id,
        "api_key_hash": key_hash,
        "api_key_encrypted": encrypt_api_key(raw_key),
        "credentials_rotated_at": now_iso(),
        "offline_reason": "",
    }

    # A `failed` operator tier is written by interview results and never
    # cleared by a later pass (the node->operator copy is gated on
    # tier == "pending"). If any node of this operator is currently healthy,
    # that tombstone is stale -- clear it on rotation so a fresh identity does
    # not inherit a ban earned by a stale daemon.
    old_tier = _pin_text(
        old_data.get(b"quality_tier") or old_data.get("quality_tier"))
    if old_tier == "failed":
        best = None
        try:
            for node in storage.pin_list_operator_nodes(operator.id) or []:
                nid = node.get("id") if isinstance(node, dict) else getattr(node, "id", None)
                if not nid:
                    continue
                nd = storage.r.hgetall(f"aiconsult:pin:nodes:{nid}") or {}
                tier = nd.get("quality_tier")
                if tier in ("verified", "standard", "slow"):
                    best = tier
                    break
        except Exception as exc:  # noqa: BLE001
            print(f"[PIN ROTATE] node tier lookup failed: {exc}")
        if best:
            updates["quality_tier"] = best
            print(f"[PIN ROTATE] Cleared stale 'failed' tier on {operator.id} "
                  f"-> '{best}' (from node)")

    storage.r.hset(operator_key, mapping=updates)

    # Drop any live socket bound to this operator: it is authenticated against
    # credentials that no longer exist.
    closed = 0
    for cid, oid in list(pin_ws_manager.operator_map.items()):
        if oid != operator.id:
            continue
        ws = pin_ws_manager.active_connections.get(cid)
        pin_ws_manager.disconnect(cid)
        closed += 1
        if ws is not None:
            try:
                await ws.close(code=4001, reason="credentials rotated")
            except Exception:  # noqa: BLE001 -- already gone is fine
                pass

    print(f"[PIN ROTATE] operator={operator.id} client_id {old_client_id!r} "
          f"-> {new_client_id!r}, closed {closed} socket(s)")

    return {
        "success": True,
        "operator_id": operator.id,
        "client_id": new_client_id,
        "api_key": raw_key,
        "previous_client_id": old_client_id or None,
        "sockets_closed": closed,
        "message": (
            "Client ID and API secret rotated. Update config.json on every "
            "machine running pin-clientd and restart it -- any daemon still "
            "holding the old pair will now fail AUTH permanently."),
    }


def generate_operator_api_key() -> tuple[str, str]:
    raw_key = f"pin_sk_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    return raw_key, key_hash


def verify_operator_api_key(api_key: str, stored_hash: str) -> bool:
    computed_hash = hashlib.sha256(api_key.encode()).hexdigest()
    return computed_hash == stored_hash


async def get_operator_from_api_key(authorization: str = Header(None)) -> Optional[PinOperator]:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    api_key = authorization[7:]
    if not api_key.startswith("pin_sk_"):
        return None
    operator = storage.pin_get_operator_by_api_key(api_key)
    return operator


# =============================================================================
# Operator Management Routes
# =============================================================================

class VerifyEndpointRequest(PydanticBaseModel):
    endpoint: str

class VerifyEndpointResponse(PydanticBaseModel):
    success: bool
    ollama_version: Optional[str] = None
    models: Optional[List[dict]] = None
    error: Optional[str] = None

@router.post("/operators/verify-endpoint", response_model=VerifyEndpointResponse)
async def verify_ollama_endpoint(
    data: VerifyEndpointRequest,
    user: User = Depends(get_current_user_optional)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    endpoint = data.endpoint.rstrip("/")
    
    if not endpoint.startswith("http"):
        return VerifyEndpointResponse(success=False, error="Invalid endpoint URL")
    
    import httpx
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            version_res = await client.get(f"{endpoint}/api/version")
            if version_res.status_code != 200:
                return VerifyEndpointResponse(success=False, error="Could not reach Ollama API")
            
            version_data = version_res.json()
            ollama_version = version_data.get("version", "unknown")
            
            tags_res = await client.get(f"{endpoint}/api/tags")
            if tags_res.status_code != 200:
                return VerifyEndpointResponse(success=False, error="Could not fetch models from Ollama")
            
            tags_data = tags_res.json()
            models = tags_data.get("models", [])
            
            if not models:
                return VerifyEndpointResponse(
                    success=False, 
                    ollama_version=ollama_version,
                    error="No models found on this Ollama instance. Please pull at least one model."
                )
            
            model_list = [
                {
                    "name": m.get("name", ""),
                    "size": m.get("size", 0),
                    "modified_at": m.get("modified_at", "")
                }
                for m in models
            ]
            
            return VerifyEndpointResponse(
                success=True,
                ollama_version=ollama_version,
                models=model_list
            )
            
    except httpx.TimeoutException:
        return VerifyEndpointResponse(success=False, error="Connection timed out. Is your endpoint accessible?")
    except httpx.ConnectError:
        return VerifyEndpointResponse(success=False, error="Could not connect to endpoint. Check the URL and firewall settings.")
    except Exception as e:
        return VerifyEndpointResponse(success=False, error=f"Verification failed: {str(e)}")


class DaemonNodeConfig(PydanticBaseModel):
    alias: str
    ollama_url: str
    models: Optional[List[str]] = None
    capacity: int = 10
    region: str = "global"


class DaemonRegisterRequest(PydanticBaseModel):
    nodes: List[DaemonNodeConfig]


class DaemonNodeCredentials(PydanticBaseModel):
    node_id: str
    alias: str
    client_id: str
    api_key: str
    status: str


class DaemonRegisterResponse(PydanticBaseModel):
    success: bool
    operator_id: str
    nodes: List[DaemonNodeCredentials]
    server_url: str


@router.post("/daemon/register", response_model=DaemonRegisterResponse)
async def daemon_register_nodes(
    data: DaemonRegisterRequest,
    authorization: str = Header(None)
):
    """
    Smart node registration for PIN Client Daemon.
    - Authenticates via operator API key (Bearer token)
    - Auto-creates nodes if they don't exist (by alias match)
    - Returns credentials for all nodes
    """
    operator = await get_operator_from_api_key(authorization)
    if not operator:
        raise HTTPException(status_code=401, detail="Invalid or missing API key. Use: Authorization: Bearer pin_sk_xxx")
    
    result_nodes = []
    existing_nodes = storage.pin_list_operator_nodes(operator.id)
    existing_by_alias = {n["alias"]: n for n in existing_nodes}
    
    for node_config in data.nodes:
        existing = existing_by_alias.get(node_config.alias)
        
        if existing:
            encrypted_key = storage.r.hget(f"aiconsult:pin:nodes:{existing['id']}", "api_key_encrypted")
            if encrypted_key:
                try:
                    api_key = decrypt_api_key(encrypted_key)
                except Exception:
                    creds = storage.pin_regenerate_node_credentials(existing["id"])
                    api_key = creds["api_key"] if creds else ""
            else:
                creds = storage.pin_regenerate_node_credentials(existing["id"])
                api_key = creds["api_key"] if creds else ""
            
            if node_config.models:
                storage.pin_update_node(existing["id"], {"models": node_config.models})
            
            result_nodes.append(DaemonNodeCredentials(
                node_id=existing["id"],
                alias=existing["alias"],
                client_id=existing["client_id"],
                api_key=api_key,
                status="existing"
            ))
        else:
            models = node_config.models or []
            if not models:
                import httpx
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        res = await client.get(f"{node_config.ollama_url.rstrip('/')}/api/tags")
                        if res.status_code == 200:
                            models = [m["name"] for m in res.json().get("models", [])]
                except Exception:
                    pass
            
            if not models:
                models = ["auto-detect"]
            
            new_node = storage.pin_create_node(
                operator_id=operator.id,
                alias=node_config.alias,
                endpoint=node_config.ollama_url,
                models=models,
                capacity=node_config.capacity,
                region=node_config.region
            )
            
            raw_key = new_node.get("api_key", "")
            encrypted = encrypt_api_key(raw_key)
            storage.r.hset(f"aiconsult:pin:nodes:{new_node['node_id']}", "api_key_encrypted", encrypted)
            
            result_nodes.append(DaemonNodeCredentials(
                node_id=new_node["node_id"],
                alias=new_node["alias"],
                client_id=new_node["client_id"],
                api_key=raw_key,
                status="created"
            ))
    
    return DaemonRegisterResponse(
        success=True,
        operator_id=operator.id,
        nodes=result_nodes,
        server_url="wss://aiassist-secure.replit.app/api/v1/pin/ws"
    )


def generate_client_id() -> str:
    return f"pin_cid_{secrets.token_urlsafe(16)}"


@router.post("/operators/register", response_model=PinOperatorRegistrationResponse)
async def register_operator(
    data: PinOperatorCreate,
    user: User = Depends(get_current_user_optional)
):
    """
    Register as a PIN operator.
    
    WebSocket-Only Mode (Recommended - Secure):
    - Don't provide endpoint, just name + models + pricing
    - Your Ollama stays on localhost, never exposed publicly
    - Run the PIN Clientd daemon to connect via WebSocket
    - All inference flows through the secure WebSocket tunnel
    
    HTTP Mode (Legacy):
    - Provide a public endpoint URL
    - AiAS will call your endpoint directly (less secure)
    """
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    if not data.name or len(data.name) < 3:
        raise HTTPException(status_code=400, detail="Operator name must be at least 3 characters")
    
    if data.capacity < 1 or data.capacity > 10000:
        raise HTTPException(status_code=400, detail="Capacity must be between 1 and 10000")
    
    if data.pricing_per_1k_tokens < 0.0001 or data.pricing_per_1k_tokens > 100:
        raise HTTPException(status_code=400, detail="Pricing must be between $0.0001 and $100 per 1K tokens")
    
    connection_mode = "websocket"
    
    if data.endpoint:
        if not data.endpoint.startswith("http"):
            raise HTTPException(status_code=400, detail="Invalid endpoint URL (must start with http)")
        existing = storage.pin_check_endpoint_exists(data.endpoint)
        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"This endpoint is already registered. Each endpoint must be unique across the network."
            )
        connection_mode = "http"
    
    raw_key, key_hash = generate_operator_api_key()
    client_id = generate_client_id()
    api_key_encrypted = encrypt_api_key(raw_key)
    
    operator = storage.pin_create_operator(
        user_id=user.id,
        name=data.name,
        endpoint=data.endpoint,
        models=data.models if data.models else [],
        capacity=data.capacity,
        pricing_per_1k_tokens=data.pricing_per_1k_tokens,
        region=data.region.value,
        api_key_hash=key_hash,
        client_id=client_id,
        api_key_encrypted=api_key_encrypted,
        connection_mode=connection_mode,
        payout_address=data.payout_address
    )
    
    if data.endpoint and data.models:
        first_node = storage.pin_create_node(
            operator_id=operator.id,
            alias=f"{data.name} - Primary",
            endpoint=data.endpoint,
            models=data.models,
            capacity=data.capacity,
            region=data.region.value
        )
        storage.pin_set_primary_node(operator.id, first_node["node_id"])
        message = "Registration received. Your endpoint is configured for direct HTTP mode."
    else:
        message = "Registration received. Run the PIN Clientd daemon to connect your node securely via WebSocket."
    
    return PinOperatorRegistrationResponse(
        operator_id=operator.id,
        api_key=raw_key,
        client_id=client_id,
        status=operator.status,
        message=message
    )


@router.post("/operators/heartbeat", response_model=PinHeartbeatResponse)
async def operator_heartbeat(
    data: PinHeartbeat,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator:
        raise HTTPException(status_code=401, detail="Invalid operator API key")
    
    if operator.id != data.operator_id:
        raise HTTPException(status_code=403, detail="Operator ID mismatch")
    
    storage.pin_record_heartbeat(
        operator_id=operator.id,
        current_load=data.current_load,
        capacity=data.capacity,
        models_available=data.models_available
    )
    
    return PinHeartbeatResponse(acknowledged=True, next_heartbeat_due=30)


# =============================================================================
# Multi-Node Management Routes
# =============================================================================

class NodeCreateRequest(PydanticBaseModel):
    alias: str
    endpoint: str
    models: List[str]
    capacity: int = 10
    region: str = "global"

class NodeUpdateRequest(PydanticBaseModel):
    alias: Optional[str] = None
    endpoint: Optional[str] = None
    models: Optional[List[str]] = None
    capacity: Optional[int] = None
    region: Optional[str] = None
    status: Optional[str] = None
    is_primary: Optional[bool] = None


@router.get("/operators/{operator_id}/nodes")
async def list_operator_nodes(
    operator_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    nodes = storage.pin_list_operator_nodes(operator_id)
    stats = storage.pin_get_operator_node_stats(operator_id)
    
    return {
        "operator_id": operator_id,
        "nodes": nodes,
        "stats": stats
    }


@router.post("/operators/{operator_id}/nodes")
async def create_node(
    operator_id: str,
    data: NodeCreateRequest,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if not data.alias or len(data.alias) < 2:
        raise HTTPException(status_code=400, detail="Node alias must be at least 2 characters")
    
    if not data.endpoint.startswith("http"):
        raise HTTPException(status_code=400, detail="Invalid endpoint URL")
    
    if data.capacity < 1 or data.capacity > 10000:
        raise HTTPException(status_code=400, detail="Capacity must be between 1 and 10000")
    
    if not data.models:
        raise HTTPException(status_code=400, detail="At least one model required")
    
    existing = storage.pin_check_endpoint_exists(data.endpoint, exclude_operator_id=operator_id)
    if existing:
        if existing.get("operator_id") == operator_id:
            raise HTTPException(
                status_code=409,
                detail=f"You already have a node '{existing['alias']}' with this endpoint. Each endpoint must be unique."
            )
        else:
            raise HTTPException(
                status_code=409,
                detail=f"This endpoint is already registered by another operator. Each endpoint (IP:port or domain:port) must be unique across the network."
            )
    
    node = storage.pin_create_node(
        operator_id=operator_id,
        alias=data.alias,
        endpoint=data.endpoint,
        models=data.models,
        capacity=data.capacity,
        region=data.region
    )
    
    return {
        "success": True,
        "node": node,
        "message": "Node created. Use the credentials to configure your PIN Client."
    }


@router.get("/nodes/{node_id}")
async def get_node(
    node_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    node = storage.pin_get_node(node_id)
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    
    if not operator or operator.id != node["operator_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return node


@router.patch("/nodes/{node_id}")
async def update_node(
    node_id: str,
    data: NodeUpdateRequest,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    node = storage.pin_get_node(node_id)
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    
    if not operator or operator.id != node["operator_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    updates = {k: v for k, v in data.dict().items() if v is not None}
    updated_node = storage.pin_update_node(node_id, updates)
    
    return {"success": True, "node": updated_node}


@router.delete("/nodes/{node_id}")
async def delete_node(
    node_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    node = storage.pin_get_node(node_id)
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    
    if not operator or operator.id != node["operator_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    storage.pin_delete_node(node_id)
    
    return {"success": True, "message": "Node deleted"}


@router.post("/nodes/{node_id}/regenerate-credentials")
async def regenerate_node_credentials(
    node_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    node = storage.pin_get_node(node_id)
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    
    if not operator or operator.id != node["operator_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    new_creds = storage.pin_regenerate_node_credentials(node_id)
    
    return {
        "success": True,
        "credentials": new_creds,
        "message": "Credentials regenerated. Update your PIN Client with new values."
    }


@router.post("/nodes/{node_id}/set-primary")
async def set_primary_node(
    node_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    node = storage.pin_get_node(node_id)
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    
    if not operator or operator.id != node["operator_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    storage.pin_set_primary_node(operator.id, node_id)
    
    return {"success": True, "message": "Node set as primary"}


@router.get("/operators/{operator_id}/status", response_model=PinOperatorStatus)
async def get_operator_status(
    operator_id: str,
    authorization: str = Header(None)
):
    operator = storage.pin_get_operator(operator_id)
    if not operator:
        raise HTTPException(status_code=404, detail="Operator not found")
    
    reputation = storage.pin_get_reputation(operator_id)
    billing = storage.pin_get_operator_billing(operator_id)
    is_online = storage.pin_is_operator_online(operator_id)
    
    return PinOperatorStatus(
        operator_id=operator_id,
        status=operator.status,
        online=is_online,
        reputation_score=reputation.score if reputation else 0.5,
        total_requests=reputation.total_requests if reputation else 0,
        success_rate=reputation.success_rate if reputation else 1.0,
        avg_latency_ms=reputation.avg_latency_ms if reputation else 0.0,
        earnings_balance=billing.earnings_balance if billing else 0.0,
        total_earned=billing.total_earned if billing else 0.0
    )


@router.get("/operators/{operator_id}/earnings", response_model=PinOperatorEarnings)
async def get_operator_earnings(
    operator_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    billing = storage.pin_get_operator_billing(operator_id)
    if not billing:
        return PinOperatorEarnings(
            balance=0.0,
            total_earned=0.0,
            total_paid_out=0.0
        )
    
    earnings_24h = storage.pin_get_operator_earnings_period(operator_id, hours=24)
    earnings_7d = storage.pin_get_operator_earnings_period(operator_id, hours=168)
    earnings_30d = storage.pin_get_operator_earnings_period(operator_id, hours=720)
    
    return PinOperatorEarnings(
        balance=billing.earnings_balance,
        total_earned=billing.total_earned,
        total_paid_out=billing.total_paid_out,
        earnings_24h=earnings_24h,
        earnings_7d=earnings_7d,
        earnings_30d=earnings_30d
    )


@router.get("/operators/{operator_id}/wallet")
async def get_operator_wallet(
    operator_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    wallet_data = storage.r.hgetall(f"aiconsult:pin:wallet:{operator_id}")
    return {
        "bsc_address": wallet_data.get("bsc_address", ""),
        "updated_at": wallet_data.get("updated_at", "")
    }


@router.put("/operators/{operator_id}/wallet")
async def update_operator_wallet(
    operator_id: str,
    data: PinWalletUpdate,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if not data.bsc_address or len(data.bsc_address) != 42 or not data.bsc_address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Invalid BSC address. Must be 42 characters starting with 0x")
    
    now = datetime.utcnow().isoformat()
    storage.r.hset(f"aiconsult:pin:wallet:{operator_id}", mapping={
        "bsc_address": data.bsc_address,
        "updated_at": now
    })
    
    return {
        "bsc_address": data.bsc_address,
        "updated_at": now,
        "message": "Wallet address updated successfully"
    }


@router.post("/operators/{operator_id}/withdraw")
async def request_withdrawal(
    operator_id: str,
    data: PinWithdrawalRequest,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    wallet_data = storage.r.hgetall(f"aiconsult:pin:wallet:{operator_id}")
    bsc_address = wallet_data.get("bsc_address", "")
    
    if not bsc_address:
        raise HTTPException(status_code=400, detail="Please set your BSC wallet address first")
    
    result = storage.pin_create_withdrawal_atomic(
        operator_id=operator_id,
        amount=data.amount,
        payout_email=bsc_address,
        min_withdrawal=50.0
    )
    
    if not result.get('success'):
        error_type = result.get('error')
        if error_type == 'INSUFFICIENT_BALANCE':
            raise HTTPException(
                status_code=400, 
                detail=f"Insufficient balance. Available: ${result.get('balance', 0):.2f}"
            )
        elif error_type == 'BELOW_MINIMUM':
            raise HTTPException(
                status_code=400, 
                detail=f"Minimum withdrawal is ${result.get('minimum', 50):.2f}"
            )
        raise HTTPException(status_code=400, detail="Withdrawal failed")
    
    return {
        "withdrawal_id": result.get('withdrawal_id'),
        "amount": data.amount,
        "bsc_address": bsc_address,
        "new_balance": result.get('new_balance'),
        "status": "pending",
        "message": "Withdrawal claim submitted. You will receive USDT on BSC once processed."
    }


@router.get("/operators/{operator_id}/withdrawals")
async def get_operator_withdrawals(
    operator_id: str,
    authorization: str = Header(None)
):
    operator = await get_operator_from_api_key(authorization)
    if not operator or operator.id != operator_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    withdrawal_ids = storage.r.lrange(f"aiconsult:pin:withdrawals:operator:{operator_id}", 0, 50)
    withdrawals = []
    
    for wd_id in withdrawal_ids or []:
        wd_data = storage.r.hgetall(f"aiconsult:pin:withdrawals:{wd_id}")
        if wd_data:
            withdrawals.append({
                "id": wd_id,
                "amount": float(wd_data.get("amount", 0)),
                "bsc_address": wd_data.get("bsc_address", ""),
                "status": wd_data.get("status", "pending"),
                "created_at": wd_data.get("created_at", ""),
                "processed_at": wd_data.get("processed_at"),
                "tx_hash": wd_data.get("tx_hash")
            })
    
    return {"withdrawals": withdrawals}


# =============================================================================
# Network Status Routes
# =============================================================================

@router.get("/network/status", response_model=PinNetworkStatus)
async def get_network_status():
    return storage.pin_get_network_status()


@router.get("/network/stats")
async def get_network_stats():
    """Public endpoint for landing page - comprehensive network statistics."""
    cache_key = "aiconsult:pin:network_stats_cache"
    cached = storage.r.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except:
            pass
    
    try:
        operators = storage.pin_list_operators(active_only=False) or []
    except:
        operators = []
    
    online_count = 0
    total_capacity = 0
    regions = set()
    all_models = set()
    
    for op in operators:
        try:
            heartbeat_key = f"aiconsult:pin:heartbeats:{op.id}"
            last_heartbeat = storage.r.get(heartbeat_key)
            if last_heartbeat:
                try:
                    hb_time = float(last_heartbeat)
                    if time.time() - hb_time < 300:
                        online_count += 1
                except:
                    pass
            
            if hasattr(op, 'region') and op.region:
                region_val = op.region.value if hasattr(op.region, 'value') else str(op.region)
                regions.add(region_val)
            
            if hasattr(op, 'models') and op.models:
                for m in op.models:
                    if m:
                        all_models.add(m)
            
            if hasattr(op, 'capacity') and op.capacity:
                total_capacity += int(op.capacity)
        except Exception as e:
            print(f"[PIN Stats] Error processing operator: {e}")
            continue
    
    try:
        nodes = storage.r.smembers("aiconsult:pin:nodes:all") or set()
        for node_id in nodes:
            try:
                node_data = storage.r.hgetall(f"aiconsult:pin:nodes:{node_id}")
                if node_data:
                    models_str = node_data.get("models", "[]")
                    if models_str:
                        try:
                            node_models = json.loads(models_str) if isinstance(models_str, str) else models_str
                            for m in node_models:
                                if m:
                                    all_models.add(m)
                        except:
                            pass
                    if node_data.get("region"):
                        regions.add(node_data.get("region"))
                    if node_data.get("capacity"):
                        try:
                            total_capacity += int(node_data.get("capacity", 0))
                        except:
                            pass
            except:
                continue
    except:
        pass
    
    total_requests = 0
    total_tokens = 0
    try:
        total_requests = int(storage.r.get("aiconsult:pin:stats:total_requests") or 0)
        total_tokens = int(storage.r.get("aiconsult:pin:stats:total_tokens") or 0)
    except:
        pass
    
    result = {
        "operators_total": len(operators),
        "operators_online": online_count,
        "total_capacity": total_capacity,
        "regions": list(regions),
        "region_count": len(regions),
        "models": list(all_models),
        "model_count": len(all_models),
        "total_requests_served": total_requests,
        "total_tokens_processed": total_tokens,
        "network_status": "healthy" if online_count > 0 else "growing"
    }
    
    try:
        storage.r.setex(cache_key, 30, json.dumps(result))
    except:
        pass
    
    return result


@router.get("/pricing/summary")
async def get_pricing_summary(model: str = None, tier: str = None):
    """Public endpoint for landing page - aggregated pricing from operators."""
    cache_key = f"aiconsult:pin:pricing_cache:{model or 'all'}:{tier or 'all'}"
    try:
        cached = storage.r.get(cache_key)
        if cached:
            return json.loads(cached)
    except:
        pass
    
    try:
        operators = storage.pin_list_operators(active_only=False) or []
    except:
        operators = []
    
    prices = []
    
    for op in operators:
        try:
            pricing = getattr(op, 'pricing_per_1k_tokens', None)
            if pricing and float(pricing) > 0:
                if model:
                    op_models = getattr(op, 'models', []) or []
                    if not any(model.lower() == m.lower() for m in op_models if m):
                        continue
                
                if tier:
                    op_key = f"aiconsult:pin:operators:{op.id}"
                    op_tier = storage.r.hget(op_key, "quality_tier")
                    if op_tier != tier:
                        continue
                
                prices.append(float(pricing))
        except:
            continue
    
    try:
        nodes = storage.r.smembers("aiconsult:pin:nodes:all") or set()
        for node_id in nodes:
            try:
                node_data = storage.r.hgetall(f"aiconsult:pin:nodes:{node_id}")
                if node_data:
                    node_pricing = node_data.get("pricing_per_1k_tokens")
                    if node_pricing:
                        price = float(node_pricing)
                        if price > 0:
                            if model:
                                models_str = node_data.get("models", "[]")
                                try:
                                    node_models = json.loads(models_str) if isinstance(models_str, str) else []
                                except:
                                    node_models = []
                                if not any(model.lower() == m.lower() for m in node_models if m):
                                    continue
                            if tier:
                                node_tier = node_data.get("quality_tier")
                                if node_tier != tier:
                                    continue
                            prices.append(price)
            except:
                continue
    except:
        pass
    
    if prices:
        result = {
            "average": round(sum(prices) / len(prices), 6),
            "lowest": round(min(prices), 6),
            "highest": round(max(prices), 6),
            "operator_count": len(prices),
            "sample_count": len(prices),
            "currency": "USDT",
            "unit": "per 1K tokens",
            "filters": {"model": model, "tier": tier}
        }
    else:
        result = {
            "average": None,
            "lowest": None,
            "highest": None,
            "operator_count": 0,
            "sample_count": 0,
            "currency": "USDT",
            "unit": "per 1K tokens",
            "filters": {"model": model, "tier": tier},
            "message": "No pricing data available yet. Network is growing!"
        }
    
    try:
        storage.r.setex(cache_key, 30, json.dumps(result))
    except:
        pass
    
    return result


@router.get("/config/operator-share")
async def get_operator_share():
    """Public endpoint to get the current operator share percentage."""
    fee_percent = storage.pin_get_protocol_fee_percent()
    operator_share = 1.0 - fee_percent
    return {
        "operator_share_percent": operator_share,
        "protocol_fee_percent": fee_percent,
        "operator_share_display": f"{int(operator_share * 100)}%",
        "protocol_fee_display": f"{int(fee_percent * 100)}%"
    }


@router.get("/network/operators", response_model=List[PinOperatorPublic])
async def list_operators():
    operators = storage.pin_list_operators(active_only=True)
    result = []
    for op in operators:
        rep = storage.pin_get_reputation(op.id)
        result.append(PinOperatorPublic(
            id=op.id,
            name=op.name,
            region=op.region,
            models=op.models,
            status=op.status,
            reputation_score=rep.score if rep else 0.5,
            pricing_per_1k_tokens=op.pricing_per_1k_tokens
        ))
    return result


@router.get("/network/models")
async def list_available_models():
    models = storage.pin_list_available_models()
    return {"models": models}


@router.get("/network/health")
async def get_network_health():
    operators = storage.pin_list_operators(active_only=True)
    health_status = []
    
    for op in operators:
        health_data = storage.r.hgetall(f"aiconsult:pin:health:{op.id}")
        health_status.append({
            "operator_id": op.id,
            "name": op.name,
            "last_check": health_data.get("last_check", "never"),
            "status": health_data.get("status", "unknown"),
            "latency_ms": int(health_data.get("latency_ms", 0)) if health_data.get("latency_ms") else None,
            "error": health_data.get("error")
        })
    
    healthy_count = sum(1 for h in health_status if h["status"] == "healthy")
    
    return {
        "total_operators": len(health_status),
        "healthy": healthy_count,
        "unhealthy": len(health_status) - healthy_count,
        "operators": health_status
    }


# =============================================================================
# Inference Routes
# =============================================================================

class PinSpeechRequest(PydanticBaseModel):
    model: str
    input: str
    voice: str = "default"
    response_format: str = "mp3"
    speed: Optional[float] = None


@router.post("/audio/speech")
async def pin_audio_speech(
    request: PinSpeechRequest,
    user: User = Depends(get_current_user_optional)
):
    """OpenAI-compatible speech endpoint served by PIN TTS nodes.

    Routes to an operator whose daemon registered the model with a "tts:"
    prefix (pin-clientd v2.4.0+). Returns raw audio bytes, exactly like
    OpenAI's /v1/audio/speech.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    from api.services.pin_service import pin_tts_speech

    try:
        result = await pin_tts_speech(
            user_id=user.id,
            model=request.model,
            input_text=request.input,
            voice=request.voice,
            response_format=request.response_format,
            speed=request.speed,
        )
        audio = base64.b64decode(result["audio_b64"])
        return Response(
            content=audio,
            media_type=result.get("content_type", "application/octet-stream"),
            headers={
                "x-pin-operator": result["pin_metadata"]["operator_id"],
                "x-pin-latency-ms": str(result["pin_metadata"]["latency_ms"]),
                "x-pin-credits-charged": str(result["pin_metadata"]["credits_charged"]),
            },
        )
    except ValueError as e:
        error_msg = str(e)
        if "insufficient" in error_msg.lower():
            raise HTTPException(status_code=402, detail=error_msg)
        if "no operators" in error_msg.lower():
            raise HTTPException(status_code=503, detail=error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/chat/completions")
async def pin_chat_completions(
    request: PinChatCompletionRequest,
    user: User = Depends(get_current_user_optional)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    from api.services.pin_service import (
        pin_chat_completion,
        pin_chat_completion_stream,
    )

    messages = [{"role": m.role, "content": m.content} for m in request.messages]

    # stream=True was accepted by the schema and then silently ignored: this
    # route always called the non-streaming path, so a caller asking for SSE
    # got one buffered JSON object after the full generation. On a reasoning
    # model that wait is minutes, and the request died at the gateway with a
    # 504 before the model ever finished thinking.
    #
    # pin_chat_completion_stream has existed and been tested all along; it was
    # only reachable through the internal provider adapter. Wire it up.
    if request.stream:
        async def event_generator():
            try:
                async for chunk in pin_chat_completion_stream(
                    user_id=user.id,
                    model=request.model,
                    messages=messages,
                    temperature=request.temperature,
                    max_tokens=request.max_tokens,
                    tools=request.tools,
                    tool_choice=request.tool_choice,
                ):
                    yield f"data: {json.dumps(chunk)}\n\n"
                yield "data: [DONE]\n\n"
            except ValueError as e:
                # The stream has already begun (headers are out), so an error
                # cannot become an HTTP status. Report it IN the stream so the
                # caller sees a reason instead of a silent truncation.
                yield f"data: {json.dumps({'error': {'message': str(e), 'type': 'pin_error'}})}\n\n"
                yield "data: [DONE]\n\n"
            except Exception as e:
                import traceback
                traceback.print_exc()
                yield f"data: {json.dumps({'error': {'message': str(e), 'type': 'internal_error'}})}\n\n"
                yield "data: [DONE]\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                # Without this nginx buffers the whole response and the
                # streaming is undone one hop from the client.
                "X-Accel-Buffering": "no",
            },
        )

    try:
        result = await pin_chat_completion(
            user_id=user.id,
            model=request.model,
            messages=messages,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            tools=request.tools,
            tool_choice=request.tool_choice,
        )
        return result
    except ValueError as e:
        error_msg = str(e)
        if "insufficient" in error_msg.lower():
            raise HTTPException(status_code=402, detail=error_msg)
        if "no operators" in error_msg.lower():
            raise HTTPException(status_code=503, detail=error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/chat/completions_old_disabled")
async def pin_chat_completions_old(
    request: PinChatCompletionRequest,
    user: User = Depends(get_current_user_optional)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user_billing = storage.pin_get_user_billing(user.id)
    if not user_billing or user_billing.credits_balance <= 0:
        raise HTTPException(status_code=402, detail="Insufficient credits")
    
    excluded_operators = []
    max_attempts = 3
    
    for attempt in range(max_attempts):
        operator = storage.pin_select_operator(
            model=request.model,
            exclude=excluded_operators
        )
        
        if not operator:
            raise HTTPException(
                status_code=503,
                detail=f"No operators available for model: {request.model}"
            )
        
        job = storage.pin_create_job(
            user_id=user.id,
            operator_id=operator.id,
            model=request.model
        )
        
        try:
            start_time = time.time()
            
            async with httpx.AsyncClient(timeout=OPERATOR_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"{operator.endpoint}/v1/chat/completions",
                    json={
                        "model": request.model,
                        "messages": [{"role": m.role, "content": m.content} for m in request.messages],
                        "temperature": request.temperature,
                        "max_tokens": request.max_tokens,
                        "stream": False
                    },
                    headers={"Content-Type": "application/json"}
                )
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            if response.status_code != 200:
                raise Exception(f"Operator returned {response.status_code}")
            
            result = response.json()
            
            usage = result.get("usage", {})
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", input_tokens + output_tokens)
            
            billing_result = storage.pin_complete_job_atomic(
                job_id=job.id,
                user_id=user.id,
                operator_id=operator.id,
                total_tokens=total_tokens,
                model=request.model,
                pricing_per_1k=operator.pricing_per_1k_tokens,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms
            )
            
            if not billing_result.get('success'):
                storage.pin_fail_job(job.id, billing_result.get('error', 'billing_failed'))
                if billing_result.get('error') == 'INSUFFICIENT_CREDITS':
                    raise HTTPException(
                        status_code=402, 
                        detail=f"Insufficient credits. Balance: ${billing_result.get('balance', 0):.4f}"
                    )
                raise Exception(f"Billing failed: {billing_result}")
            
            credits_charged = billing_result.get('total_cost', 0)
            storage.pin_update_reputation(operator.id, success=True, latency_ms=latency_ms)
            
            choices = result.get("choices", [])
            
            return {
                "id": f"pin-{job.id}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": request.model,
                "choices": choices,
                "usage": {
                    "prompt_tokens": input_tokens,
                    "completion_tokens": output_tokens,
                    "total_tokens": total_tokens
                },
                "pin_metadata": {
                    "operator_id": operator.id,
                    "latency_ms": latency_ms,
                    "credits_charged": credits_charged
                }
            }
            
        except Exception as e:
            storage.pin_fail_job(job.id, str(e))
            storage.pin_update_reputation(operator.id, success=False)
            excluded_operators.append(operator.id)
            
            if attempt == max_attempts - 1:
                raise HTTPException(
                    status_code=503,
                    detail="All operators failed to respond"
                )
    
    raise HTTPException(status_code=503, detail="Inference failed")


# =============================================================================
# User Billing Routes
# =============================================================================

@router.get("/credits/balance")
async def get_credits_balance(user: User = Depends(get_current_user_optional)):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    billing = storage.pin_get_user_billing(user.id)
    return {
        "credits_balance": billing.credits_balance if billing else 0.0,
        "total_spent": billing.total_spent if billing else 0.0
    }


@router.post("/credits/add")
async def add_credits(
    amount: float,
    user: User = Depends(get_current_user_optional)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    storage.pin_add_user_credits(user.id, amount)
    billing = storage.pin_get_user_billing(user.id)
    
    return {
        "credits_added": amount,
        "new_balance": billing.credits_balance if billing else amount
    }


# =============================================================================
# Admin Routes
# =============================================================================

admin_router = APIRouter(prefix="/api/admin/pin", tags=["pin-admin"])

@admin_router.get("/operators")
async def admin_list_operators(user: User = Depends(get_current_user_optional)):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    operators = storage.pin_list_operators(active_only=False)
    return {"operators": [op.model_dump() for op in operators]}


@admin_router.patch("/operators/{operator_id}/status")
async def admin_update_operator_status(
    operator_id: str,
    status: OperatorStatus,
    user: User = Depends(get_current_user_optional)
):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    storage.pin_update_operator_status(operator_id, status.value)
    return {"message": f"Operator status updated to {status.value}"}


@admin_router.post("/seed-test-operator")
async def seed_test_operator(
    user: User = Depends(get_current_user_optional)
):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    test_endpoint = "https://shared-llama.interchained.org"
    
    existing = storage.pin_get_operator_by_endpoint(test_endpoint)
    if existing:
        return {"message": "Test operator already exists", "operator_id": existing.id}
    
    test_models = [
        "llama3:8b", "mistral:latest", "codellama:34b", "phi3:latest",
        "gemma:2b", "tinyllama:latest", "qwen2:7b", "stablelm2:1.6b"
    ]
    
    raw_key, key_hash = generate_operator_api_key()
    
    operator = storage.pin_create_operator(
        user_id=user.id,
        name="Interchained Shared LLaMA",
        endpoint=test_endpoint,
        models=test_models,
        capacity=20,
        pricing_per_1k_tokens=0.10,
        region="global",
        api_key_hash=key_hash
    )
    
    storage.pin_update_operator_status(operator.id, "active")
    
    storage.pin_record_heartbeat(
        operator_id=operator.id,
        current_load=0,
        capacity=20,
        models_available=test_models
    )
    
    return {
        "message": "Test operator seeded successfully",
        "operator_id": operator.id,
        "api_key": raw_key,
        "models": test_models
    }


@admin_router.get("/config/protocol-fee")
async def get_protocol_fee(user: User = Depends(get_current_user_optional)):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    fee_percent = storage.pin_get_protocol_fee_percent()
    operator_share = 1.0 - fee_percent
    
    return {
        "protocol_fee_percent": fee_percent,
        "operator_share_percent": operator_share,
        "display": f"{int(operator_share * 100)}% operator / {int(fee_percent * 100)}% protocol"
    }


@admin_router.patch("/config/protocol-fee")
async def set_protocol_fee(
    percent: float,
    user: User = Depends(get_current_user_optional)
):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    if percent < 0 or percent > 0.5:
        raise HTTPException(status_code=400, detail="Protocol fee must be between 0% and 50%")
    
    storage.pin_set_protocol_fee_percent(percent)
    operator_share = 1.0 - percent
    
    return {
        "protocol_fee_percent": percent,
        "operator_share_percent": operator_share,
        "display": f"{int(operator_share * 100)}% operator / {int(percent * 100)}% protocol",
        "message": "Protocol fee updated successfully"
    }


@admin_router.get("/withdrawals")
async def admin_list_withdrawals(
    status: Optional[str] = None,
    user: User = Depends(get_current_user_optional)
):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    pending_ids = storage.r.smembers("aiconsult:pin:withdrawals:pending")
    withdrawals = []
    
    for wd_id in pending_ids or []:
        wd_data = storage.r.hgetall(f"aiconsult:pin:withdrawals:{wd_id}")
        if wd_data:
            wd_status = wd_data.get("status", "pending")
            if status and wd_status != status:
                continue
            
            operator = storage.pin_get_operator(wd_data.get("operator_id", ""))
            withdrawals.append({
                "id": wd_id,
                "operator_id": wd_data.get("operator_id"),
                "operator_name": operator.name if operator else "Unknown",
                "amount": float(wd_data.get("amount", 0)),
                "bsc_address": wd_data.get("bsc_address", ""),
                "status": wd_status,
                "created_at": wd_data.get("created_at", ""),
                "processed_at": wd_data.get("processed_at"),
                "tx_hash": wd_data.get("tx_hash")
            })
    
    withdrawals.sort(key=lambda x: x["created_at"], reverse=True)
    
    return {
        "total": len(withdrawals),
        "pending": sum(1 for w in withdrawals if w["status"] == "pending"),
        "withdrawals": withdrawals
    }




@admin_router.patch("/withdrawals/{withdrawal_id}/process")
async def admin_process_withdrawal(
    withdrawal_id: str,
    tx_hash: str,
    notes: Optional[str] = None,
    user: User = Depends(get_current_user_optional)
):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    wd_data = storage.r.hgetall(f"aiconsult:pin:withdrawals:{withdrawal_id}")
    if not wd_data:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    if wd_data.get("status") != "pending":
        raise HTTPException(status_code=400, detail="Withdrawal already processed")
    
    now = datetime.utcnow().isoformat()
    
    storage.r.hset(f"aiconsult:pin:withdrawals:{withdrawal_id}", mapping={
        "status": "completed",
        "processed_at": now,
        "tx_hash": tx_hash,
        "admin_notes": notes or "",
        "processed_by": user.id
    })
    
    storage.r.srem("aiconsult:pin:withdrawals:pending", withdrawal_id)
    
    operator_id = wd_data.get("operator_id")
    amount = float(wd_data.get("amount", 0))
    storage.r.hincrbyfloat(f"aiconsult:pin:billing:operator:{operator_id}", "total_paid_out", amount)
    
    return {
        "withdrawal_id": withdrawal_id,
        "status": "completed",
        "tx_hash": tx_hash,
        "processed_at": now,
        "message": "Withdrawal marked as completed"
    }


@admin_router.patch("/withdrawals/{withdrawal_id}/reject")
async def admin_reject_withdrawal(
    withdrawal_id: str,
    reason: str,
    user: User = Depends(get_current_user_optional)
):
    if not user or user.role.value != "super_admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    wd_data = storage.r.hgetall(f"aiconsult:pin:withdrawals:{withdrawal_id}")
    if not wd_data:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    if wd_data.get("status") != "pending":
        raise HTTPException(status_code=400, detail="Withdrawal already processed")
    
    now = datetime.utcnow().isoformat()
    operator_id = wd_data.get("operator_id")
    amount = float(wd_data.get("amount", 0))
    
    storage.r.hset(f"aiconsult:pin:withdrawals:{withdrawal_id}", mapping={
        "status": "rejected",
        "processed_at": now,
        "admin_notes": reason,
        "processed_by": user.id
    })
    
    storage.r.srem("aiconsult:pin:withdrawals:pending", withdrawal_id)
    
    storage.r.hincrbyfloat(f"aiconsult:pin:billing:operator:{operator_id}", "earnings_balance", amount)
    
    return {
        "withdrawal_id": withdrawal_id,
        "status": "rejected",
        "reason": reason,
        "refunded": amount,
        "message": "Withdrawal rejected and funds returned to operator balance"
    }


# =============================================================================
# PIN Client WebSocket Connection Manager
# =============================================================================

class _StaleSocketCleanup(Exception):
    """Internal: skip eviction because the operator is still held elsewhere.
    Mirrors _StaleStreamCleanup on the SSE teardown so both transports read
    the same way."""


class PinClientConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.operator_map: Dict[str, str] = {}
        self.pending_requests: Dict[str, asyncio.Future] = {}
        # operator bus subscriptions, one per held connection
        self.bus_tasks = {}
        # request_id -> queue of stream events, for requests made with
        # stream=True. Separate from pending_requests: a streaming request has
        # no single moment of resolution, it has a sequence.
        self.stream_queues: Dict[str, asyncio.Queue] = {}
        # request_id -> operator_id, so a disconnect can fail the streams it
        # owns instead of leaving them to time out.
        self.stream_owners: Dict[str, str] = {}
        self.node_interviews: Dict[str, str] = {}  # interview_id -> node_id mapping
        self.interview_api_modes: Dict[str, str] = {}  # interview_id -> api_mode ("ollama" or "openai")
    
    async def connect(self, websocket: WebSocket, operator_id: str, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.operator_map[client_id] = operator_id
        
        storage.r.sadd("aiconsult:pin:operators:online", operator_id)
        storage.r.setex(f"aiconsult:pin:heartbeats:{operator_id}", 300, str(time.time()))
        storage.r.hset(f"aiconsult:pin:operators:{operator_id}", "last_connected", datetime.utcnow().isoformat())
        
        # THE PROCESS HOLDING THIS SOCKET IS NOW THE ONE THAT CAN SERVE IT.
        #
        # Subscribing here is what lets a request that landed on a DIFFERENT
        # worker still reach this operator: that worker publishes, this one
        # consumes and answers. Without it, `operator_map` is a private fact and
        # every other worker truthfully reports "no operators in this process".
        self.bus_tasks[client_id] = asyncio.create_task(
            bus.serve(operator_id, self._serve_bus_job))

        print(f"[PIN WS] Operator {operator_id} connected via client {client_id}")
    
    def disconnect(self, client_id: str):
        # Cancel FIRST. A subscription that outlives its socket keeps this
        # worker's `holders()` count non-zero, so other workers keep dispatching
        # jobs into a process that can no longer deliver them — which reads as a
        # timeout rather than as "no operator", and is worse than either.
        task = self.bus_tasks.pop(client_id, None)
        if task is not None:
            task.cancel()

        if client_id in self.active_connections:
            del self.active_connections[client_id]
        
        operator_id = self.operator_map.pop(client_id, None)
        if operator_id:
            # FIRST, before any I/O: unblock anyone mid-stream. This is
            # in-memory and cannot fail, whereas the storage writes below can
            # raise if the store is unreachable -- and if they raise before
            # this runs, every consumer on this operator hangs for the full
            # idle timeout waiting on a socket we already know is gone.
            self.fail_streams_for_operator(
                operator_id, "operator disconnected mid-stream")
            # ONLY IF NOTHING ELSE IS HOLDING THIS OPERATOR.
            #
            # This cleanup used to be unconditional, so a stale WebSocket
            # closing evicted an operator whose SSE downlink was alive and
            # heartbeating. Observed in production 2026-09-07: the daemon's
            # heartbeats all answered 200 while the store held
            #
            #     pin:operators:online           -> {'members': []}
            #     pin:heartbeats:op_a32a252d-292 -> None
            #
            # and the gateway answered "No operators available" for every
            # model, because pin_select_operator intersects with that empty
            # online set. The SSE teardown already guards against exactly this
            # (see pin_sse_transport.is_current_lease, "the corpse must not
            # evict the living"); the WebSocket teardown never got the guard,
            # so the same bug survived on the other transport.
            #
            # `operator_map` has already dropped THIS client, so what remains
            # is genuinely other holders: another socket in this worker, or an
            # SSE lease in any worker.
            still_held = (operator_id in self.operator_map.values()
                          or sse_transport.has_live_lease(operator_id))
            try:
                if still_held:
                    print(f"[PIN WS] {operator_id} socket closed but operator "
                          f"is still held elsewhere; leaving online set alone")
                    raise _StaleSocketCleanup()
                storage.r.srem("aiconsult:pin:operators:online", operator_id)
                storage.r.delete(f"aiconsult:pin:heartbeats:{operator_id}")
            except _StaleSocketCleanup:
                pass
            except Exception as exc:  # noqa: BLE001 -- cleanup must complete
                print(f"[PIN WS] online-set cleanup failed for {operator_id}: {exc}")
            print(f"[PIN WS] Operator {operator_id} disconnected")
    
    def is_operator_online(self, operator_id: str) -> bool:
        return operator_id in self.operator_map.values()
    
    def get_websocket_for_operator(self, operator_id: str) -> Optional[WebSocket]:
        for cid, oid in self.operator_map.items():
            if oid == operator_id:
                return self.active_connections.get(cid)
        return None
    
    async def _serve_bus_job(self, operator_id: str, kind: str,
                             request_id: str, payload: dict):
        """A job another worker handed us for an operator WE hold — over either
        transport.

        Routed through the SAME send_* methods a local request uses, so a
        dispatched job and a local one share one code path, one future
        discipline, one set of timeouts and one billing path.

        THE OPERATOR ID NOW ARRIVES WITH THE JOB. The first version scanned
        `operator_map` to work out who the job was for, which is a
        WebSocket-only structure: an SSE operator appears in no such map, so the
        scan returned None and every dispatched job came back empty — a
        permanent failure wearing a timeout's clothes. Since `bus.serve` is
        subscribed PER OPERATOR it always knew the answer; it just was not
        saying.
        """
        if kind == "tts":
            return await self.send_tts_request(
                operator_id=operator_id, request_id=request_id, payload=payload)
        if kind == "cancel":
            # Forward-only, like chat_stream: put CANCEL_REQUEST on the
            # downlink we hold and answer nothing — there is no consumer, that
            # is the whole point of the cancel.
            message = {"type": "CANCEL_REQUEST", "request_id": request_id}
            ws = self.get_websocket_for_operator(operator_id)
            if ws:
                try:
                    await ws.send_json(message)
                    return {"queued": True}
                except Exception as exc:  # noqa: BLE001
                    return {"error": f"failed to reach operator: {exc}"}
            if sse_transport.queue(operator_id, message):
                return {"queued": True}
            return {"error": "operator is not connected to this worker"}
        if kind == "chat_stream":
            # Forward-only. The requesting worker holds the consumer and a
            # stream relay subscribed to pin:bus:stream:{request_id}; the
            # operator's uplink POSTs publish straight onto that channel. So
            # this worker's whole job is putting the request on the downlink
            # it holds -- waiting here for a "result" would duplicate the
            # stream's own final message and hold a bus slot for the entire
            # generation.
            message = {
                "type": "INFERENCE_REQUEST",
                "request_id": request_id,
                "payload": payload,
            }
            ws = self.get_websocket_for_operator(operator_id)
            if ws:
                try:
                    await ws.send_json(message)
                    return {"queued": True}
                except Exception as exc:  # noqa: BLE001
                    return {"error": f"failed to reach operator: {exc}"}
            if sse_transport.queue(operator_id, message):
                return {"queued": True}
            return {"error": "operator is not connected to this worker"}
        return await self.send_inference_request(
            operator_id=operator_id, request_id=request_id, payload=payload)

    async def send_inference_request(self, operator_id: str, request_id: str, payload: dict) -> Optional[dict]:
        """Send one INFERENCE_REQUEST over whichever downlink this operator has.

        WebSocket if we hold a socket, otherwise the SSE event queue — the same
        two-transport shape `send_tts_request` already had. Returning None the
        moment no WebSocket exists made SSE operators permanently unreachable
        even while their downlink was open and their uplink was posting results.

        The waiting future is identical either way: `/stream/result` resolves it
        through `resolve_request`, exactly as the WebSocket INFERENCE_RESPONSE
        handler does. One pending map, one resolution path, two ways in.
        """
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[request_id] = future

        message = {
            "type": "INFERENCE_REQUEST",
            "request_id": request_id,
            "payload": payload,
        }

        # The daemon's result POST rides its keep-alive connection to ONE
        # worker -- not necessarily this one, even though this worker holds
        # the downlink the job went out on. Subscribe to the request's stream
        # channel BEFORE dispatching, so /stream/result's publish finds this
        # waiter wherever the POST happens to land. When the relay cannot
        # open (no Redis), the local resolve fallback is the old behavior.
        relay_q: asyncio.Queue = asyncio.Queue()
        relay = bus.open_stream_relay(request_id, relay_q)
        resolver = None
        if relay is not None:
            async def _resolve_from_relay():
                while True:
                    event = await relay_q.get()
                    if event.get("type") == "final":
                        if not future.done():
                            future.set_result(event.get("result"))
                        return

            resolver = asyncio.get_event_loop().create_task(_resolve_from_relay())

        try:
            ws = self.get_websocket_for_operator(operator_id)
            if ws:
                await ws.send_json(message)
            elif sse_transport.queue(operator_id, message):
                pass
            else:
                # Neither transport holds this operator. Fail NOW rather than
                # waiting out the deadline for an answer nobody was asked for.
                return None

            return await asyncio.wait_for(future, timeout=60.0)
        except asyncio.TimeoutError:
            # The caller is giving up; the operator should too. Without this a
            # timed-out job still burns the GPU to its last token in the dark.
            self.cancel_operator_job(operator_id, request_id)
            return None
        finally:
            if relay is not None:
                relay.close()
            if resolver is not None:
                resolver.cancel()
            self.pending_requests.pop(request_id, None)

    async def send_tts_request(self, operator_id: str, request_id: str,
                               payload: dict,
                               timeout: float = 300.0) -> Optional[dict]:
        """Send a TTS_REQUEST and wait for its single TTS_RESPONSE/TTS_ERROR.

        Same future discipline as send_inference_request, longer deadline:
        a cold TTS model can spend minutes loading before the first render,
        and unlike chat there are no interim chunks to keep the wait honest.
        """
        ws = self.get_websocket_for_operator(operator_id)
        queued = False
        if ws:
            queued = True
        elif sse_transport.queue(operator_id, {
            "type": "TTS_REQUEST",
            "request_id": request_id,
            "payload": payload,
        }):
            queued = True
        if not queued:
            return None

        future = asyncio.get_event_loop().create_future()
        self.pending_requests[request_id] = future

        try:
            if ws:
                await ws.send_json({
                    "type": "TTS_REQUEST",
                    "request_id": request_id,
                    "payload": payload
                })

            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return None
        finally:
            self.pending_requests.pop(request_id, None)

    def resolve_request(self, request_id: str, result: dict):
        if request_id in self.pending_requests:
            self.pending_requests[request_id].set_result(result)
        # A streaming request resolves through its queue, not a future.
        q = self.stream_queues.get(request_id)
        if q is not None:
            q.put_nowait({"type": "final", "result": result})

    def push_chunk(self, request_id: str, index: int, delta: str,
                   kind: str = "content"):
        """Route one INFERENCE_CHUNK to its waiting stream, if any.

        `kind` is "content" or "thinking". Reasoning models (muse-local and
        friends) emit their reasoning first with an EMPTY content field, and
        that phase can run for the whole generation -- surfacing it is the
        difference between a live thinking trace and a request that looks
        hung. Defaults to "content" so a pre-2.4 daemon, which sends no kind,
        behaves exactly as before.

        Unknown request_ids are ignored rather than logged per token: a
        cancelled or already-completed request can still have chunks in
        flight, and that is not an error worth a line each.
        """
        q = self.stream_queues.get(request_id)
        if q is not None:
            q.put_nowait({"type": "chunk", "index": index, "delta": delta,
                          "kind": kind if kind in ("content", "thinking") else "content"})

    def cancel_operator_job(self, operator_id: str, request_id: str):
        """Tell the operator to STOP GENERATING a request nobody is waiting on.

        THE ZOMBIE THIS KILLS, observed 2026-08-29: the caller aborts a
        looping stream (correctly) and retries (correctly) — and the operator
        keeps generating the aborted attempt to the last token, because
        nothing ever told it to stop. Each retry stacked another full-resume
        generation onto one GPU: 'model online, PIN queue overflowed', with
        every layer locally right.

        Best-effort by design — an operator that never receives the cancel
        merely finishes work nobody collects, which is exactly today's
        behavior. Same three-hop delivery as dispatch: local WebSocket, local
        SSE queue, then the operator bus for a downlink held by another
        worker. A pre-cancel daemon ignores the unknown message type.
        """
        message = {"type": "CANCEL_REQUEST", "request_id": request_id}
        ws = self.get_websocket_for_operator(operator_id)
        if ws:
            asyncio.ensure_future(ws.send_json(message))
            return
        if sse_transport.queue(operator_id, message):
            return
        bus.dispatch(operator_id, request_id, "cancel", {})

    def fail_streams_for_operator(self, operator_id: str, reason: str):
        """Unblock every stream owned by an operator that just went away.

        Without this a consumer waits out the full idle timeout on a socket
        that is known to be gone -- the disconnect is information we already
        have, so spend it.
        """
        for request_id, op_id in list(self.stream_owners.items()):
            if op_id != operator_id:
                continue
            q = self.stream_queues.get(request_id)
            if q is not None:
                q.put_nowait({"type": "error", "error": reason})

    async def stream_inference_request(self, operator_id: str, request_id: str,
                                       payload: dict, idle_timeout: float = None):
        """Send an inference request and yield events as the operator produces them.

        Yields dicts: {"type": "chunk", "delta": str} per token, then exactly
        one {"type": "final", "result": {...}} or {"type": "error", ...}.

        `idle_timeout` bounds the gap BETWEEN events, not the whole
        generation -- a long answer that keeps producing tokens must not be
        killed for taking a while, but a silent operator must not hang the
        caller forever.
        """
        # Default to an env-overridable value so a slow-reasoning local model
        # (muse-local can think in silence for over 90s before its first token)
        # does not trip a deadline the rest of the stack is perfectly happy to
        # wait for. This bounds the gap BETWEEN events, never the whole run --
        # a stream that keeps producing runs as long as it needs.
        if idle_timeout is None:
            idle_timeout = float(os.environ.get("PIN_IDLE_TIMEOUT", "180"))

        # Dispatch over whichever downlink this operator has — the same
        # two-transport shape send_inference_request uses. Returning "not
        # connected" the moment no WebSocket exists made SSE operators
        # permanently unstreamable even while their lease was open and their
        # chunk uplink was posting tokens. The return path is already
        # transport-agnostic: POST /stream/chunk and /stream/result feed
        # push_chunk and resolve_request, which write to this queue.
        message = {
            "type": "INFERENCE_REQUEST",
            "request_id": request_id,
            "payload": {**payload, "stream": True},
        }

        # THE QUEUE AND THE RELAY COME BEFORE THE DISPATCH. Pub/sub has no
        # memory: a chunk published a millisecond before we subscribe is gone,
        # and the first tokens of a fast model are exactly the ones at risk.
        # The relay is how chunks POSTed to OTHER workers reach this queue --
        # without it, this stream only ever heard uplinks that happened to
        # land on this process, which with the daemon's keep-alive connection
        # meant all of them or none of them.
        q: asyncio.Queue = asyncio.Queue()
        self.stream_queues[request_id] = q
        self.stream_owners[request_id] = operator_id
        relay = bus.open_stream_relay(request_id, q)
        finished = False

        ws = self.get_websocket_for_operator(operator_id)
        dispatched = False
        if ws:
            dispatched = True
        elif sse_transport.queue(operator_id, message):
            dispatched = True
        elif relay is not None and bus.dispatch(
                operator_id, request_id, "chat_stream", message["payload"]) > 0:
            # The downlink lives in another worker: hand the request across
            # the operator bus exactly as the non-streaming path does. Only
            # valid WITH a relay -- dispatching a stream whose tokens can
            # never come back would burn the operator's work to produce a
            # guaranteed timeout.
            dispatched = True
        if not dispatched:
            if relay is not None:
                relay.close()
            self.stream_queues.pop(request_id, None)
            self.stream_owners.pop(request_id, None)
            yield {"type": "error", "error": "operator is not connected"}
            return

        try:
            if ws:
                try:
                    await ws.send_json(message)
                except Exception as exc:  # noqa: BLE001
                    yield {"type": "error", "error": f"failed to reach operator: {exc}"}
                    return
            # SSE: already queued above; the operator's POST /stream/chunk
            # uplink feeds this queue -- via the bus relay when it lands on
            # another worker, via push_chunk when it lands here.

            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=idle_timeout)
                except asyncio.TimeoutError:
                    yield {
                        "type": "error",
                        "error": (f"operator produced nothing for {idle_timeout:.0f}s "
                                  f"(timeout between chunks, not total duration)"),
                    }
                    return

                if event["type"] == "chunk":
                    yield event
                    continue

                # "final" or "error" terminates the stream.
                finished = True
                yield event
                return
        finally:
            # EVERY exit that is not the operator's own terminal event means
            # nobody is waiting anymore — the caller disconnected mid-stream
            # (GeneratorExit lands here), the idle timeout fired, the send
            # failed. The operator must not keep paying for it.
            if not finished:
                self.cancel_operator_job(operator_id, request_id)
            if relay is not None:
                relay.close()
            self.stream_queues.pop(request_id, None)
            self.stream_owners.pop(request_id, None)
    
    async def heartbeat(self, client_id: str, node_id: str = None):
        operator_id = self.operator_map.get(client_id)
        if operator_id:
            storage.r.setex(f"aiconsult:pin:heartbeats:{operator_id}", 300, str(time.time()))
            storage.r.sadd("aiconsult:pin:operators:online", operator_id)
        if node_id:
            storage.r.setex(f"aiconsult:pin:node_heartbeats:{node_id}", 300, str(time.time()))
            storage.r.hset(f"aiconsult:pin:nodes:{node_id}", "last_seen", now_iso())
    
    async def send_interview_request(self, operator_id: str, model: str) -> Optional[dict]:
        """
        Send interview request to operator and wait for results.
        Returns quality tier information or None on failure.
        """
        ws = self.get_websocket_for_operator(operator_id)
        if not ws:
            return None
        
        interview_id = f"interview_{secrets.token_urlsafe(16)}"
        prompts = generate_interview_prompts(5)
        
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[interview_id] = future
        
        try:
            await ws.send_json({
                "type": "INTERVIEW_REQUEST",
                "interview_id": interview_id,
                "model": model,
                "prompts": [
                    {
                        "id": p["id"],
                        "prompt": p["prompt"],
                        "max_tokens": p["max_tokens"]
                    } for p in prompts
                ],
                "timeout_ms": INTERVIEW_TIMEOUT_SECONDS * 1000
            })
            
            print(f"[PIN INTERVIEW] Sent interview {interview_id} to operator {operator_id} for model {model}")
            
            result = await asyncio.wait_for(future, timeout=float(INTERVIEW_TIMEOUT_SECONDS))
            return result
        except asyncio.TimeoutError:
            print(f"[PIN INTERVIEW] Interview {interview_id} timed out")
            return None
        finally:
            self.pending_requests.pop(interview_id, None)


pin_ws_manager = PinClientConnectionManager()


class TestCredentialsRequest(PydanticBaseModel):
    client_id: str
    api_secret: str


@router.post("/test-credentials")
async def test_operator_credentials(data: TestCredentialsRequest):
    """
    Test if operator credentials are valid. 
    Use this to verify your daemon config before connecting.
    """
    import time as time_mod
    
    client_id = data.client_id
    api_secret = data.api_secret
    
    timestamp = str(int(time_mod.time()))
    signature = signature_from_secret(client_id, timestamp, api_secret)
    
    operator_id, op_data = _find_pin_operator_credentials(client_id)
    if not operator_id or not op_data:
        return {
            "valid": False,
            "error": f"Client ID {client_id} not found. Use operator credentials from your dashboard."
        }

    stored_hash = _pin_text(
        op_data.get(b"api_key_hash") or op_data.get("api_key_hash"))
    stored_name = _pin_text(op_data.get(b"name") or op_data.get("name"))
    if not stored_hash:
        return {"valid": False, "error": "Operator credential is missing; regenerate the API key."}

    if verify_signature(client_id, timestamp, signature, stored_hash):
        return {
            "valid": True,
            "operator_id": operator_id,
            "name": stored_name,
            "message": "Credentials are valid! Daemon will auto-register nodes on connect."
        }

    # Do not return stored/computed hash material to an unauthenticated caller.
    return {
        "valid": False,
        "error": (
            "Signature mismatch. The configured apiSecret does not match the "
            "current operator API key; copy/regenerate it and restart pin-clientd."
        ),
        "debug": {"client_id_found": True, "operator_id": operator_id},
    }


def _pin_text(value) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value or "")


def _find_pin_operator_credentials(client_id: str):
    """Return (operator_id, operator hash doc) for operator-id or issued client-id."""
    direct = storage.r.hgetall(f"aiconsult:pin:operators:{client_id}")
    if direct:
        return client_id, direct
    for raw_id in storage.r.smembers("aiconsult:pin:operators:all"):
        operator_id = _pin_text(raw_id)
        data = storage.r.hgetall(f"aiconsult:pin:operators:{operator_id}")
        if data:
            issued_client_id = _pin_text(
                data.get(b"client_id") or data.get("client_id"))
            if issued_client_id == client_id:
                return operator_id, data
    return None, None


def verify_client_signature(client_id: str, timestamp: str,
                            signature: str) -> Optional[tuple]:
    """Verify the daemon/Tauri WebSocket AUTH signature.

    Accept both documented operator ids (op_...) and issued client ids
    (pin_cid_...). The signature payload always uses the exact client_id sent
    by the daemon. The authoritative secret hash is api_key_hash — the same
    field used by Bearer authentication — so key regeneration cannot leave WS
    auth validating a different encrypted credential copy.
    """
    print(f"[PIN AUTH] Verifying client_id={client_id}")
    operator_id, op_data = _find_pin_operator_credentials(client_id)
    if not op_data or not operator_id:
        print(f"[PIN AUTH] Unknown client id: {client_id}")
        return None

    stored_hash = _pin_text(
        op_data.get(b"api_key_hash") or op_data.get("api_key_hash"))
    encrypted = _pin_text(
        op_data.get(b"api_key_encrypted")
        or op_data.get("api_key_encrypted"))

    # Backward compatibility for operator rows created before api_key_hash was
    # populated. Do not prefer this over the authoritative stored hash.
    if not stored_hash and encrypted:
        try:
            stored_hash = hash_api_secret(decrypt_api_key(encrypted))
        except Exception as exc:
            print(f"[PIN AUTH] Credential decrypt error for {operator_id}: {exc}")
            return None

    if not stored_hash:
        print(f"[PIN AUTH] Operator {operator_id} has no credential hash")
        return None

    # Diagnose internally inconsistent rows without leaking hash material.
    if encrypted:
        try:
            encrypted_hash = hash_api_secret(decrypt_api_key(encrypted))
            if not hmac.compare_digest(encrypted_hash, stored_hash):
                print(
                    f"[PIN AUTH] WARNING operator {operator_id} has mismatched "
                    "api_key_hash and encrypted credential; using api_key_hash")
        except Exception as exc:
            print(f"[PIN AUTH] Encrypted credential unreadable for {operator_id}: {exc}")

    if verify_signature(client_id, timestamp, signature, stored_hash):
        print(f"[PIN AUTH] SUCCESS operator={operator_id} client={client_id}")
        return (operator_id, None)

    print(
        f"[PIN AUTH] Signature mismatch for operator={operator_id}; "
        "daemon apiSecret does not match the current operator API key. "
        "Regenerate/copy the key and restart pin-clientd if it was rotated.")
    return None


@router.websocket("/ws")
async def pin_client_websocket(websocket: WebSocket):
    print(f"[PIN WS] WebSocket connection attempt from {websocket.client}")
    client_id = None
    operator_id = None
    node_id = None
    
    try:
        print(f"[PIN WS] Accepting WebSocket connection...")
        await websocket.accept()
        print(f"[PIN WS] WebSocket accepted, waiting for AUTH message...")
        
        auth_msg = await asyncio.wait_for(websocket.receive_json(), timeout=10.0)
        
        if auth_msg.get("type") != "AUTH":
            await websocket.send_json({"type": "ERROR", "message": "Expected AUTH message"})
            await websocket.close(code=4001)
            return
        
        client_id = auth_msg.get("client_id")
        timestamp = auth_msg.get("timestamp")
        signature = auth_msg.get("signature")
        
        if not client_id or not timestamp or not signature:
            await websocket.send_json({"type": "ERROR", "message": "Missing auth fields"})
            await websocket.close(code=4001)
            return
        
        ts_diff = abs(time.time() - float(timestamp))
        if ts_diff > 300:
            await websocket.send_json({"type": "ERROR", "message": "Timestamp expired"})
            await websocket.close(code=4001)
            return
        
        auth_result = verify_client_signature(client_id, timestamp, signature)
        if not auth_result:
            await websocket.send_json({"type": "ERROR", "message": "Invalid credentials"})
            await websocket.close(code=4003)
            return
        
        operator_id, node_id = auth_result
        
        pin_ws_manager.active_connections[client_id] = websocket
        pin_ws_manager.operator_map[client_id] = operator_id
        
        storage.r.sadd("aiconsult:pin:operators:online", operator_id)
        storage.r.setex(f"aiconsult:pin:heartbeats:{operator_id}", 300, str(time.time()))
        
        if node_id:
            storage.r.setex(f"aiconsult:pin:node_heartbeats:{node_id}", 300, str(time.time()))
            storage.r.hset(f"aiconsult:pin:nodes:{node_id}", "last_seen", now_iso())
        
        await websocket.send_json({
            "type": "AUTH_SUCCESS",
            "operator_id": operator_id,
            "node_id": node_id,
            "message": "Connected to PIN network"
        })
        
        print(f"[PIN WS] Operator {operator_id} node {node_id} authenticated and connected")
        
        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive_json(), timeout=60.0)
                msg_type = msg.get("type")
                
                if msg_type == "PONG":
                    await pin_ws_manager.heartbeat(client_id, node_id)
                
                elif msg_type == "HEARTBEAT":
                    await pin_ws_manager.heartbeat(client_id, node_id)
                    await websocket.send_json({"type": "HEARTBEAT_ACK"})
                
                elif msg_type == "MODEL_LIST":
                    # Legacy: Just store models at operator level, interviews happen per-node via REGISTER_NODE
                    models = msg.get("models", [])
                    storage.r.hset(f"aiconsult:pin:operators:{operator_id}", "models", json.dumps(models))
                    for model in models:
                        storage.r.sadd(f"aiconsult:pin:models:{model}", operator_id)
                        storage.r.sadd("aiconsult:pin:models:available", model)
                    print(f"[PIN WS] Operator {operator_id} models: {models} (use REGISTER_NODE for per-node interview)")
                    await websocket.send_json({"type": "MODEL_LIST_ACK", "models": models})
                
                elif msg_type == "REGISTER_NODE":
                    node_alias = msg.get("alias", "Default Node")
                    node_models = msg.get("models", [])
                    node_capacity = msg.get("capacity", 10)
                    node_region = msg.get("region", "global")
                    node_api_mode = msg.get("apiMode", "ollama")  # "ollama" or "openai"
                    node_price = msg.get("pricePerThousandTokens", 0.001)  # Default $0.001/1K tokens
                    node_interview_model = msg.get("interviewModel")  # Operator-specified interview model
                    
                    # Find existing node by alias or create with unique naming
                    result = storage.pin_find_or_create_node(
                        operator_id=operator_id,
                        alias=node_alias,
                        models=node_models,
                        capacity=node_capacity,
                        region=node_region
                    )
                    node_data = result["node"]
                    was_created = result["created"]
                    node_id = node_data.get("node_id") or node_data.get("id")
                    final_alias = node_data.get("alias", node_alias)
                    
                    # Store api_mode and price on node
                    target_key = f"aiconsult:pin:nodes:{node_id}"
                    storage.r.hset(target_key, "api_mode", node_api_mode)
                    storage.r.hset(target_key, "price_per_thousand_tokens", str(node_price))
                    
                    storage.r.setex(f"aiconsult:pin:node_heartbeats:{node_id}", 300, str(time.time()))
                    
                    for model in node_models:
                        storage.r.sadd(f"aiconsult:pin:models:{model}", operator_id)
                        storage.r.sadd("aiconsult:pin:models:available", model)
                    
                    await websocket.send_json({
                        "type": "REGISTER_NODE_ACK",
                        "node_id": node_id,
                        "alias": final_alias,
                        "models": node_models,
                        "api_mode": node_api_mode,
                        "created": was_created,
                        "message": f"Node {'registered' if was_created else 'updated'} successfully"
                    })
                    print(f"[PIN WS] Operator {operator_id} {'registered' if was_created else 'updated'} node {node_id} ({final_alias}) api_mode={node_api_mode} with models: {node_models}")
                    
                    # Trigger interview for this node
                    if node_models:
                        current_tier = storage.r.hget(target_key, "quality_tier")
                        interview_completed_at = storage.r.hget(target_key, "interview_completed_at")
                        
                        # Check if there's already a pending interview for this node
                        pending_for_node = any(nid == node_id for nid in pin_ws_manager.node_interviews.values())
                        if pending_for_node:
                            print(f"[PIN INTERVIEW] Skipping interview for {node_id} - already has pending interview")
                            continue
                        
                        # Skip if interview just completed (within 5 minutes) - prevents loops on reconnect during same session
                        # This allows interviews on genuine reconnects while preventing the loop when daemon drops mid-interview
                        if interview_completed_at:
                            try:
                                completed_ts = float(interview_completed_at)
                                elapsed = time.time() - completed_ts
                                if elapsed < INTERVIEW_COOLDOWN_SECONDS:
                                    # If tier is "failed", don't let them stay connected - disconnect immediately
                                    if current_tier == "failed":
                                        print(f"[PIN INTERVIEW] Node {node_id} has failed tier and recently interviewed ({int(elapsed)}s ago) - disconnecting")
                                        await websocket.send_json({
                                            "type": "INTERVIEW_FAILED",
                                            "node_id": node_id,
                                            "reason": "recently_failed",
                                            "message": (
                                                f"Interview failed {int(elapsed)}s ago. Please wait "
                                                f"{max(1, int(INTERVIEW_COOLDOWN_SECONDS - elapsed))}s "
                                                f"before reconnecting, or upgrade your hardware."),
                                            "retry_after_seconds": int(INTERVIEW_COOLDOWN_SECONDS - elapsed)
                                        })
                                        await websocket.close(code=4003, reason="Recently failed interview - wait before retry")
                                        return
                                    # For passing tiers, just skip re-interview
                                    print(f"[PIN INTERVIEW] Skipping interview for {node_id} - just completed {int(elapsed)}s ago (tier: {current_tier})")
                                    continue
                            except:
                                pass  # If parsing fails, proceed with interview
                        
                        # Interview model selection:
                        # 1. Operator-specified model (interviewModel from daemon config)
                        # 2. OpenAI mode: use first model
                        # 3. Ollama mode: use benchmark model
                        interview_model = None
                        
                        if node_interview_model:
                            # Operator specified their preferred interview model
                            interview_model = node_interview_model
                            in_list = node_interview_model in node_models
                            print(f"[PIN INTERVIEW] Node {node_id} using operator-specified interview model: {interview_model} (in model list: {in_list})")
                        elif node_api_mode == "openai" and node_models:
                            # OpenAI mode: use first model, no benchmark requirement
                            # OpenAI-compatible servers (vLLM, TGI, LocalAI) have variable naming
                            interview_model = node_models[0]
                            print(f"[PIN INTERVIEW] OpenAI mode node {node_id} - using first model: {interview_model}")
                        else:
                            # Ollama mode: benchmark model preferred (scores
                            # comparable across nodes), first advertised model
                            # otherwise — a custom fleet's absence from a list
                            # of four frozen 2024 names is not a failure.
                            interview_model = find_benchmark_model(node_models) \
                                or (node_models[0] if node_models else None)
                            if interview_model:
                                print(f"[PIN INTERVIEW] Ollama mode node {node_id} - using interview model: {interview_model}")
                        
                        # Final check: no interview model found
                        if not interview_model:
                            print(f"[PIN INTERVIEW] Node {node_id} has no interview model - interview failed")
                            storage.r.hset(target_key, mapping={
                                "quality_tier": "failed",
                                "interview_failure_reason": "no_interview_model",
                                "last_interview": now_iso(),
                                "interview_completed_at": str(time.time())  # Prevent re-interview loop
                            })
                            await websocket.send_json({
                                "type": "INTERVIEW_FAILED",
                                "node_id": node_id,
                                "reason": "no_interview_model",
                                "message": f"No interview model available. For Ollama mode, either specify 'interviewModel' in config or have a benchmark model (llama3:8b, mistral:7b, etc.). Your models: {node_models}",
                                "required_models": BENCHMARK_MODELS[:4]
                            })
                            continue
                        
                        if current_tier and current_tier not in ["failed", "pending"]:
                            print(f"[PIN INTERVIEW] Node {node_id} reconnected (tier: {current_tier}) - re-interviewing with {interview_model}")
                        else:
                            mode_label = "(OpenAI mode - harder exam)" if node_api_mode == "openai" else ""
                            print(f"[PIN INTERVIEW] New node {node_id} - initiating interview {mode_label} with model: {interview_model}")
                        
                        # Generate prompts based on API mode
                        prompts = generate_interview_prompts(5, api_mode=node_api_mode)
                        interview_id = f"interview_{secrets.token_urlsafe(16)}"
                        
                        # Store pending interview node_id and api_mode for result handling
                        pin_ws_manager.node_interviews[interview_id] = node_id
                        pin_ws_manager.interview_api_modes[interview_id] = node_api_mode
                        
                        await websocket.send_json({
                            "type": "INTERVIEW_REQUEST",
                            "interview_id": interview_id,
                            "node_id": node_id,
                            "model": interview_model,
                            "api_mode": node_api_mode,
                            "prompts": [
                                {
                                    "id": p["id"],
                                    "prompt": p["prompt"],
                                    "max_tokens": p["max_tokens"]
                                } for p in prompts
                            ],
                            "timeout_ms": INTERVIEW_TIMEOUT_SECONDS * 1000
                        })
                        prompt_count = len(prompts)
                        print(f"[PIN INTERVIEW] Sent interview {interview_id} ({prompt_count} prompts, {node_api_mode} mode) to node {node_id}")
                
                elif msg_type == "UPDATE_WALLET":
                    payout_address = msg.get("payout_address", "")
                    if payout_address:
                        # Validate BSC address format (0x + 40 hex chars)
                        if payout_address.startswith("0x") and len(payout_address) == 42:
                            storage.r.hset(f"aiconsult:pin:operators:{operator_id}", "payout_address", payout_address)
                            storage.r.hset(f"aiconsult:pin:operators:{operator_id}", "updated_at", now_iso())
                            await websocket.send_json({
                                "type": "UPDATE_WALLET_ACK",
                                "success": True,
                                "message": f"Payout wallet updated to {payout_address[:6]}...{payout_address[-4:]}"
                            })
                            print(f"[PIN WS] Operator {operator_id} updated payout wallet: {payout_address[:6]}...{payout_address[-4:]}")
                        else:
                            await websocket.send_json({
                                "type": "UPDATE_WALLET_ACK",
                                "success": False,
                                "message": "Invalid BSC address format (must be 0x + 40 hex characters)"
                            })
                    else:
                        await websocket.send_json({
                            "type": "UPDATE_WALLET_ACK",
                            "success": False,
                            "message": "No payout address provided"
                        })
                
                elif msg_type == "INFERENCE_RESPONSE":
                    request_id = msg.get("request_id")
                    result = msg.get("result")
                    print(f"[PIN WS] Received INFERENCE_RESPONSE for {request_id}")
                    if request_id:
                        pin_ws_manager.resolve_request(request_id, result)
                    else:
                        print(f"[PIN WS] WARNING: INFERENCE_RESPONSE missing request_id")
                
                elif msg_type == "INFERENCE_CHUNK":
                    # Streamed token. Deliberately NOT logged per chunk -- one
                    # line per token would bury every other log on this socket.
                    req_id = msg.get("request_id")
                    if req_id:
                        pin_ws_manager.push_chunk(
                            req_id, msg.get("index", 0), msg.get("delta", ""),
                            msg.get("kind", "content"))

                elif msg_type == "INFERENCE_ERROR":
                    request_id = msg.get("request_id")
                    error = msg.get("error", "Unknown error")
                    print(f"[PIN WS] Received INFERENCE_ERROR for {request_id}: {error}")
                    if request_id:
                        pin_ws_manager.resolve_request(request_id, {"error": error})

                elif msg_type == "TTS_RESPONSE":
                    request_id = msg.get("request_id")
                    result = msg.get("result")
                    size = (result or {}).get("bytes", "?")
                    print(f"[PIN WS] Received TTS_RESPONSE for {request_id} ({size} bytes)")
                    if request_id:
                        pin_ws_manager.resolve_request(request_id, result)
                    else:
                        print(f"[PIN WS] WARNING: TTS_RESPONSE missing request_id")

                elif msg_type == "TTS_ERROR":
                    request_id = msg.get("request_id")
                    error = msg.get("error", "Unknown error")
                    print(f"[PIN WS] Received TTS_ERROR for {request_id}: {error}")
                    if request_id:
                        pin_ws_manager.resolve_request(request_id, {"error": error})
                
                elif msg_type == "HEALTH":
                    health = msg.get("health", {})
                    target_key = f"aiconsult:pin:nodes:{node_id}" if node_id else f"aiconsult:pin:operators:{operator_id}"
                    storage.r.hset(target_key, mapping={
                        "current_load": str(health.get("current_load", 0)),
                        "capacity": str(health.get("capacity", 10)),
                        "gpu_memory_used": str(health.get("gpu_memory_used", 0)),
                        "gpu_memory_total": str(health.get("gpu_memory_total", 0))
                    })
                    await pin_ws_manager.heartbeat(client_id, node_id)
                
                elif msg_type == "INTERVIEW_RESULT":
                    interview_id = msg.get("interview_id")
                    results = msg.get("results", [])
                    model = msg.get("model", "unknown")
                    
                    # Get node_id and api_mode from the interview mappings
                    interviewed_node_id = pin_ws_manager.node_interviews.pop(interview_id, None)
                    interview_api_mode = pin_ws_manager.interview_api_modes.pop(interview_id, "ollama")
                    
                    print(f"[PIN INTERVIEW] Received INTERVIEW_RESULT for {interview_id} (node: {interviewed_node_id}, mode: {interview_api_mode}) with {len(results)} results")
                    
                    # Calculate quality tier using appropriate thresholds for API mode
                    tier_info = calculate_quality_tier(results, api_mode=interview_api_mode)
                    tier = tier_info["tier"]
                    
                    # Store tier info in Redis - use interviewed node or fallback to operator
                    if interviewed_node_id:
                        target_key = f"aiconsult:pin:nodes:{interviewed_node_id}"
                    else:
                        target_key = f"aiconsult:pin:operators:{operator_id}"
                    
                    storage.r.hset(target_key, mapping={
                        "quality_tier": tier,
                        "interview_accuracy": str(tier_info["accuracy"]),
                        "interview_tokens_per_sec": str(tier_info["avg_tokens_per_sec"]),
                        "interview_ttft_ms": str(tier_info["avg_ttft_ms"]),
                        "last_interview": now_iso(),
                        "interview_completed_at": str(time.time()),  # Unix timestamp for loop prevention
                        "interview_model": model,
                        "interview_api_mode": interview_api_mode
                    })
                    
                    # Store interview attempt
                    interview_key = f"aiconsult:pin:interview:{operator_id}"
                    storage.r.hincrby(interview_key, "attempts", 1)
                    storage.r.hset(interview_key, "last_attempt", now_iso())
                    storage.r.hset(interview_key, "last_tier", tier)
                    storage.r.expire(interview_key, 86400)  # Reset attempts after 24h
                    
                    node_label = f"node {interviewed_node_id}" if interviewed_node_id else f"operator {operator_id}"
                    print(f"[PIN INTERVIEW] {node_label} assigned tier: {tier} ({tier_info['reason']})")
                    
                    # Send acknowledgment with tier info
                    await websocket.send_json({
                        "type": "INTERVIEW_COMPLETE",
                        "interview_id": interview_id,
                        "node_id": interviewed_node_id,
                        "tier": tier,
                        "accuracy": tier_info["accuracy"],
                        "tokens_per_sec": tier_info["avg_tokens_per_sec"],
                        "reason": tier_info["reason"]
                    })
                    
                    # Resolve the pending future if waiting
                    if interview_id in pin_ws_manager.pending_requests:
                        pin_ws_manager.resolve_request(interview_id, tier_info)
                    
                    # Disconnect failed operators - they cannot serve production traffic
                    if tier == "failed":
                        print(f"[PIN INTERVIEW] Disconnecting failed operator {operator_id} - node cannot serve traffic")
                        await websocket.close(code=4003, reason="Interview failed - upgrade hardware and retry")
                        return  # Exit the WebSocket handler
                
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "PING"})
                await pin_ws_manager.heartbeat(client_id, node_id)
    
    except WebSocketDisconnect:
        print(f"[PIN WS] Client {client_id} disconnected normally")
    except Exception as e:
        print(f"[PIN WS] Error: {e}")
    finally:
        if client_id:
            pin_ws_manager.disconnect(client_id)


# =============================================================================
# Admin Endpoints for PIN Configuration
# =============================================================================

@router.get("/admin/thresholds")
async def get_pin_thresholds(request: Request, user: User = Depends(get_current_user_required)):
    """Get current PIN interview tier thresholds (admin only)."""
    if user.role not in ["super_admin", "manager"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    thresholds = get_tier_thresholds()
    return {
        "thresholds": thresholds,
        "defaults": DEFAULT_TIER_THRESHOLDS,
        "descriptions": {
            "verified_accuracy": "Minimum accuracy % for verified tier",
            "verified_tokens_per_sec": "Minimum tokens/sec for verified tier",
            "standard_accuracy": "Minimum accuracy % for standard tier",
            "standard_tokens_per_sec": "Minimum tokens/sec for standard tier",
            "min_tokens_per_sec": "Absolute minimum tokens/sec (below = failed)"
        }
    }


class UpdateThresholdsRequest(PydanticBaseModel):
    verified_accuracy: Optional[float] = None
    verified_tokens_per_sec: Optional[float] = None
    standard_accuracy: Optional[float] = None
    standard_tokens_per_sec: Optional[float] = None
    min_tokens_per_sec: Optional[float] = None


@router.put("/admin/thresholds")
async def update_pin_thresholds(
    request: Request,
    body: UpdateThresholdsRequest,
    user: User = Depends(get_current_user_required)
):
    """Update PIN interview tier thresholds (admin only)."""
    if user.role not in ["super_admin", "manager"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    current = get_tier_thresholds()
    
    # Update only provided values
    updates = {}
    if body.verified_accuracy is not None:
        if not (0 <= body.verified_accuracy <= 100):
            raise HTTPException(status_code=400, detail="verified_accuracy must be 0-100")
        updates["verified_accuracy"] = str(body.verified_accuracy)
    if body.verified_tokens_per_sec is not None:
        if body.verified_tokens_per_sec < 0:
            raise HTTPException(status_code=400, detail="verified_tokens_per_sec must be >= 0")
        updates["verified_tokens_per_sec"] = str(body.verified_tokens_per_sec)
    if body.standard_accuracy is not None:
        if not (0 <= body.standard_accuracy <= 100):
            raise HTTPException(status_code=400, detail="standard_accuracy must be 0-100")
        updates["standard_accuracy"] = str(body.standard_accuracy)
    if body.standard_tokens_per_sec is not None:
        if body.standard_tokens_per_sec < 0:
            raise HTTPException(status_code=400, detail="standard_tokens_per_sec must be >= 0")
        updates["standard_tokens_per_sec"] = str(body.standard_tokens_per_sec)
    if body.min_tokens_per_sec is not None:
        if body.min_tokens_per_sec < 0:
            raise HTTPException(status_code=400, detail="min_tokens_per_sec must be >= 0")
        updates["min_tokens_per_sec"] = str(body.min_tokens_per_sec)
    
    if updates:
        # Initialize with current values first
        for k, v in current.items():
            storage.r.hset("aiconsult:pin:config:thresholds", k, str(v))
        # Then apply updates
        storage.r.hset("aiconsult:pin:config:thresholds", mapping=updates)
    
    return {
        "success": True,
        "thresholds": get_tier_thresholds()
    }


@router.post("/admin/thresholds/reset")
async def reset_pin_thresholds(request: Request, user: User = Depends(get_current_user_required)):
    """Reset PIN thresholds to defaults (admin only)."""
    if user.role not in ["super_admin", "manager"]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    storage.r.delete("aiconsult:pin:config:thresholds")
    
    return {
        "success": True,
        "thresholds": DEFAULT_TIER_THRESHOLDS
    }


# =============================================================================
# PIN SSE transport — persistent operator downlink, independent HTTP uplink.
# Additive to the existing WebSocket path; both run simultaneously.
# =============================================================================

def _unwrap_result_envelope(result):
    """The completion itself, however many envelopes the daemon wrapped it in.

    The SSE daemon routes results through `send_uplink_json`, which posts

        {"request_id": ..., "result": <the WHOLE parsed envelope>}

    so the server's own `body["result"]` is not a completion but another
    envelope: `{"type": "INFERENCE_RESPONSE", "request_id": ..., "result": ...}`.
    `pin_chat_completion` then read `choices` off that and got `[]`, and
    `public_api` did `choices[0]` on it:

        [PIN SERVICE] Inference result received: ['request_id', 'result', 'type']
        Provider API error: list index out of range
        POST /v1/chat/completions 500

    The daemon is fixed separately, but it must be fixed HERE too and first:
    operators upgrade on their own schedule, and a gateway that only works
    with the newest daemon is a gateway that breaks every operator who has
    not rebuilt yet. Unwrapping is safe in both directions — a bare completion
    has no `type` key to strip.

    Bounded, because "unwrap until it looks right" on attacker-adjacent input
    is how you write an infinite loop.
    """
    for _ in range(3):
        if not isinstance(result, dict):
            return result
        if "type" in result and isinstance(result.get("result"), dict):
            result = result["result"]
            continue
        return result
    return result


class _StaleStreamCleanup(Exception):
    """Control-flow marker: this stream is a corpse, skip the online-set eviction."""


def sse_transport_lease_seconds() -> int:
    from api.services.pin_sse_transport import LEASE_SECONDS
    return LEASE_SECONDS


@router.get("/stream/connect")
async def pin_sse_connect(
    request: Request,
    client_id: str = Header(..., alias="X-PIN-Client-Id"),
    timestamp: str = Header(..., alias="X-PIN-Timestamp"),
    signature: str = Header(..., alias="X-PIN-Signature"),
    last_event_id: Optional[str] = Header(None, alias="Last-Event-ID"),
):
    """Open a persistent SSE downlink for a PIN operator.

    Same signature contract as the WebSocket AUTH frame:
        signature = sha256(client_id + timestamp + sha256(api_secret))

    A new connection for the same operator revokes the previous lease, so
    only one downlink per operator is live at a time. Heartbeats and inference
    chunks are queued onto this stream; the operator's HTTP uplink calls
    /stream/result and /stream/heartbeat to push completions and lease
    renewals.
    """
    ts_diff = abs(time.time() - float(timestamp))
    if ts_diff > 300:
        raise HTTPException(status_code=401, detail="Timestamp expired")

    auth_result = verify_client_signature(client_id, timestamp, signature)
    if not auth_result:
        raise HTTPException(status_code=403, detail="Invalid credentials")

    operator_id, _ = auth_result
    token = sse_transport.open(operator_id, client_id)

    storage.r.sadd("aiconsult:pin:operators:online", operator_id)
    storage.r.setex(f"aiconsult:pin:heartbeats:{operator_id}", 300, str(time.time()))
    storage.r.hset(f"aiconsult:pin:operators:{operator_id}", "last_connected", datetime.utcnow().isoformat())

    # Same fact as a WebSocket accept: this process now holds the operator.
    # Without the bus subscription, #174's holders() stays zero and every
    # other worker still reports "Connected operators in this process: none".
    bus_task = asyncio.create_task(bus.serve(operator_id, pin_ws_manager._serve_bus_job))

    async def event_stream():
        session = sse_transport.get(token)
        if not session:
            return
        yield ": connected operator={}\n\n".format(operator_id)
        try:
            while True:
                try:
                    event = await asyncio.wait_for(session.events.get(), timeout=45.0)
                    payload = json.dumps(event)
                    yield "event: {}\ndata: {}\n\n".format(event.get("type", "message"), payload)
                except asyncio.TimeoutError:
                    yield ": keepalive {}\n\n".format(int(time.time()))
        except asyncio.CancelledError:
            pass
        finally:
            bus_task.cancel()
            still_ours = sse_transport.is_current_lease(operator_id, token)
            sse_transport.close(token)
            try:
                # ONLY IF THIS IS STILL THE OPERATOR'S LEASE. A reconnect
                # revokes the old lease; the old generator then finishes and
                # its cleanup used to remove the operator that had just come
                # back, leaving `pin:operators:online` empty while the daemon
                # heartbeated happily. The corpse must not evict the living.
                if not still_ours:
                    print(f"[PIN SSE] stale stream for {operator_id} ended; "
                          f"leaving online set alone (operator reconnected)")
                    raise _StaleStreamCleanup()
                storage.r.srem("aiconsult:pin:operators:online", operator_id)
                storage.r.delete(f"aiconsult:pin:heartbeats:{operator_id}")
            except _StaleStreamCleanup:
                pass
            except Exception as exc:  # noqa: BLE001
                print(f"[PIN SSE] online-set cleanup failed for {operator_id}: {exc}")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-PIN-Stream-Token": token,
        },
    )


@router.post("/stream/heartbeat")
async def pin_sse_heartbeat(
    request: Request,
    client_id: str = Header(..., alias="X-PIN-Client-Id"),
    timestamp: str = Header(..., alias="X-PIN-Timestamp"),
    signature: str = Header(..., alias="X-PIN-Signature"),
    stream_token: str = Header(..., alias="X-PIN-Stream-Token"),
    models: Optional[str] = Header(None, alias="X-PIN-Models"),
):
    """Renew the SSE lease and re-assert online status.

    Reuses the WebSocket signature contract. `models` is an optional
    comma-separated list to refresh the advertised model set without
    triggering a quality interview.
    """
    auth_result = verify_client_signature(client_id, timestamp, signature)
    if not auth_result:
        raise HTTPException(status_code=403, detail="Invalid credentials")

    operator_id, _ = auth_result
    # ANY WORKER MAY ANSWER THIS. The lease lives in Redis precisely because
    # the balancer sends this request wherever it likes; requiring the holding
    # worker meant (N-1)/N of heartbeats 404'd before reaching the online-set
    # write below, and the reaper then evicted a connected operator.
    leased_operator = sse_transport.renew(
        stream_token, models=models.split(",") if models else None
    )
    if leased_operator != operator_id:
        raise HTTPException(status_code=404, detail="Stream lease not found")
    lease_until = time.time() + sse_transport_lease_seconds()

    storage.r.sadd("aiconsult:pin:operators:online", operator_id)
    storage.r.setex(f"aiconsult:pin:heartbeats:{operator_id}", 300, str(time.time()))

    # X-PIN-Models IS A REPAIR, NOT A DECORATION.
    #
    # The daemon re-asserts its model set on every fourth heartbeat — roughly
    # once a minute — precisely so a model index that drifted comes back
    # without a restart. That header reached `sse_transport.heartbeat`, which
    # keeps it in the in-process lease, and went no further: Redis never saw
    # it. So the one mechanism designed to heal index drift healed nothing,
    # and drift lasted until someone restarted the daemon.
    #
    # The WebSocket MODEL_LIST branch has written these keys all along. This
    # is the SSE path catching up to its own fallback.
    if models:
        advertised = [m.strip() for m in models.split(",") if m.strip()]
        for model in advertised:
            storage.r.sadd(f"aiconsult:pin:models:{model}", operator_id)
            storage.r.sadd("aiconsult:pin:models:available", model)
        if advertised:
            storage.r.hset(
                f"aiconsult:pin:operators:{operator_id}",
                "models", json.dumps(advertised),
            )

    return {"type": "HEARTBEAT_ACK", "operator_id": operator_id, "lease_until": lease_until}


@router.post("/stream/register")
async def pin_sse_register_node(
    request: Request,
    client_id: str = Header(..., alias="X-PIN-Client-Id"),
    timestamp: str = Header(..., alias="X-PIN-Timestamp"),
    signature: str = Header(..., alias="X-PIN-Signature"),
    stream_token: str = Header(..., alias="X-PIN-Stream-Token"),
    body: dict = Depends(lambda: None),
):
    """Register or update a node's models over HTTP, mirroring the WebSocket
    REGISTER_NODE path. Queues the ack onto the operator's SSE downlink."""
    auth_result = verify_client_signature(client_id, timestamp, signature)
    if not auth_result:
        raise HTTPException(status_code=403, detail="Invalid credentials")

    operator_id, _ = auth_result
    # Shared-lease check: registration is an uplink and lands on any worker.
    # Requiring the holding worker is why model advertisement only landed by
    # luck, and why the indexes drifted.
    if sse_transport.resolve(stream_token) != operator_id:
        raise HTTPException(status_code=404, detail="Stream lease not found")

    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    node_alias = payload.get("alias", "Default Node")
    node_models = payload.get("models", [])
    node_capacity = payload.get("capacity", 10)
    node_region = payload.get("region", "global")
    node_api_mode = payload.get("apiMode", "ollama")
    node_price = payload.get("pricePerThousandTokens", 0.001)
    node_interview_model = payload.get("interviewModel")

    result = storage.pin_find_or_create_node(
        operator_id=operator_id, alias=node_alias, models=node_models,
        capacity=node_capacity, region=node_region,
    )
    node_data = result["node"]
    node_id = node_data.get("node_id") or node_data.get("id")
    target_key = f"aiconsult:pin:nodes:{node_id}"
    storage.r.hset(target_key, "api_mode", node_api_mode)
    storage.r.hset(target_key, "price_per_thousand_tokens", str(node_price))
    storage.r.setex(f"aiconsult:pin:node_heartbeats:{node_id}", 300, str(time.time()))
    for model in node_models:
        storage.r.sadd(f"aiconsult:pin:models:{model}", operator_id)
        storage.r.sadd("aiconsult:pin:models:available", model)

    sse_transport.queue(operator_id, {
        "type": "REGISTER_NODE_ACK",
        "node_id": node_id,
        "alias": node_data.get("alias", node_alias),
        "models": node_models,
        "api_mode": node_api_mode,
        "created": result["created"],
        "message": f"Node {'registered' if result['created'] else 'updated'} successfully",
    })
    return {"queued": True, "operator_id": operator_id, "node_id": node_id}


@router.post("/stream/result")
async def pin_sse_inference_result(
    request: Request,
    client_id: str = Header(..., alias="X-PIN-Client-Id"),
    timestamp: str = Header(..., alias="X-PIN-Timestamp"),
    signature: str = Header(..., alias="X-PIN-Signature"),
    stream_token: str = Header(..., alias="X-PIN-Stream-Token"),
):
    """Receive a completed inference result from the operator over HTTP.

    The body is the existing INFERENCE_RESPONSE payload. The server resolves
    the pending request exactly as the WebSocket path does, then queues an
    acknowledgment onto the SSE downlink.
    """
    auth_result = verify_client_signature(client_id, timestamp, signature)
    if not auth_result:
        raise HTTPException(status_code=403, detail="Invalid credentials")

    operator_id, _ = auth_result
    # Shared-lease check: registration is an uplink and lands on any worker.
    # Requiring the holding worker is why model advertisement only landed by
    # luck, and why the indexes drifted.
    if sse_transport.resolve(stream_token) != operator_id:
        raise HTTPException(status_code=404, detail="Stream lease not found")

    body = await request.json()
    request_id = body.get("request_id")
    result = _unwrap_result_envelope(body.get("result"))
    if request_id:
        # Same channel the chunks rode, so the final cannot overtake tokens
        # still in flight -- pub/sub preserves per-channel order, a local
        # shortcut would not. Zero subscribers means nobody holds a stream
        # relay for this request, which is every non-streaming request (its
        # future lives in pending_requests, resolved locally or via the
        # operator bus reply) and every pre-bus fallback: resolve locally,
        # exactly as before.
        if bus.publish_stream_event(request_id, {"type": "final", "result": result}) == 0:
            pin_ws_manager.resolve_request(request_id, result)

    sse_transport.queue(operator_id, {
        "type": "RESULT_ACK",
        "request_id": request_id,
    })
    return {"queued": True, "request_id": request_id}


@router.post("/stream/chunk")
async def pin_sse_inference_chunk(
    request: Request,
    client_id: str = Header(..., alias="X-PIN-Client-Id"),
    timestamp: str = Header(..., alias="X-PIN-Timestamp"),
    signature: str = Header(..., alias="X-PIN-Signature"),
    stream_token: str = Header(..., alias="X-PIN-Stream-Token"),
):
    """Receive a streamed inference chunk from the operator over HTTP.

    The chunk is pushed onto the consumer's SSE stream via
    pin_ws_manager.push_chunk, so the downstream SSE path reuses the exact
    same queue and billing discipline as the WebSocket transport.
    """
    auth_result = verify_client_signature(client_id, timestamp, signature)
    if not auth_result:
        raise HTTPException(status_code=403, detail="Invalid credentials")

    operator_id, _ = auth_result
    # Shared-lease check: registration is an uplink and lands on any worker.
    # Requiring the holding worker is why model advertisement only landed by
    # luck, and why the indexes drifted.
    if sse_transport.resolve(stream_token) != operator_id:
        raise HTTPException(status_code=404, detail="Stream lease not found")

    body = await request.json()
    req_id = body.get("request_id")
    if req_id:
        # This POST landed on a kernel-chosen worker; the consumer's queue
        # lives in the worker holding the SSE response. Publish to the
        # request's stream channel so the chunk reaches that worker WHEREVER
        # it is -- the daemon's keep-alive connection pins every uplink to
        # one worker, so without this the consumer saw all chunks or none.
        # Zero subscribers means no relay anywhere (single-process deploy,
        # Redis down, or the waiter is gone): fall back to the local queue,
        # which is exactly the pre-bus behavior including its deliberate
        # drop of unknown request_ids.
        event = {
            "type": "chunk",
            "index": body.get("index", 0),
            "delta": body.get("delta", ""),
            "kind": body.get("kind", "content"),
        }
        if bus.publish_stream_event(req_id, event) == 0:
            pin_ws_manager.push_chunk(
                req_id, event["index"], event["delta"], event["kind"],
            )
    return {"queued": True, "request_id": req_id}
