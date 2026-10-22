"""PIN data models — extracted verbatim from api/models/schemas.py.

Source: aiassistsecure/aias @ march_2026 (58a16643993188898ca8b51a29a9ffa771e64755), classes listed in
EXTRACTION_MAP.md. Class bodies are unedited; the rest of the platform's
schemas (workspaces, billing, licensing, …) are not part of the PIN slice.
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Literal, List, Dict, Any
from datetime import datetime
from enum import Enum
import uuid


class UserRole(str, Enum):
    CLIENT = "client"
    RESELLER = "reseller"
    MANAGER = "manager"
    SUPER_ADMIN = "super_admin"



class PlanType(str, Enum):
    FREE = "free"
    BASIC = "basic"
    PRO = "pro"
    ENTERPRISE = "enterprise"



class UserBase(BaseModel):
    email: str
    display_name: str
    role: UserRole = UserRole.CLIENT
    plan: PlanType = PlanType.FREE



class UserCreate(UserBase):
    password: str
    organization_id: Optional[str] = None



class UserRegister(BaseModel):
    email: str
    password: str
    display_name: str
    register_as_reseller: bool = False
    referral_code: Optional[str] = None
    client_ip: Optional[str] = None



class User(UserBase):
    id: str
    organization_id: Optional[str] = None
    active_environment_id: Optional[str] = None
    is_active: bool = True
    email_verified: bool = False
    created_at: str



class UserLogin(BaseModel):
    email: str
    password: str



class ChatMessage(BaseModel):
    role: str
    content: Optional[str] = None
    tool_calls: Optional[List[dict]] = None
    tool_call_id: Optional[str] = None
    name: Optional[str] = None



class ChatCompletionChoice(BaseModel):
    index: int
    message: ChatMessage
    finish_reason: str



class ChatCompletionUsage(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int



class OperatorStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    OFFLINE = "offline"



class PinJobStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"



class OperatorRegion(str, Enum):
    US_EAST = "us-east"
    US_WEST = "us-west"
    EU_WEST = "eu-west"
    EU_CENTRAL = "eu-central"
    ASIA_EAST = "asia-east"
    ASIA_SOUTH = "asia-south"
    GLOBAL = "global"



class PinOperator(BaseModel):
    id: str
    user_id: str
    name: str
    endpoint: Optional[str] = None  # None = WebSocket-only mode (secure)
    api_key_hash: Optional[str] = None
    status: OperatorStatus = OperatorStatus.PENDING
    models: List[str] = []
    capacity: int = 10
    current_load: int = 0
    pricing_per_1k_tokens: float = 0.5
    region: OperatorRegion = OperatorRegion.GLOBAL
    connection_mode: str = "websocket"  # "websocket" (secure) or "http" (legacy)
    payout_address: Optional[str] = None  # BSC wallet address for USDT payouts
    created_at: str
    updated_at: str



class PinOperatorCreate(BaseModel):
    name: str
    endpoint: Optional[str] = None  # Optional - WebSocket-only if not provided
    models: List[str] = []
    capacity: int = 10
    pricing_per_1k_tokens: float = 0.5
    region: OperatorRegion = OperatorRegion.GLOBAL
    payout_address: Optional[str] = None  # BSC wallet for USDT payouts



class PinOperatorUpdate(BaseModel):
    name: Optional[str] = None
    endpoint: Optional[str] = None
    models: Optional[List[str]] = None
    capacity: Optional[int] = None
    pricing_per_1k_tokens: Optional[float] = None
    region: Optional[OperatorRegion] = None
    status: Optional[OperatorStatus] = None
    payout_address: Optional[str] = None  # BSC wallet for USDT payouts



class PinOperatorRegistrationResponse(BaseModel):
    operator_id: str
    api_key: str
    client_id: str
    status: OperatorStatus
    message: str



class PinReputation(BaseModel):
    operator_id: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: int = 0
    avg_latency_ms: float = 0.0
    success_rate: float = 1.0
    score: float = 0.5
    last_updated: str



class PinHeartbeat(BaseModel):
    operator_id: str
    current_load: int = 0
    capacity: int = 10
    models_available: List[str] = []
    timestamp: str



class PinHeartbeatResponse(BaseModel):
    acknowledged: bool
    next_heartbeat_due: int = 30



class PinJob(BaseModel):
    id: str
    user_id: str
    operator_id: Optional[str] = None
    model: str
    status: PinJobStatus = PinJobStatus.PENDING
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    credits_charged: float = 0.0
    latency_ms: int = 0
    error: Optional[str] = None
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None



class PinBillingUser(BaseModel):
    user_id: str
    credits_balance: float = 0.0
    total_spent: float = 0.0
    last_transaction: Optional[str] = None



class PinBillingOperator(BaseModel):
    operator_id: str
    earnings_balance: float = 0.0
    total_earned: float = 0.0
    total_paid_out: float = 0.0
    last_payout: Optional[str] = None



class PinTransaction(BaseModel):
    id: str
    job_id: str
    user_id: str
    operator_id: str
    amount: float
    operator_share: float
    protocol_fee: float
    tokens: int
    model: str
    timestamp: str



class PinWithdrawalRequest(BaseModel):
    amount: float



class PinWalletUpdate(BaseModel):
    bsc_address: str



class PinWithdrawal(BaseModel):
    id: str
    operator_id: str
    amount: float
    bsc_address: str
    status: str = "pending"
    created_at: str
    processed_at: Optional[str] = None
    tx_hash: Optional[str] = None
    admin_notes: Optional[str] = None



class PinOperatorEarnings(BaseModel):
    balance: float
    pending_payout: float = 0.0
    total_earned: float
    total_paid_out: float
    earnings_24h: float = 0.0
    earnings_7d: float = 0.0
    earnings_30d: float = 0.0



class PinOperatorStatus(BaseModel):
    operator_id: str
    status: OperatorStatus
    online: bool
    reputation_score: float
    total_requests: int
    success_rate: float
    avg_latency_ms: float
    earnings_balance: float
    total_earned: float



class PinNetworkStatus(BaseModel):
    status: str = "healthy"
    total_operators: int = 0
    online_operators: int = 0
    total_capacity: int = 0
    models: List[str] = []
    requests_24h: int = 0
    tokens_24h: int = 0
    avg_latency_ms: float = 0.0



class PinOperatorPublic(BaseModel):
    id: str
    name: str
    region: OperatorRegion
    models: List[str]
    status: OperatorStatus
    reputation_score: float
    pricing_per_1k_tokens: float



class PinChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    temperature: Optional[float] = 0.7
    # 1024 was far too low a default for a REASONING model. muse-local spends
    # most of its budget in the thinking phase, so a caller who omitted
    # max_tokens got cut off mid-deliberation with an empty content channel --
    # and no way to tell that from the model simply having nothing to say.
    # Callers who want a small ceiling still pass one explicitly.
    max_tokens: Optional[int] = 8192
    stream: bool = False
    # TOOL CALLING, absent by default.
    #
    # The generic ChatCompletionRequest has carried these since the beginning;
    # PIN's did not, so a caller could describe tools to every hosted provider
    # and not to their own operator. That gap forced structured extraction to be
    # done as PROSE INSTRUCTIONS in the prompt, and paid for it in exactly the
    # ways prose does: invented predicates, stitched quotes, malformed blocks,
    # and a model rehearsing the rules instead of applying them.
    #
    # Optional and omitted-when-absent (see pin_service): a request without
    # tools must stay byte-identical to what it sends today.
    tools: Optional[List[dict]] = Field(
        default=None, description="List of tools/functions the operator's model may call")
    tool_choice: Optional[str] = Field(
        default=None, description="'auto', 'none', or a specific tool name")



class PinChatCompletionMetadata(BaseModel):
    operator_id: str
    latency_ms: int
    credits_charged: float



class PinChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: ChatCompletionUsage
    pin_metadata: PinChatCompletionMetadata
