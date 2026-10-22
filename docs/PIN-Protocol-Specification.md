# PIN Protocol Specification

## P2P Inference Network (PIN) v2.1.0

**Status:** Production  
**Last Updated:** January 2026

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Authentication](#authentication)
4. [Connection Lifecycle](#connection-lifecycle)
5. [Message Protocol](#message-protocol)
6. [Node Registration](#node-registration)
7. [Quality Interview System](#quality-interview-system)
8. [Inference Routing](#inference-routing)
9. [Data Structures](#data-structures)
10. [Redis Schema](#redis-schema)
11. [Security Model](#security-model)
12. [Economics](#economics)

---

## Overview

PIN is a decentralized inference network that connects GPU operators running local LLM servers to cloud consumers via authenticated WebSocket tunnels. Operators earn credits for each inference request processed.

### Key Principles

- **WebSocket-Only Security**: No public endpoint exposure required
- **Operator-Level Authentication**: Single API key manages all nodes
- **Daemon-Based Registration**: Nodes auto-register on daemon connect
- **Per-Node Quality Interviews**: Automated tier assignment via benchmarking
- **Proof-of-Response Economics**: Pay only for successful completions

### Supported Backends

| API Mode | Endpoint Format | Compatible Software |
|----------|-----------------|---------------------|
| `ollama` | `/api/chat`, `/api/tags` | Ollama |
| `openai` | `/v1/chat/completions`, `/v1/models` | vLLM, TGI, LMStudio, LocalAI |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PIN Cloud (AiAS)                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐   │
│  │   WebSocket  │  │    Redis     │  │   Inference Router       │   │
│  │   Gateway    │←→│  (State)     │←→│   (Model→Node Mapping)   │   │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘   │
│         ↑                                        ↓                   │
└─────────│────────────────────────────────────────│───────────────────┘
          │ WSS (Authenticated)                    │ /v1/chat/completions
          │                                        ↓
┌─────────│────────────────────────────────────────────────────────────┐
│         │              Operator Environment                          │
│  ┌──────┴──────┐                                                     │
│  │ pin-clientd │ ←── config.json (clientId, apiSecret, nodes[])     │
│  │  (Daemon)   │                                                     │
│  └──────┬──────┘                                                     │
│         │ HTTP (localhost)                                           │
│         ↓                                                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │   Ollama     │  │    vLLM      │  │  LMStudio    │  ...          │
│  │ :11434       │  │   :8000      │  │   :1234      │               │
│  └──────────────┘  └──────────────┘  └──────────────┘               │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Authentication

### Operator Registration

1. User registers at `/pin/join` with operator name
2. Server generates:
   - `operator_id`: `op_{uuid}` - Unique operator identifier
   - `api_secret`: Cryptographic secret (shown once)
3. Credentials stored in Redis

### Daemon Authentication

The daemon authenticates using HMAC-SHA256 signed messages:

```
Signature = HMAC-SHA256(client_id + timestamp, api_secret)
```

**AUTH Message:**
```json
{
  "type": "AUTH",
  "client_id": "op_abc123",
  "timestamp": "1704067200",
  "signature": "a1b2c3d4e5f6..."
}
```

**Server Validation:**
1. Lookup operator by `client_id`
2. Verify timestamp within 5-minute window
3. Compute expected signature and compare
4. On success: `AUTH_SUCCESS` with operator details
5. On failure: `ERROR` with reason, connection closed

---

## Connection Lifecycle

```
Daemon                                    Server
   │                                         │
   │──────── WSS Connect ───────────────────→│
   │                                         │
   │──────── AUTH {client_id, sig} ─────────→│
   │                                         │ Validate signature
   │←─────── AUTH_SUCCESS {operator_id} ─────│
   │                                         │
   │──────── REGISTER_NODE {alias, ...} ────→│
   │                                         │ Create/update node
   │←─────── REGISTER_NODE_ACK {node_id} ────│
   │                                         │
   │←─────── INTERVIEW_REQUEST {prompts} ────│
   │                                         │ Quality check
   │──────── INTERVIEW_RESULT {results} ────→│
   │                                         │
   │←─────── INTERVIEW_COMPLETE {tier} ──────│
   │                                         │
   │←─────── PING ───────────────────────────│
   │──────── PONG ──────────────────────────→│ (every 30s)
   │                                         │
   │←─────── INFERENCE_REQUEST {payload} ────│
   │                                         │ Route user request
   │──────── INFERENCE_RESPONSE {result} ───→│
   │                                         │
```

---

## Message Protocol

All messages are JSON over WebSocket with a `type` field.

### Client → Server Messages

| Type | Description | Fields |
|------|-------------|--------|
| `AUTH` | Authenticate operator | `client_id`, `timestamp`, `signature` |
| `REGISTER_NODE` | Register/update a node | `alias`, `models[]`, `capacity`, `region` |
| `UPDATE_WALLET` | Update payout address | `payout_address` |
| `PONG` | Heartbeat response | - |
| `HEARTBEAT` | Explicit heartbeat | - |
| `INTERVIEW_RESULT` | Quality test results | `interview_id`, `model`, `results[]` |
| `INFERENCE_RESPONSE` | Completion result | `request_id`, `result` |
| `INFERENCE_ERROR` | Completion failed | `request_id`, `error` |

### Server → Client Messages

| Type | Description | Fields |
|------|-------------|--------|
| `AUTH_SUCCESS` | Authentication passed | `operator_id`, `node_id`, `message` |
| `ERROR` | Authentication/operation failed | `message` |
| `REGISTER_NODE_ACK` | Node registered | `node_id`, `alias`, `models[]`, `created` |
| `UPDATE_WALLET_ACK` | Wallet updated | `success`, `message` |
| `PING` | Heartbeat request | - |
| `HEARTBEAT_ACK` | Heartbeat confirmed | - |
| `INTERVIEW_REQUEST` | Quality test prompts | `interview_id`, `node_id`, `model`, `prompts[]` |
| `INTERVIEW_COMPLETE` | Tier assigned | `interview_id`, `node_id`, `tier`, `accuracy`, `tokens_per_sec` |
| `INFERENCE_REQUEST` | Completion request | `request_id`, `payload` |

---

## Node Registration

### REGISTER_NODE Message

```json
{
  "type": "REGISTER_NODE",
  "alias": "GPU-A100-1",
  "models": ["llama3.2:latest", "mistral:7b"],
  "capacity": 10,
  "region": "us-east"
}
```

### Server Processing

1. **Find existing**: Search operator's nodes by alias
2. **If found**: Update models, capacity, region, timestamps
3. **If new**: Create node with unique ID
4. **Duplicate alias**: Auto-suffix with `-a`, `-b`, `-c`
5. **Index models**: Add to model→node routing index
6. **Trigger interview**: If models present, send quality test

### REGISTER_NODE_ACK Response

```json
{
  "type": "REGISTER_NODE_ACK",
  "node_id": "pin_node_xyz789",
  "alias": "GPU-A100-1",
  "models": ["llama3.2:latest", "mistral:7b"],
  "created": true,
  "message": "Node registered successfully"
}
```

### Regions

| ID | Name | Example Locations |
|----|------|-------------------|
| `us-east` | US East | Virginia, NYC, AWS us-east-1 |
| `us-west` | US West | California, Oregon, AWS us-west-2 |
| `eu-west` | EU West | Ireland, UK, AWS eu-west-1 |
| `eu-central` | EU Central | Frankfurt, Amsterdam |
| `asia-pacific` | Asia Pacific | Tokyo, Singapore |
| `global` | Global | Unknown/multi-region |

---

## Quality Interview System

### Purpose

Automated quality assurance to:
- Verify LLM functionality before production traffic
- Measure response latency and throughput
- Assign quality tier affecting routing priority

### Interview Flow

```
Server                                    Daemon
   │                                         │
   │── INTERVIEW_REQUEST ───────────────────→│
   │   {interview_id, model, prompts[]}      │
   │                                         │ Execute prompts locally
   │                                         │ Measure timing
   │←── INTERVIEW_RESULT ────────────────────│
   │    {results[{response, ttft_ms, ...}]}  │
   │                                         │
   │ Validate accuracy                       │
   │ Calculate speed metrics                 │
   │ Assign tier                             │
   │                                         │
   │── INTERVIEW_COMPLETE ──────────────────→│
   │   {tier, accuracy, tokens_per_sec}      │
```

### Interview Prompts

Each interview includes 5 prompts testing:
- Factual accuracy (known-answer questions)
- Instruction following (exact format tests)
- Coherence (gibberish rejection)

```json
{
  "type": "INTERVIEW_REQUEST",
  "interview_id": "interview_abc123",
  "node_id": "pin_node_xyz",
  "model": "llama3.2:latest",
  "prompts": [
    {
      "id": "factual_1",
      "prompt": "What is 2 + 2? Reply with just the number.",
      "max_tokens": 10
    }
  ],
  "timeout_ms": 60000
}
```

### Prompt Results

```json
{
  "type": "INTERVIEW_RESULT",
  "interview_id": "interview_abc123",
  "model": "llama3.2:latest",
  "results": [
    {
      "prompt_id": "factual_1",
      "response": "4",
      "ttft_ms": 45,
      "total_ms": 120,
      "tokens_generated": 1,
      "error": null
    }
  ]
}
```

### Metrics Collected

| Metric | Description |
|--------|-------------|
| `ttft_ms` | Time to first token (responsiveness) |
| `total_ms` | End-to-end completion time |
| `tokens_generated` | Output token count |
| `tokens_per_sec` | Throughput (tokens/total_ms*1000) |

### Quality Tiers

| Tier | Accuracy | Speed | Routing Priority |
|------|----------|-------|------------------|
| `verified` | >90% | >20 tok/s | Highest |
| `standard` | >70% | >10 tok/s | Normal |
| `slow` | >70% | <10 tok/s | Low (budget) |
| `failed` | <70% | Any | Blocked |

### Anti-Gaming Measures

- Randomized prompt variants per interview
- Server-side timestamp validation
- Periodic spot-checks during operation
- Anomaly detection for suspicious patterns

### Retry Policy

- Failed nodes can retry after 1-hour cooldown
- Maximum 3 attempts per 24-hour period
- Passing resets attempt counter

---

## Inference Routing

### Request Flow

1. **User Request**: `/v1/chat/completions` with model
2. **Router**: Find online nodes with model
3. **Filter**: Exclude failed/offline nodes
4. **Rank**: Sort by tier, latency, load
5. **Select**: Pick best available node
6. **Forward**: Send INFERENCE_REQUEST via WebSocket
7. **Await**: Wait for INFERENCE_RESPONSE
8. **Return**: Forward response to user

### INFERENCE_REQUEST

```json
{
  "type": "INFERENCE_REQUEST",
  "request_id": "req_abc123xyz",
  "payload": {
    "model": "llama3.2:latest",
    "messages": [
      {"role": "user", "content": "Hello, how are you?"}
    ],
    "stream": false
  }
}
```

### INFERENCE_RESPONSE

```json
{
  "type": "INFERENCE_RESPONSE",
  "request_id": "req_abc123xyz",
  "result": {
    "id": "chatcmpl-xxx",
    "object": "chat.completion",
    "choices": [
      {
        "index": 0,
        "message": {"role": "assistant", "content": "I'm doing well!"},
        "finish_reason": "stop"
      }
    ],
    "usage": {
      "prompt_tokens": 10,
      "completion_tokens": 5,
      "total_tokens": 15
    }
  }
}
```

---

## Data Structures

### Daemon Config (config.json)

```json
{
  "clientId": "op_abc123",
  "apiSecret": "secret_xyz789",
  "payoutAddress": "0x1234567890abcdef1234567890abcdef12345678",
  "nodes": [
    {
      "alias": "GPU-1",
      "inferenceUri": "http://localhost:11434",
      "apiMode": "ollama",
      "region": "us-east",
      "capacity": 10
    }
  ]
}
```

### Node Fields (All Required)

| Field | Type | Description |
|-------|------|-------------|
| `alias` | string | Human-readable node name |
| `inferenceUri` | string | LLM server endpoint URL |
| `apiMode` | string | `ollama` or `openai` |
| `region` | string | Geographic region ID |
| `capacity` | integer | Max concurrent requests |

---

## Redis Schema

All keys use `aiconsult:` prefix.

### Operators

```
pin:operators:{operator_id}     → Hash
  id, user_id, name, api_secret_hash, status, quality_tier,
  total_earned, credits_balance, payout_address, created_at
```

### Nodes

```
pin:nodes:{node_id}             → Hash
  id, operator_id, alias, endpoint, client_id, models (JSON),
  capacity, current_load, region, status, is_primary,
  quality_tier, created_at, updated_at, last_seen

pin:operator:{operator_id}:nodes → Set of node_ids
pin:nodes:all                    → Set of all node_ids
pin:nodes:active                 → Set of active node_ids
pin:node_heartbeats:{node_id}    → String (TTL 5min)
```

### Model Index

```
pin:models:{model_name}          → Set of operator_ids
pin:models:available             → Set of all available models
pin:node_models:{model_name}     → Set of node_ids
```

### Interviews

```
pin:interview:{interview_id}     → Hash (interview state)
pin:operator:{id}:interviews     → Sorted set (history)
```

---

## Security Model

### No Public Endpoint Exposure

- Operators never expose LLM servers publicly
- All traffic tunnels through WebSocket
- Daemon initiates outbound connection only

### Authentication Chain

```
Operator Registration → API Secret → HMAC Signature → WebSocket Auth
```

### Heartbeat Liveness

- Server sends PING every 30 seconds
- Daemon must respond with PONG
- Missing 2 consecutive heartbeats → Node marked offline
- Heartbeat TTL: 5 minutes in Redis

### Rate Limiting

- Connection attempts: 10/minute per IP
- Interview retries: 3/24 hours
- Inference requests: Based on node capacity

---

## Economics

### Pricing

- Operators set price per 1,000 tokens
- Suggested range: $0.05 - $0.50

### Revenue Split

| Recipient | Share |
|-----------|-------|
| Operator | 67%+ |
| Protocol | Up to 33% |

*Protocol fee covers infrastructure, routing, and quality assurance. Operator share increases as network scales.*

### Billing Events

- Credits deducted only on successful `INFERENCE_RESPONSE`
- Failed/timeout requests generate no charges
- Token count from response `usage` field

### Payouts

- Minimum withdrawal: $10
- Currency: USDT on BSC (Binance Smart Chain)
- Manual processing (automated in future)

---

## Appendix: API Mode Compatibility

### Ollama API

**Model Discovery:**
```
GET /api/tags
Response: {"models": [{"name": "llama3.2:latest", ...}]}
```

**Chat Completion:**
```
POST /api/chat
Body: {"model": "...", "messages": [...], "stream": false}
Response: {"message": {"content": "..."}, "eval_count": 50, ...}
```

### OpenAI API

**Model Discovery:**
```
GET /v1/models
Response: {"data": [{"id": "llama3.2", ...}]}
```

**Chat Completion:**
```
POST /v1/chat/completions
Body: {"model": "...", "messages": [...]}
Response: {"choices": [{"message": {"content": "..."}}], "usage": {...}}
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.1.0 | Jan 2026 | Per-node inferenceUri/apiMode, OpenAI API mode support |
| 2.0.0 | Dec 2025 | Operator-level auth, daemon-based node registration |
| 1.0.0 | Nov 2025 | Initial release, node-level credentials |

---

**Document maintained by AiAssist Secure**  
**Protocol Implementation:** `pin-clientd` (Rust), `api/routes/pin.py` (Python/FastAPI)
