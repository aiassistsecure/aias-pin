# AiAS PIN — P2P Inference Network (server-side)

The server half of **PIN**, the peer-to-peer inference network behind
[aiassist.net](https://aiassist.net): independent operators register GPU
nodes, the network routes OpenAI-style chat completions to them, and
settlement (credits, a 10% protocol fee, operator earnings and
withdrawals) is handled atomically.

This repository is a **distillation for examination**: the PIN subsystem
extracted from AiAS, the private AiAssist monolith, and published under
the [Business Source License 1.1](LICENSE). How every file got here —
what is verbatim, what was extracted, and what was deliberately left
out — is documented in [EXTRACTION_MAP.md](EXTRACTION_MAP.md).

## How it works

```
client ──► POST /v1/chat/completions (via the AiAssist public API)
                │
                ▼
        pin_service.pin_chat_completion        api/services/pin_service.py
                │  selects an operator (reputation, price, load,
                │  live model index)             └─ storage.pin_select_operator
                ▼
        operator bus                            api/services/pin_operator_bus.py
          ├─ same process: the node's WebSocket, held by routes/pin.py
          ├─ other worker: Redis pub/sub dispatch + reply
          └─ SSE fallback: pin_sse_transport (leased, shared across workers)
                ▼
        operator's node (pin-clientd daemon — separate repository)
                ▼
        result settles atomically: LUA_PIN_JOB_COMPLETE debits the user,
        credits the operator minus protocol fee, records the transaction
```

Key pieces:

- **`api/routes/pin.py`** (4,113 lines) — operator/node registration,
  HMAC-signed daemon handshakes, heartbeat, the interview (node admission)
  flow, WebSocket connection managers for clients and operators, streaming,
  TTS relay, wallet/pricing/withdrawals, network status.
- **`api/services/pin_operator_bus.py`** — solves the multi-worker problem:
  a node's socket lives in one worker's memory, so requests landing on
  other workers are dispatched over Redis pub/sub (or the SSE lease
  transport) instead of failing with "no operators available".
- **`api/services/pin_sse_transport.py`** — SSE node transport with
  cross-worker leases.
- **`api/services/redis_storage.py`** (extract) — the PIN data layer on
  Redis: operators, nodes, reputation, billing, job lifecycle, and the
  live model index, with Lua scripts for atomic financial operations.
- **`api/utils/pin_auth.py`** — the daemon signature contract
  (HMAC over client_id + timestamp), pinned by a known-vector test the
  Rust daemon also implements.
- **`api/workers/pin_heartbeat.py`** — reaps stale nodes/operators.

## Docs

- [PIN Protocol Specification](docs/PIN-Protocol-Specification.md) — the wire contract
- [PIN Operator Guide](docs/PIN-Operator-Guide.md) — running a node
- [PIN Product Announcement](docs/PIN-Product-Announcement.md)

## Running the tests

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
for t in tests/test_pin_*.py; do python -m pytest "$t" -q; done
# → 164 passed
```

Run the files individually: several modules stub
`api.services.redis_storage` in `sys.modules` without restoring it, so a
single-process full-suite run shows cross-test pollution — in the source
repo too (see EXTRACTION_MAP.md, "Test verification").

The tests need no network and no Redis server: they drive the bus,
transports, streaming, model resolution, and storage logic against
fakes and stubs — which is also a good guided tour of the failure modes
this code was written to survive.

## Configuration

Environment variables only — see [`.env.example`](.env.example).
Notable: `REDIS_URL`, `REDIS_NAMESPACE`, `SESSION_SECRET`, and
`AIAS_STORAGE_BACKEND` (`redis` default; `nedb` rides the included
Redis-on-NEDB shim in `api/config.py`).

## License

Business Source License 1.1 (see [LICENSE](LICENSE)). In short:
copy, modify, evaluate, and run it non-production freely; production
use or offering it as a service to third parties requires a commercial
license from INTERCHAINED LLC (dev@interchained.org). Each version
converts to the Apache License 2.0 on its Change Date — for this
version, 2030-10-22 at the latest.

Copyright (C) 2025-2026 INTERCHAINED LLC.
