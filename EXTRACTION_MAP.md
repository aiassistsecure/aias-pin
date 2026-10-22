# Extraction Map — how this repo was distilled

**Source:** `aiassistsecure/aias`, branch `march_2026`, commit
`58a16643993188898ca8b51a29a9ffa771e64755` ("Merge PR #213: fix Imagine PIN
context capacity to 8192", 2026-10-07). AiAS is the private monolith behind
aiassist.net. This repo contains only the PIN subsystem, extracted
2026-10-22 for public examination under the Business Source License 1.1
(see `LICENSE`).

Method: files are either copied **byte-for-byte** from the source, or
extracted by AST node range (whole classes / methods / constants, bodies
unedited) from the two platform-wide files PIN depends on. Every deviation
from a straight copy is listed below — there are no silent edits.

## Copied byte-for-byte

| File | Source path (same) |
|---|---|
| PIN API routes (4,113 lines) | `api/routes/pin.py` |
| PIN service layer | `api/services/pin_service.py` |
| Cross-worker operator bus | `api/services/pin_operator_bus.py` |
| SSE transport / leases | `api/services/pin_sse_transport.py` |
| PIN provider adapter | `api/providers/adapters/pin.py` |
| Provider registry (boundary file) | `api/providers/config.py` |
| Heartbeat / stale-node worker | `api/workers/pin_heartbeat.py` |
| Daemon signature auth (HMAC) | `api/utils/pin_auth.py` |
| Model-name resolution | `api/utils/pin_models.py` |
| Provider usage normalizer | `api/utils/provider_usage.py` |
| Fernet key-encryption helper (supporting) | `api/utils/encryption.py` |
| App config / Redis seam | `api/config.py` |
| NEDB redis shim (supporting) | `api/services/nedb_redis_shim.py` |
| NEDB atomic client (supporting) | `api/services/nedb_atomic.py` |
| Tests (21 files, all `test_pin_*`) | `tests/test_pin_*.py` |
| Protocol / operator docs | `docs/PIN-*.md` |

The two NEDB files are supporting modules: `api/config.py` imports the shim
lazily when `AIAS_STORAGE_BACKEND=nedb`, and `test_pin_bus_on_nedb_shim.py`
exercises the operator bus over it. The full NEDB storage bridge is a
separate subsystem and is not part of this slice.

`api/providers/config.py` is a boundary file: it is the multi-provider
registry (a different subsystem), included verbatim because the
`PinHasNoStaticCatalogue` tests in `test_pin_stale_models_are_evicted.py`
import it to prove PIN's entry advertises **no** hardcoded models while
other providers keep theirs. It is self-contained (stdlib + pydantic) and
is included as evidence for that property, not as part of the PIN runtime.

## Extracted by AST range (bodies unedited)

### `api/models/schemas.py` — 362 lines, from a 2,904-line original

Header imports (narrowed to what these classes use) plus 34 classes:
`UserRole`, `PlanType`, `UserBase`, `UserCreate`, `UserRegister`, `User`,
`UserLogin` (source lines 8–151, the identity types the PIN routes
authenticate against), `ChatMessage`, `ChatCompletionChoice`,
`ChatCompletionUsage` (443–514), and the complete PIN model section
(2393–2639): `OperatorStatus`, `PinJobStatus`, `OperatorRegion`,
`PinOperator[Create|Update]`, `PinOperatorRegistrationResponse`,
`PinReputation`, `PinHeartbeat[Response]`, `PinJob`, `PinBillingUser`,
`PinBillingOperator`, `PinTransaction`, `PinWithdrawalRequest`,
`PinWalletUpdate`, `PinWithdrawal`, `PinOperatorEarnings`,
`PinOperatorStatus`, `PinNetworkStatus`, `PinOperatorPublic`,
`PinChatCompletionRequest`, `PinChatCompletionMetadata`,
`PinChatCompletionResponse`.

### `api/services/redis_storage.py` — 1,492 lines, from a 10,723-line original

- Helpers `now_iso`, `gen_id` (verbatim).
- The three PIN Lua scripts, verbatim: `LUA_PIN_JOB_COMPLETE` (source
  301–376), `LUA_PIN_WITHDRAW` (378–431), `LUA_PIN_ADD_CREDITS` (433–456) —
  atomic job settlement, withdrawal, and credit grants.
- `RedisStorage` with: `__init__`, `_register_lua_scripts`,
  `get_user` (633–648), `get_session_user` (724–728),
  `get_api_key_by_secret` (1726–1758), and all **51 PIN methods** in source
  order, `pin_create_operator` (8827) through `pin_evict_stale_models`
  (10050) — operators, nodes, endpoint uniqueness, reputation, billing,
  operator selection, job lifecycle, withdrawals, network status, and the
  live model index.

**Documented packaging edits in this file (the only edits in the repo):**

1. The import block is narrowed to the schemas/helpers the extracted code
   references (the original imports ~150 platform schema types).
2. `_register_lua_scripts` registers only the three PIN Lua scripts (the
   original also registers license/seat scripts for unrelated subsystems).
3. The module-level singleton is `storage = RedisStorage()`. The original
   selects a `NedbStorage` class when `AIAS_STORAGE_BACKEND=nedb`; that
   class is a separate subsystem (see Tier C in the examination notes).
   `api/config.py` is unedited, including its NEDB client seam.

And in the package `__init__` files: `api/utils/__init__.py` is verbatim
(it imports `api/utils/encryption.py`, included above as a supporting
module), but `api/providers/__init__.py` and
`api/providers/adapters/__init__.py` are replaced with short docstring-only
modules — the originals aggregate the full multi-provider registry,
router, and the base/OpenAI/Anthropic/Gemini adapters, which are a
different subsystem. Nothing in this slice imports them; the PIN adapter
is imported directly as `api.providers.adapters.pin`.

## Deliberately excluded

- **The rest of AiAS** (~67k lines of live API: billing/Stripe, licensing,
  resellers, workspaces, dashboard) — different subsystems, several
  commercially sensitive.
- **~72k lines of version snapshots** in the source tree
  (`public_api_v1…v11`, `redis_storage_stable/broken/v11`, …) — nothing
  imports them; they are history, not source.
- `test_public_api_pin_providers.py` and `test_workspace_pin_override.py` —
  these AST-inspect `public_api.py`, `workspaces.py`, and
  `ai_orchestrator.py`, which belong to a different slice.
- `quests-engine/` — third-party Quests (quests.dev), Apache-2.0. Not
  AiAssist code and not covered by this license.
- All data and secrets: `.env`, Redis snapshots (`*.rdb`,
  `appendonlydir/`), databases, the committed `venv/`, and build output
  (`dist*/`) exist in the source branch and are **not** in this repo.
  Configuration here is by environment variable — see `.env.example`
  (names only).
- Client daemons: `pin-clientd` (Rust) and `pin-client` (Tauri) live in
  their own repositories; this slice is the server side they speak to.
  The wire contract is in `docs/PIN-Protocol-Specification.md`.

## Test verification (2026-10-22, against this distilled tree)

All 21 test files, 164 tests, run **per file**:

```
for t in tests/test_pin_*.py; do python -m pytest $t -q; done
# → 164 passed, 0 failed (identical per-file results to the source repo)
```

Known upstream property, preserved faithfully: running the whole suite in
a single pytest process shows 26 failures **in the source repo as well**
— several test modules stub `api.services.redis_storage` in `sys.modules`
without restoring it, which breaks later modules that load the real
storage class (the source tests themselves comment on this). The
distilled tree reproduces that behaviour exactly (same 26, no others),
because the tests are unedited. Run them per file, as above.
