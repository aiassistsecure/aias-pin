import os
import time
import secrets
from typing import Dict, Any, List, Optional
from api.services.redis_storage import storage
from api.models.schemas import User
from api.utils.provider_usage import normalize_usage
from api.services import pin_operator_bus as bus
from api.services.pin_sse_transport import transport as sse_transport

OPERATOR_TIMEOUT_SECONDS = 60
PROTOCOL_FEE_PERCENT = 0.10


def get_pin_ws_manager():
    """Lazy import to avoid circular dependency."""
    from api.routes.pin import pin_ws_manager
    return pin_ws_manager


def _connected_operator_ids(ws_manager) -> List[str]:
    """Operators this process can actually serve right now.

    WebSocket sockets and live SSE leases are the same fact: a connection
    this worker is holding. A process that only counted websockets reported
    "none" for an operator sitting on /stream/connect.
    """
    ids = set()
    try:
        ids.update(ws_manager.operator_map.values())
    except Exception:  # noqa: BLE001 -- liveness must never raise
        pass
    try:
        ids.update(sse_transport.operator_ids())
    except Exception:  # noqa: BLE001
        pass
    return sorted(ids)


def _reachability(ws_manager, operator_id: str) -> str:
    """Why this operator is or is not reachable, in one line.

    THE MESSAGE HAS TO CARRY THE EVIDENCE. "No connected operators available"
    is true of at least four different states — no WebSocket, no SSE lease, no
    bus subscriber in any worker, or code that predates the bus entirely — and
    it distinguishes none of them. Reading the difference cost an evening of
    swapping theories against log fragments, which is exactly the failure the
    rest of this system is built to avoid: a report that says something is
    wrong without saying what.
    """
    try:
        ws = ws_manager.is_operator_online(operator_id)
    except Exception:  # noqa: BLE001
        ws = "?"
    try:
        sse = operator_id in sse_transport.operator_ids()
    except Exception:  # noqa: BLE001
        sse = "?"
    try:
        subscribed = bus.holders(operator_id)
    except Exception:  # noqa: BLE001
        subscribed = "?"
    return (f"ws_here={ws} sse_here={sse} bus_holders={subscribed} "
            f"pid={os.getpid()}")


def reconcile_online_set(ws_manager) -> None:
    """Re-assert `pin:operators:online` from the sockets actually in hand.

    `pin:operators:online` is a derived cache, not truth. It is written on
    AUTH and cleared by disconnect() and by the heartbeat reaper
    (api/workers/pin_heartbeat.py), and it goes stale in ways that leave a
    connected, serving operator invisible to pin_select_operator -- observed
    in production as "No operators available" while the daemon log showed
    `Authenticated!` and the node was heartbeating. Cheap to repair, and the
    socket we are holding is not a matter of opinion.
    """
    now = str(time.time())
    for op_id in _connected_operator_ids(ws_manager):
        try:
            storage.r.sadd("aiconsult:pin:operators:online", op_id)
            storage.r.setex(f"aiconsult:pin:heartbeats:{op_id}", 300, now)
        except Exception as exc:  # noqa: BLE001
            print(f"[PIN SERVICE] online-set reconcile failed for {op_id}: {exc}")


def _smembers(key: str) -> set:
    """Set members as plain strings, or empty. Liveness must never raise."""
    try:
        raw = storage.r.smembers(key) or set()
    except Exception:  # noqa: BLE001
        return set()
    return {m.decode() if isinstance(m, bytes) else str(m) for m in raw}


def _model_aliases(model: str) -> List[str]:
    """`name` and `name:latest` are the same Ollama model.

    The registry stores whatever string the daemon reported, and the two
    registration paths do not always report the same one. api/utils/pin_models.py
    already normalises this for model RESOLUTION; the membership checks here did
    not, so a node registered as `muse-local` was invisible to a request for
    `muse-local:latest` even with both indexes intact.
    """
    m = (model or "").strip()
    if not m:
        return []
    base = m[: -len(":latest")] if m.endswith(":latest") else m
    return [m] if base == m and ":" in m else list(dict.fromkeys([m, base, f"{base}:latest"]))


def _operator_serves(op_id: str, model: str) -> bool:
    """Did this operator register `model` — by ANY of the places that record it?

    THERE ARE TWO MODEL INDEXES AND NOTHING KEPT THEM IN AGREEMENT.

        pin:models:{model}       -> OPERATOR ids
            pin_register_operator, WS MODEL_LIST, WS REGISTER_NODE
        pin:node_models:{model}  -> NODE ids
            pin_create_node, pin_find_or_create_node (UPDATE path),
            pin_update_node

    This function read the first, then walked `pin:operator:{op}:nodes`. Nothing
    read the second. So a daemon whose models were last written by a node path —
    i.e. every steady-state `[NODE] UPDATED`, which is what a long-running
    operator does forever after its first registration — was registered only
    where nobody looked. If that operator's node-set had also moved on (a re-auth
    under a fresh operator id, a re-created node), BOTH checks missed and the
    gateway told the caller:

        No operators available for model: muse-local:latest.
        Connected operators in this process: ['op_a32a252d-292'].

    while that operator's own log read `Completed successfully (1571+107 tokens)`
    in the same minute. Both statements were true.

    Every index is consulted now, in cost order, and the node->operator direction
    is followed so the node index can answer a question asked about an operator.
    Widening the read is deliberately preferred over trusting any single index:
    they disagree in production today, and a selector that believes one of them
    is a selector that hides a working operator.
    """
    import json as _json

    for name in _model_aliases(model):
        # 1. operator-level index — cheapest, and the only one read before.
        if op_id in _smembers(f"aiconsult:pin:models:{name}"):
            return True

        # 2. node-level index, followed back to its owner. This is the arm that
        #    was missing entirely.
        for nid in _smembers(f"aiconsult:pin:node_models:{name}"):
            try:
                nd = storage.r.hgetall(f"aiconsult:pin:nodes:{nid}") or {}
            except Exception:  # noqa: BLE001
                continue
            if nd.get("operator_id") == op_id:
                return True

    # 3. The node rows this operator owns, read directly. Survives both indexes
    #    being stale, because the node's own `models` field is written on every
    #    create and update.
    try:
        wanted = set(_model_aliases(model))
        for node in storage.pin_list_operator_nodes(op_id) or []:
            nid = node.get("id") if isinstance(node, dict) else getattr(node, "id", None)
            if not nid:
                continue
            nd = storage.r.hgetall(f"aiconsult:pin:nodes:{nid}") or {}
            raw = nd.get("models") or "[]"
            models = raw if isinstance(raw, list) else _json.loads(raw)
            if any(str(m) in wanted for m in models):
                return True
    except Exception:  # noqa: BLE001
        pass

    return False


def selection_evidence(ws_manager, model: str) -> str:
    """Why each connected operator was NOT chosen, in one line.

    THE MESSAGE HAS TO CARRY THE EVIDENCE — the rule this module already
    states for `_reachability`, which the most common failure in the system
    does not follow:

        No operators available for model: muse-local:latest.
        Connected operators in this process: ['op_a32a252d-292'].

    That is true of at least four different states: the model indexes are
    empty, the node rows disagree, the operator carries a `failed` tombstone,
    or the operator row itself is unreadable. It distinguishes none of them,
    so each occurrence costs a round of swapping theories against production —
    three separate diagnoses of this same string so far, each ending in a
    different root cause.

    Reports per operator: whether it is registered for the model (`serves`),
    whether a tier tombstone excluded it (`failed`), and whether its operator
    row loaded (`record`). Cheap: it runs only on the failure path, and only
    across operators this process is holding.
    """
    parts = []
    for op_id in _connected_operator_ids(ws_manager):
        try:
            serves = _operator_serves(op_id, model)
        except Exception as exc:  # noqa: BLE001 -- a diagnostic must not raise
            serves = f"error:{exc}"
        try:
            failed = _is_failed(op_id)
        except Exception as exc:  # noqa: BLE001
            failed = f"error:{exc}"
        try:
            record = storage.pin_get_operator(op_id) is not None
        except Exception as exc:  # noqa: BLE001
            record = f"error:{exc}"
        parts.append(f"{op_id}[serves={serves} failed_tier={failed} record={record}]")
    return " ".join(parts) if parts else "no operator connected to this worker"


def _is_failed(op_id: str) -> bool:
    """Operator-level `quality_tier == failed`, but only when no node of that
    operator says otherwise.

    Interview verdicts are written per NODE; the operator row is a stale
    aggregate that nothing maintains, so a passing node can sit behind a
    `failed` operator tombstone forever (there is no code path that clears
    it). Trust the node when they disagree.
    """
    try:
        op = storage.r.hgetall(f"aiconsult:pin:operators:{op_id}") or {}
        if op.get("quality_tier") != "failed":
            return False
        for node in storage.pin_list_operator_nodes(op_id) or []:
            nid = node.get("id") if isinstance(node, dict) else getattr(node, "id", None)
            if not nid:
                continue
            nd = storage.r.hgetall(f"aiconsult:pin:nodes:{nid}") or {}
            if nd.get("quality_tier") in ("verified", "standard", "slow"):
                return False
        return True
    except Exception:  # noqa: BLE001
        return False


def select_from_live_sockets(ws_manager, model: str,
                             exclude: Optional[List[str]] = None):
    """Pick an operator from the sockets this process is holding.

    Last-resort path for when storage-based selection misses. Uses only
    signals that cannot go stale behind our back: the socket in hand, plus
    the model registration written when the node registered.
    """
    exclude = set(exclude or [])
    for op_id in _connected_operator_ids(ws_manager):
        if op_id in exclude:
            continue
        if not _operator_serves(op_id, model):
            continue
        if _is_failed(op_id):
            print(f"[PIN SERVICE] {op_id} connected but tier=failed, skipping")
            continue
        op = storage.pin_get_operator(op_id)
        if op:
            return op
    return None


async def pin_chat_completion(
    user_id: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.7,
    max_tokens: int = 1024,
    max_price: float = None,
    tools: Optional[List[dict]] = None,
    tool_choice: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute PIN chat completion via WebSocket to connected Clientd.
    
    Security Model:
    - Operator's Ollama runs on localhost (never exposed publicly)
    - Clientd connects to AiAS via authenticated WebSocket
    - All inference requests flow through this secure tunnel
    - Consumer never sees or accesses operator's endpoint directly
    """
    user_billing = storage.pin_get_user_billing(user_id)
    if not user_billing or user_billing.credits_balance <= 0:
        raise ValueError("Insufficient PIN credits. Purchase credits to continue.")
    
    excluded_operators = []
    # WHY the operator was excluded, not just that it was. A failed attempt
    # excludes the operator; the next pass then finds nobody and raises
    # "No operators available for model", which reads as a routing problem
    # and sends the reader hunting through indexes and online sets. The real
    # cause is right here and was being discarded.
    exclusion_reasons: List[str] = []
    max_attempts = 3
    
    pin_model = model
    if model.startswith("pin:"):
        pin_model = model[4:]
    
    print(f"[PIN SERVICE] Starting chat completion for model={pin_model}, user={user_id}")
    
    ws_manager = get_pin_ws_manager()
    
    # `pin:operators:online` is a derived cache that goes stale while an
    # operator is demonstrably connected. Repair it from the sockets in hand
    # before selecting.
    reconcile_online_set(ws_manager)

    for attempt in range(max_attempts):
        operator = storage.pin_select_operator(
            model=pin_model,
            exclude=excluded_operators,
            max_price=max_price
        )

        if not operator:
            # Storage selection missed. Fall back to the sockets we are
            # holding: connected + registered this model == can serve,
            # whatever the online set currently claims.
            operator = select_from_live_sockets(
                ws_manager, pin_model, excluded_operators)
            if operator:
                print(f"[PIN SERVICE] Storage selection missed; routing to "
                      f"live socket for operator {operator.id}")

        if not operator:
            connected = _connected_operator_ids(ws_manager)
            raise ValueError(
                f"No operators available for model: {pin_model}. "
                f"Connected operators in this process: "
                f"{connected if connected else 'none'}. "
                f"{selection_evidence(ws_manager, pin_model)}"
                + (f" Excluded this request after failing: "
                   f"{'; '.join(exclusion_reasons)}" if exclusion_reasons else ""))

        # REACHABLE, NOT NECESSARILY LOCAL.
        #
        # An operator's socket lives in ONE worker process. This check used to
        # read only this process's map, so with several workers behind one port
        # most requests were refused for an operator that was healthy and
        # connected — to a different worker. `bus.holders()` asks Redis how many
        # processes are currently subscribed for this operator, which is the
        # same question asked across the whole gateway instead of inside one
        # process.
        # EITHER transport counts as local. `is_operator_online` reads the
        # WebSocket map only, so an operator on an SSE lease in THIS process
        # looked remote and was dispatched onto the bus back to itself.
        local = (ws_manager.is_operator_online(operator.id)
                 or operator.id in sse_transport.operator_ids())
        if not local and bus.holders(operator.id) == 0:
            print(f"[PIN SERVICE] Operator {operator.id} unreachable in any worker, excluding")
            excluded_operators.append(operator.id)
            if attempt == max_attempts - 1:
                raise ValueError(
                    f"No connected operators available for model: {pin_model}. "
                    f"{_reachability(ws_manager, operator.id)}")
            continue
        
        print(f"[PIN SERVICE] Selected operator {operator.id} for model {pin_model}")
        
        job = storage.pin_create_job(
            user_id=user_id,
            operator_id=operator.id,
            model=pin_model
        )
        
        try:
            start_time = time.time()
            
            request_id = f"pin_req_{secrets.token_urlsafe(16)}"
            
            payload = {
                "model": pin_model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "stream": False
            }
            # OMITTED WHEN ABSENT, never sent as null.
            #
            # llama.cpp treats a present-but-null `tools` differently from an
            # absent one when applying a chat template, and several
            # OpenAI-compatible servers reject the null outright. A caller that
            # wants no tools must produce a request byte-identical to today's,
            # or this becomes a change to every existing PIN inference rather
            # than an addition for the ones that ask.
            if tools:
                payload["tools"] = tools
                if tool_choice:
                    payload["tool_choice"] = tool_choice

            if local:
                print(f"[PIN SERVICE] Sending inference request {request_id} to operator {operator.id}")
                result = await ws_manager.send_inference_request(
                    operator_id=operator.id, request_id=request_id, payload=payload)
            else:
                # The socket is held by another worker. Hand the job over and
                # wait for that worker to post the answer back. Same timeout
                # discipline, same `None`-means-no-result contract, so every
                # branch below is unchanged.
                delivered = bus.dispatch(operator.id, request_id, "chat", payload)
                print(f"[PIN SERVICE] Dispatched {request_id} to {operator.id} "
                      f"via bus ({delivered} worker(s) holding)")
                result = await bus.await_reply(request_id, timeout=65.0) if delivered else None
            print(f"[PIN SERVICE] Inference result received: {list(result.keys()) if result else 'None'}")
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            if not result:
                raise Exception("WebSocket inference timeout or operator disconnected")
            
            if "error" in result:
                raise Exception(f"Operator error: {result['error']}")
            
            usage, usage_metadata = normalize_usage(result)
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", input_tokens + output_tokens)
            
            billing_result = storage.pin_complete_job_atomic(
                job_id=job.id,
                user_id=user_id,
                operator_id=operator.id,
                total_tokens=total_tokens,
                model=pin_model,
                pricing_per_1k=operator.pricing_per_1k_tokens,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms
            )
            
            if not billing_result.get('success'):
                storage.pin_fail_job(job.id, billing_result.get('error', 'billing_failed'))
                if billing_result.get('error') == 'INSUFFICIENT_CREDITS':
                    raise ValueError(f"Insufficient credits. Balance: ${billing_result.get('balance', 0):.4f}")
                raise Exception(f"Billing failed: {billing_result}")
            
            credits_charged = billing_result.get('total_cost', 0)
            storage.pin_update_reputation(operator.id, success=True, latency_ms=latency_ms)
            
            choices = result.get("choices", [])
            
            return {
                "id": f"pin-{job.id}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": pin_model,
                "choices": choices,
                "usage": usage,
                "pin_metadata": {
                    "operator_id": operator.id,
                    "latency_ms": latency_ms,
                    "credits_charged": credits_charged,
                    "job_id": job.id,
                    "tx_id": billing_result.get("tx_id"),
                    "pricing_per_1k_tokens": operator.pricing_per_1k_tokens,
                    **usage_metadata,
                    "routing": "websocket"
                }
            }
            
        except Exception as e:
            storage.pin_fail_job(job.id, str(e))
            storage.pin_update_reputation(operator.id, success=False)
            excluded_operators.append(operator.id)
            exclusion_reasons.append(f"{operator.id}: {e}")

            if attempt == max_attempts - 1:
                raise ValueError(f"All operators failed: {str(e)}")

    raise ValueError("PIN inference failed after all attempts")


async def pin_chat_completion_stream(
    user_id: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.7,
    max_tokens: int = 1024,
    max_price: float = None,
    tools: Optional[List[dict]] = None,
    tool_choice: Optional[str] = None,
):
    """Streaming counterpart to pin_chat_completion.

    Yields OpenAI-shaped `chat.completion.chunk` dicts, which is what
    ProviderRouter.route_completion_stream hands straight to the caller.

    Selection, credit gating, billing and reputation are IDENTICAL to the
    non-streaming path -- billing still happens exactly once, from the
    terminal INFERENCE_RESPONSE, never from a chunk. A daemon too old to
    stream simply sends no chunks and only that final message, so this
    degrades to one-chunk delivery rather than failing.

    Unlike the non-streaming path there is NO retry across operators: by the
    time an operator fails mid-generation the caller has already been handed
    tokens, and silently restarting on a second operator would splice two
    different answers together. Fail honestly instead.
    """
    user_billing = storage.pin_get_user_billing(user_id)
    if not user_billing or user_billing.credits_balance <= 0:
        raise ValueError("Insufficient PIN credits. Purchase credits to continue.")

    pin_model = model[4:] if model.startswith("pin:") else model

    print(f"[PIN SERVICE] Starting STREAMING completion for model={pin_model}, user={user_id}")

    ws_manager = get_pin_ws_manager()
    reconcile_online_set(ws_manager)

    operator = storage.pin_select_operator(model=pin_model, max_price=max_price)
    if not operator:
        operator = select_from_live_sockets(ws_manager, pin_model, [])
        if operator:
            print(f"[PIN SERVICE] Storage selection missed; streaming from live "
                  f"socket for operator {operator.id}")
    if not operator:
        connected = _connected_operator_ids(ws_manager)
        raise ValueError(
            f"No operators available for model: {pin_model}. "
            f"Connected operators in this process: {connected if connected else 'none'}. "
            f"{selection_evidence(ws_manager, pin_model)}")

    if not (ws_manager.is_operator_online(operator.id)
            or operator.id in sse_transport.operator_ids()
            or bus.holders(operator.id) > 0):
        raise ValueError(
            f"No connected operators available for model: {pin_model}. "
            f"{_reachability(ws_manager, operator.id)}")

    job = storage.pin_create_job(user_id=user_id, operator_id=operator.id, model=pin_model)
    request_id = f"pin_req_{secrets.token_urlsafe(16)}"
    start_time = time.time()
    chunk_id = f"pin-{job.id}"

    print(f"[PIN SERVICE] Streaming request {request_id} to operator {operator.id}")

    saw_content = False
    saw_thinking = False
    final_result = None
    stream_payload = {
        "model": pin_model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    # Same omit-when-absent rule as the non-streaming path. Divergence between
    # the two payload builders is how the streaming path stayed WebSocket-only
    # for an hour after the bus was fixed -- two places that must agree should
    # be written to agree.
    if tools:
        stream_payload["tools"] = tools
        if tool_choice:
            stream_payload["tool_choice"] = tool_choice
    try:
        async for event in ws_manager.stream_inference_request(
            operator_id=operator.id,
            request_id=request_id,
            payload=stream_payload,
        ):
            if event["type"] == "chunk":
                # A reasoning model streams its thinking FIRST, with content
                # empty, and that phase can span the whole generation. It goes
                # out as `delta.reasoning`, which the playground already
                # renders as a live thinking trace (and which the anthropic
                # adapter already emits) -- so PIN needs no new client work.
                if event.get("kind") == "thinking":
                    saw_thinking = True
                    yield {
                        "id": chunk_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": pin_model,
                        "choices": [{
                            "index": 0,
                            "delta": {"role": "assistant", "reasoning": event["delta"]},
                            "finish_reason": None,
                        }],
                    }
                    continue
                saw_content = True
                yield {
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": pin_model,
                    "choices": [{
                        "index": 0,
                        "delta": {"role": "assistant", "content": event["delta"]},
                        "finish_reason": None,
                    }],
                }
            elif event["type"] == "error":
                raise Exception(event.get("error", "operator error"))
            else:
                final_result = event.get("result") or {}
                if "error" in final_result:
                    raise Exception(f"Operator error: {final_result['error']}")
    except Exception as exc:
        storage.pin_fail_job(job.id, str(exc))
        storage.pin_update_reputation(operator.id, success=False)
        raise

    latency_ms = int((time.time() - start_time) * 1000)
    final_result = final_result or {}

    # An old daemon streams nothing and sends only the final message. Emit its
    # content as a single chunk so the caller still gets an answer instead of
    # an empty stream followed by silence.
    if not saw_content:
        choices = final_result.get("choices") or []
        content = (choices[0].get("message", {}).get("content", "") if choices else "")
        if not content and saw_thinking:
            # The model reasoned and never answered -- usually it hit its token
            # budget mid-thought. Say so in the stream rather than ending on
            # silence, which reads as a failure.
            print(f"[PIN SERVICE] {operator.id} produced reasoning but no answer "
                  f"for {pin_model} (likely hit max_tokens while thinking)")
            yield {
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": pin_model,
                "choices": [{
                    "index": 0,
                    "delta": {"role": "assistant", "content":
                              "_(the model spent its whole token budget reasoning "
                              "and produced no answer — try raising max tokens)_"},
                    "finish_reason": None,
                }],
            }
        if content:
            print("[PIN SERVICE] Operator sent no chunks (pre-2.3.0 daemon?) — "
                  "delivering the final message as one chunk")
            yield {
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": pin_model,
                "choices": [{
                    "index": 0,
                    "delta": {"role": "assistant", "content": content},
                    "finish_reason": None,
                }],
            }

    usage, usage_metadata = normalize_usage(final_result)
    input_tokens = usage.get("prompt_tokens", 0)
    output_tokens = usage.get("completion_tokens", 0)
    total_tokens = usage.get("total_tokens", input_tokens + output_tokens)

    billing_result = storage.pin_complete_job_atomic(
        job_id=job.id,
        user_id=user_id,
        operator_id=operator.id,
        total_tokens=total_tokens,
        model=pin_model,
        pricing_per_1k=operator.pricing_per_1k_tokens,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
    )

    if not billing_result.get("success"):
        # The tokens are already delivered -- we cannot un-send them. Record
        # the failure loudly and let the stream finish; silently swallowing a
        # billing failure is how revenue goes missing without a trace.
        storage.pin_fail_job(job.id, billing_result.get("error", "billing_failed"))
        print(f"[PIN SERVICE] WARNING billing failed AFTER delivery for job "
              f"{job.id}: {billing_result}")
    else:
        storage.pin_update_reputation(operator.id, success=True, latency_ms=latency_ms)

    finish = "stop"
    choices = final_result.get("choices") or []
    if choices and choices[0].get("finish_reason"):
        finish = choices[0]["finish_reason"]

    yield {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": pin_model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
        "usage": usage,
        "pin_metadata": {
            "operator_id": operator.id,
            "latency_ms": latency_ms,
            "credits_charged": billing_result.get("total_cost", 0),
            "job_id": job.id,
            "tx_id": billing_result.get("tx_id"),
            "pricing_per_1k_tokens": operator.pricing_per_1k_tokens,
            **usage_metadata,
            "routing": "websocket-stream",
        },
    }


async def pin_tts_speech(
    user_id: str,
    model: str,
    input_text: str,
    voice: str = "default",
    response_format: str = "mp3",
    speed: Optional[float] = None,
) -> Dict[str, Any]:
    """Execute PIN text-to-speech via WebSocket to a connected clientd (v2.4.0+).

    Mirrors pin_chat_completion's security model: the operator's TTS server
    runs on localhost behind the daemon; everything flows through the
    authenticated WS tunnel; the consumer never sees the endpoint.

    Speech nodes register their models with a "tts:" prefix (routing
    metadata). The consumer may pass either "chatterbox-turbo" or
    "tts:chatterbox-turbo" — selection always uses the prefixed name, and the
    daemon payload always carries the bare name.

    BILLING — PROPOSED, deliberately simple until real speech pricing exists:
    input CHARACTERS are billed as tokens against the operator's per-1K rate
    (a 150-char host line at $0.001/1K ≈ $0.00015). All of it flows through
    the same atomic job/billing path as chat, so swapping the unit later is a
    one-line change here, not a new pipeline.
    """
    user_billing = storage.pin_get_user_billing(user_id)
    if not user_billing or user_billing.credits_balance <= 0:
        raise ValueError("Insufficient PIN credits. Purchase credits to continue.")

    if not input_text or not input_text.strip():
        raise ValueError("TTS input is empty")

    bare_model = model[4:] if model.startswith("tts:") else model
    routed_model = f"tts:{bare_model}"

    print(f"[PIN SERVICE] Starting TTS for model={routed_model}, user={user_id}, {len(input_text)} chars")

    ws_manager = get_pin_ws_manager()
    reconcile_online_set(ws_manager)

    excluded_operators = []
    # WHY the operator was excluded, not just that it was. A failed attempt
    # excludes the operator; the next pass then finds nobody and raises
    # "No operators available for model", which reads as a routing problem
    # and sends the reader hunting through indexes and online sets. The real
    # cause is right here and was being discarded.
    exclusion_reasons: List[str] = []
    max_attempts = 3

    for attempt in range(max_attempts):
        operator = storage.pin_select_operator(
            model=routed_model,
            exclude=excluded_operators,
            max_price=None,
        )

        if not operator:
            operator = select_from_live_sockets(
                ws_manager, routed_model, excluded_operators)
            if operator:
                print(f"[PIN SERVICE] Storage selection missed; routing TTS to "
                      f"live socket for operator {operator.id}")

        if not operator:
            # THE MESSAGE HAS TO CARRY THE EVIDENCE.
            #
            # This raised the bare string for months. On 2026-09-07 it cost an
            # afternoon: the operator was connected in this very process, the
            # model was in `pin:models:available`, the GPU rendered a wav
            # locally in 25s, and chat completions on the SAME operator
            # succeeded minutes apart -- while this line said only "no
            # operators available", which is equally true of an empty model
            # index, a candidate set that resolves to a DIFFERENT (offline)
            # operator, a `failed` tier tombstone, and an unreadable operator
            # row. It distinguished none of them.
            #
            # `pin_chat_completion` has printed `selection_evidence` all along.
            # This is the TTS path catching up to its own sibling.
            connected = _connected_operator_ids(ws_manager)
            candidates = set()
            try:
                candidates = storage._pin_operators_for_model(routed_model)
            except Exception as exc:  # noqa: BLE001 -- a diagnostic must not raise
                candidates = {f"error:{exc}"}
            raise ValueError(
                f"No operators available for model: {routed_model}. "
                f"Connected operators in this process: "
                f"{connected if connected else 'none'}. "
                f"Registered for this model: "
                f"{sorted(candidates) if candidates else 'nobody'}. "
                f"{selection_evidence(ws_manager, routed_model)}"
                + (f" Excluded this request after failing: "
                   f"{'; '.join(exclusion_reasons)}" if exclusion_reasons else ""))

        # REACHABLE, NOT NECESSARILY LOCAL -- and EITHER transport counts.
        #
        # `is_operator_online` reads the WebSocket map only, so an operator on
        # an SSE lease (the default transport since 2026-08-26) was excluded
        # here even with its downlink open and heartbeating. The chat path was
        # fixed for this; TTS was not, so speech was unreachable for every
        # SSE operator no matter what selection returned.
        local = (ws_manager.is_operator_online(operator.id)
                 or operator.id in sse_transport.operator_ids())
        if not local and bus.holders(operator.id) == 0:
            print(f"[PIN SERVICE] Operator {operator.id} unreachable in any "
                  f"worker, excluding")
            excluded_operators.append(operator.id)
            exclusion_reasons.append(
                f"{operator.id}: unreachable "
                f"({_reachability(ws_manager, operator.id)})")
            if attempt == max_attempts - 1:
                raise ValueError(
                    f"No connected operators available for model: "
                    f"{routed_model}. "
                    f"{_reachability(ws_manager, operator.id)}")
            continue

        print(f"[PIN SERVICE] Selected operator {operator.id} for TTS model {routed_model}")

        job = storage.pin_create_job(
            user_id=user_id,
            operator_id=operator.id,
            model=routed_model,
        )

        try:
            start_time = time.time()
            request_id = f"pin_tts_{secrets.token_urlsafe(16)}"

            payload = {
                "model": bare_model,
                "input": input_text,
                "voice": voice,
                "response_format": response_format,
                **({"speed": speed} if speed is not None else {}),
            }
            if local:
                print(f"[PIN SERVICE] Sending TTS request {request_id} to "
                      f"operator {operator.id}")
                result = await ws_manager.send_tts_request(
                    operator_id=operator.id, request_id=request_id,
                    payload=payload)
            else:
                # The downlink is held by another worker. `_serve_bus_job` has
                # had a `kind == "tts"` branch since the bus was written and
                # NOTHING ever published to it -- it was unreachable code, so
                # cross-worker speech simply did not exist.
                #
                # The deadline matches send_tts_request's own 300s rather than
                # chat's 65s: a cold TTS model can spend minutes loading before
                # its first render, and unlike chat there are no interim chunks
                # to keep the wait honest. Measured on the A6000: 25s for a
                # short line once warm.
                delivered = bus.dispatch(operator.id, request_id, "tts", payload)
                print(f"[PIN SERVICE] Dispatched TTS {request_id} to "
                      f"{operator.id} via bus ({delivered} worker(s) holding)")
                result = (await bus.await_reply(request_id, timeout=300.0)
                          if delivered else None)

            latency_ms = int((time.time() - start_time) * 1000)

            if not result:
                raise Exception("WebSocket TTS timeout or operator disconnected")

            if "error" in result:
                raise Exception(f"Operator error: {result['error']}")

            audio_b64 = result.get("audio_b64")
            if not audio_b64:
                raise Exception("Operator returned no audio")

            # PROPOSED billing unit: input characters as tokens.
            billed_units = len(input_text)

            billing_result = storage.pin_complete_job_atomic(
                job_id=job.id,
                user_id=user_id,
                operator_id=operator.id,
                total_tokens=billed_units,
                model=routed_model,
                pricing_per_1k=operator.pricing_per_1k_tokens,
                input_tokens=billed_units,
                output_tokens=0,
                latency_ms=latency_ms,
            )

            if not billing_result.get('success'):
                storage.pin_fail_job(job.id, billing_result.get('error', 'billing_failed'))
                if billing_result.get('error') == 'INSUFFICIENT_CREDITS':
                    raise ValueError(f"Insufficient credits. Balance: ${billing_result.get('balance', 0):.4f}")
                raise Exception(f"Billing failed: {billing_result}")

            credits_charged = billing_result.get('total_cost', 0)
            storage.pin_update_reputation(operator.id, success=True, latency_ms=latency_ms)

            return {
                "id": f"pin-{job.id}",
                "object": "audio.speech",
                "created": int(time.time()),
                "model": bare_model,
                "voice": result.get("voice", voice),
                "audio_b64": audio_b64,
                "content_type": result.get("content_type", "application/octet-stream"),
                "bytes": result.get("bytes"),
                "pin_metadata": {
                    "operator_id": operator.id,
                    "latency_ms": latency_ms,
                    "credits_charged": credits_charged,
                    "billed_unit": "input_chars",
                    "billed_units": billed_units,
                    # Was hardcoded "websocket", which was the tell that this
                    # function predated both SSE and the bus.
                    "routing": "local" if local else "bus",
                },
            }

        except Exception as e:
            storage.pin_fail_job(job.id, str(e))
            storage.pin_update_reputation(operator.id, success=False)
            excluded_operators.append(operator.id)
            exclusion_reasons.append(f"{operator.id}: {e}")

            if attempt == max_attempts - 1:
                raise ValueError(f"All operators failed: {str(e)}")

    raise ValueError("PIN TTS failed after all attempts")
