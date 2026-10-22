"""The completion must survive however many envelopes the daemon wrapped it in.

THE PRODUCTION 500 (2026-08-28)

Routing had just been fixed; the operator was selected, the job ran, the
result came back — and the caller got a 500:

    [PIN SERVICE] Selected operator op_a32a252d-292 for model muse-local:latest
    [PIN SERVICE] Sending inference request pin_req_JcWCDaRD5IGe-YQMt0X-zw
    [PIN SERVICE] Inference result received: ['request_id', 'result', 'type']
    Provider API error: list index out of range
    POST /v1/chat/completions 500 Internal Server Error

Those three keys are the giveaway: that is an ENVELOPE, not a completion.

The SSE daemon routes results through `send_uplink_json`, which calls
`inference_result(request_id, &parsed)` with the whole parsed envelope, so the
posted body is

    {"request_id": ..., "result": {"type": "INFERENCE_RESPONSE",
                                   "request_id": ...,
                                   "result": {...the real completion...}}}

The server unwrapped exactly one layer, `pin_chat_completion` read `choices`
off the remaining envelope and got `[]`, and `public_api` did `choices[0]`.

The daemon is fixed separately. It must be fixed here too, and first:
operators upgrade on their own schedule, and a gateway that only works with
the newest daemon breaks everyone who has not rebuilt.
"""
import importlib.util
import pathlib
import sys


def _unwrap():
    """Pull the helper out of the routes module without importing FastAPI deps.

    `api.routes.pin` drags in the whole application; this test only needs one
    pure function, so it is read from source and exec'd in isolation.
    """
    path = (pathlib.Path(__file__).resolve().parent.parent
            / "api" / "routes" / "pin.py")
    src = path.read_text()
    start = src.index("def _unwrap_result_envelope(result):")
    end = src.index("class _StaleStreamCleanup", start)
    namespace = {}
    exec(compile(src[start:end], str(path), "exec"), namespace)  # noqa: S102
    return namespace["_unwrap_result_envelope"]


COMPLETION = {
    "id": "chatcmpl-abc",
    "choices": [{"index": 0,
                 "message": {"role": "assistant", "content": "OK"},
                 "finish_reason": "stop"}],
    "usage": {"prompt_tokens": 12, "completion_tokens": 2, "total_tokens": 14},
}


def test_the_double_wrapped_shape_that_produced_the_500():
    """Exactly what the SSE daemon posts today."""
    envelope = {
        "type": "INFERENCE_RESPONSE",
        "request_id": "pin_req_JcWCDaRD5IGe-YQMt0X-zw",
        "result": COMPLETION,
    }
    assert _unwrap()(envelope) == COMPLETION
    assert _unwrap()(envelope)["choices"][0]["message"]["content"] == "OK", \
        "the caller must get the answer, not an IndexError"


def test_an_already_unwrapped_completion_is_left_alone():
    """The fixed daemon, and the WebSocket path, both send it bare.

    Unwrapping must not damage the correct shape, or fixing the daemon would
    break the gateway.
    """
    assert _unwrap()(COMPLETION) == COMPLETION


def test_a_completion_that_merely_has_a_type_field_is_not_stripped():
    """`type` alone is not an envelope.

    Only `type` PLUS a dict `result` is. A completion carrying an unrelated
    `type` must survive intact — over-eager unwrapping would silently discard
    the payload.
    """
    payload = dict(COMPLETION, type="chat.completion")
    assert _unwrap()(payload) == payload


def test_nesting_is_bounded():
    """Three layers is generous; unbounded unwrapping is an infinite loop
    waiting for a malformed body."""
    deep = COMPLETION
    for _ in range(10):
        deep = {"type": "INFERENCE_RESPONSE", "request_id": "x", "result": deep}
    out = _unwrap()(deep)
    assert isinstance(out, dict), "must terminate and return something usable"


def test_none_and_non_dicts_pass_through():
    """A timeout resolves with None; the unwrapper must not turn that into a
    crash on the error path."""
    assert _unwrap()(None) is None
    assert _unwrap()("nope") == "nope"
    assert _unwrap()([1, 2]) == [1, 2]


def test_an_error_envelope_still_reaches_the_caller():
    """`INFERENCE_ERROR` uses the same uplink, and pin_service checks for an
    `error` key. Unwrapping must expose it, or a real operator error is
    reported as an empty completion."""
    envelope = {
        "type": "INFERENCE_ERROR",
        "request_id": "pin_req_x",
        "result": {"error": "model not loaded"},
    }
    assert _unwrap()(envelope) == {"error": "model not loaded"}
