"""PIN model-id resolution.

Every other provider ships a fixed catalog (ai_orchestrator.DEFAULT_MODELS), so
`get_model_for_user` can fall back to a hardcoded id. PIN has no such catalog:
its models are whatever the currently-connected operators registered.

That pool is supplied by `storage.pin_list_available_models()`, which derives
it from the node rows of ONLINE operators. It is NOT `pin:models:available`
read directly: that set is append-only (7 writers, zero removers) and so lists
every model any operator ever advertised, including ones evicted from the box
weeks earlier. Resolving against the raw set is how a request gets routed to a
model nobody serves -- the caller then sees "No operators available" for a
model the registry claimed was there.

There was no "pin" key in DEFAULT_MODELS, so provider "pin" fell through to the
platform default -- `llama-3.3-70b-versatile`. `pin_select_operator` then does
an EXACT set-membership test on `pin:models:{model}`, so the call died as
"No operators available for model: llama-3.3-70b-versatile" while the operator
sat online happily serving `muse-local:latest`. The daemon never saw a request
and the error named a model nobody had asked for.

This module is pure (no storage, no I/O) so the resolution rules are unit
testable, same shape as api/utils/pin_auth.py.
"""
from __future__ import annotations

from typing import Iterable, List, Optional


class PinModelUnavailable(ValueError):
    """Raised when no PIN model can be resolved for a request.

    Subclasses ValueError so the existing handler in api/routes/pin.py maps a
    model-availability failure to 503 rather than 500.
    """


def _norm(model: str) -> str:
    return (model or "").strip().lower()


def _untagged(model: str) -> str:
    """Ollama treats `name` and `name:latest` as the same model; the registry
    stores whatever string the daemon reported."""
    return model[: -len(":latest")] if model.endswith(":latest") else model


def resolve_pin_model(requested: str, available: Iterable[str]) -> Optional[str]:
    """Return the registered model id matching `requested`, or None.

    Matching is exact first, then case-insensitive, then insensitive to an
    implicit `:latest` tag in either direction. Deliberately NOT fuzzy: prefix
    or substring matching would silently route a request to a different model.
    """
    pool: List[str] = [m for m in available if m]
    if not requested:
        return None

    if requested in pool:
        return requested

    want = _norm(requested)
    for m in pool:
        if _norm(m) == want:
            return m

    want_base = _untagged(want)
    for m in pool:
        if _untagged(_norm(m)) == want_base:
            return m

    return None


def choose_pin_model(preferred: Optional[str],
                     available: Iterable[str]) -> str:
    """Pick the model id to send to the PIN network.

    `preferred` is the user's configured model for provider "pin" (may be None).
    Never invents an id and never falls back to another provider's default: an
    unresolvable request raises with the live model list in the message.
    """
    pool = sorted({m for m in available if m})

    if preferred:
        match = resolve_pin_model(preferred, pool)
        if match:
            return match
        raise PinModelUnavailable(
            f"No operators available for model: {preferred}. "
            f"Models on the PIN network right now: "
            f"{', '.join(pool) if pool else 'none -- no operator is connected'}."
        )

    if len(pool) == 1:
        return pool[0]

    if not pool:
        raise PinModelUnavailable(
            "No operators available: PIN is the selected provider but no "
            "operator is connected with a registered model.")

    raise PinModelUnavailable(
        "PIN is the selected provider but no PIN model is configured, and the "
        f"network is serving several: {', '.join(pool)}. Set a default model "
        "for provider 'pin' to choose one.")
