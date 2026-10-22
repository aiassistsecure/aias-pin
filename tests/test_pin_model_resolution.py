"""PIN model-id resolution regression tests.

Guards the defect where provider "pin" fell through DEFAULT_MODELS to
`llama-3.3-70b-versatile` and pin_select_operator's exact set-membership test
reported "No operators available" for a model nobody requested.
"""
import pytest

from api.utils.pin_models import (
    PinModelUnavailable,
    choose_pin_model,
    resolve_pin_model,
)

AVAILABLE = ["muse-local:latest"]


def test_exact_match_wins():
    assert resolve_pin_model("muse-local:latest", AVAILABLE) == "muse-local:latest"


def test_implicit_latest_tag_resolves_both_directions():
    assert resolve_pin_model("muse-local", AVAILABLE) == "muse-local:latest"
    assert resolve_pin_model("muse-local:latest", ["muse-local"]) == "muse-local"


def test_case_insensitive():
    assert resolve_pin_model("MUSE-Local:Latest", AVAILABLE) == "muse-local:latest"


def test_no_fuzzy_matching():
    # "muse" is NOT muse-local -- silently routing it would answer the wrong
    # question with someone else's model.
    assert resolve_pin_model("muse", AVAILABLE) is None
    assert resolve_pin_model("muse-local-v2", AVAILABLE) is None
    assert resolve_pin_model("", AVAILABLE) is None


def test_never_falls_through_to_another_providers_default():
    with pytest.raises(PinModelUnavailable) as exc:
        choose_pin_model("llama-3.3-70b-versatile", AVAILABLE)
    msg = str(exc.value)
    assert "No operators available for model: llama-3.3-70b-versatile" in msg
    # the live registry is named so the operator can see the real mismatch
    assert "muse-local:latest" in msg


def test_single_registered_model_is_used_when_no_preference():
    assert choose_pin_model(None, AVAILABLE) == "muse-local:latest"


def test_ambiguous_network_requires_an_explicit_choice():
    with pytest.raises(PinModelUnavailable) as exc:
        choose_pin_model(None, ["muse-local:latest", "llama3:8b"])
    assert "no PIN model is configured" in str(exc.value)


def test_empty_network_says_so():
    with pytest.raises(PinModelUnavailable) as exc:
        choose_pin_model(None, [])
    assert "No operators available" in str(exc.value)
    with pytest.raises(PinModelUnavailable) as exc:
        choose_pin_model("muse-local", [])
    assert "no operator is connected" in str(exc.value)


def test_pin_model_unavailable_is_a_valueerror():
    # api/routes/pin.py maps ValueError containing "no operators" to 503.
    assert issubclass(PinModelUnavailable, ValueError)
    err = PinModelUnavailable("No operators available for model: x")
    assert "no operators" in str(err).lower()
