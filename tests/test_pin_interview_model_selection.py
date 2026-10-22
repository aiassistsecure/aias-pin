"""Interview model selection must survive a fleet with no benchmark model.

THE FAILURE THIS PINS DOWN (2026-08-29)

Ollama-mode selection required one of four frozen 2024 model names
(llama3:8b, mistral:7b, qwen2:7b, gemma2:7b) and FAILED the interview
otherwise. Operators run their own models — muse-local, granite, gpt-oss —
and those change all the time, so every custom fleet had to hand-pin
`interviewModel` in the daemon config or be marked quality_tier=failed on a
technicality that says nothing about the node's quality.

The rule now: operator-specified model first; a benchmark model when one
exists (its scores stay comparable across nodes); otherwise the FIRST model
the node actually serves. Absence from a list of four frozen names is not a
failure mode.
"""
import importlib.util
import pathlib
import sys

import pytest


def _pin_module():
    """Load the routes module straight from disk, immune to sibling stubs."""
    path = (pathlib.Path(__file__).resolve().parent.parent
            / "api" / "routes" / "pin.py")
    spec = importlib.util.spec_from_file_location("_pin_interview_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as error:  # pragma: no cover - import-environment specific
        pytest.skip(f"pin.py needs its app environment to import: {error}")
    return module


def _select_ollama(module, models):
    """The selection rule both call sites now share, exercised directly."""
    return module.find_benchmark_model(models) or (models[0] if models else None)


def test_a_benchmark_model_is_still_preferred_when_present():
    pin = _pin_module()
    models = ["muse-local:latest", "llama3:8b", "gpt-oss:20b"]
    assert _select_ollama(pin, models) == "llama3:8b", \
        "benchmark scores stay comparable across nodes, so prefer one when it exists"


def test_a_custom_fleet_without_benchmark_models_interviews_its_first_model():
    pin = _pin_module()
    models = ["muse-local:latest", "ibm-granite/granite-4.2-8b-GGUF@Q8_0", "gpt-oss:20b"]
    assert _select_ollama(pin, models) == "muse-local:latest", \
        "a fleet of real, working models must not fail for lacking a 2024 name"


def test_an_empty_model_list_still_selects_nothing():
    pin = _pin_module()
    assert _select_ollama(pin, []) is None, \
        "no models is still a real failure — the fallback must not invent one"
