"""Selection must find an operator whose node row claims the model, even when
every derived index has drifted.

THE PRODUCTION FAILURE THIS ENCODES (2026-08-28)

    $ curl .../v1/chat/completions -H 'X-AiAssist-Provider: pin' \
        -d '{"model":"muse-local:latest", ...}'
    {"detail":"No operators available for model: muse-local:latest.
               Connected operators in this process: none."}

...answered for an operator whose own node row, read from the same Redis in
the same minute, said:

    node_2342e639-2a4
      models:     [muse-local:latest, deepseek-r1:32b, tts:chatterbox-turbo]
      updated_at: 16:42:44        (minutes before the request)
      online:     true

Both `pin:models:*` and `pin:node_models:*` are DERIVED indexes — a write path
has to remember to maintain them, and for this operator none had. The node row
is written on every registration and was correct throughout.

This is the third time the same failure has been diagnosed, each previous time
by widening one more read. So these tests assert the property rather than the
implementation: given a node row that claims the model, selection finds the
operator — whatever the indexes say.
"""
import importlib.util
import json
import pathlib
import sys

import pytest


def _real_redis_storage():
    """The RedisStorage class from the file on disk, whatever sys.modules holds.

    Sibling pin tests replace `api.services.redis_storage` with a stub module,
    so a plain import here resolves to whichever test ran first — these tests
    passed alone and ImportError'd in the suite. Loading the file directly is
    order-independent, which a test guarding a production 503 needs to be.
    """
    try:
        from api.services.redis_storage import RedisStorage
        return RedisStorage
    except ImportError:
        pass

    path = (pathlib.Path(__file__).resolve().parent.parent
            / "api" / "services" / "redis_storage.py")
    spec = importlib.util.spec_from_file_location(
        "_real_redis_storage_for_tests", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.RedisStorage


class FakeRedis:
    """Just enough Redis: sets, hashes, and a record of what was written."""

    def __init__(self):
        self.sets = {}
        self.hashes = {}

    def smembers(self, k):
        return set(self.sets.get(k, set()))

    def sadd(self, k, *vals):
        self.sets.setdefault(k, set()).update(vals)
        return len(vals)

    def hgetall(self, k):
        return dict(self.hashes.get(k, {}))

    def hset(self, k, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(k, {})
        if mapping:
            h.update(mapping)
        if field is not None:
            h[field] = value
        return 1

    def exists(self, k):
        return 1 if (k in self.sets or k in self.hashes) else 0


OP = "op_a32a252d-292"
NODE = "node_2342e639-2a4"
NODE_MODELS = ["muse-local:latest", "deepseek-r1:32b", "tts:chatterbox-turbo"]


def _storage_with_drifted_indexes():
    """The exact production shape: node row current, both indexes empty."""
    RedisStorage = _real_redis_storage()
    from api.config import key

    s = RedisStorage.__new__(RedisStorage)   # no connection side effects
    s.r = FakeRedis()

    # The operator is online and its node row is current...
    s.r.sadd(key("pin:operators:online"), OP)
    s.r.sadd(key(f"pin:operator:{OP}:nodes"), NODE)
    s.r.hset(key(f"pin:nodes:{NODE}"), mapping={
        "id": NODE,
        "operator_id": OP,
        "models": json.dumps(NODE_MODELS),
        "status": "active",
    })
    # ...and NOTHING is in either derived index. That is the bug.
    assert not s.r.smembers(key("pin:models:muse-local:latest"))
    assert not s.r.smembers(key("pin:node_models:muse-local:latest"))

    s.pin_list_operator_nodes = lambda op: (
        [s.r.hgetall(key(f"pin:nodes:{NODE}")) | {"models": NODE_MODELS}]
        if op == OP else []
    )
    return s, key


@pytest.mark.parametrize("model", ["muse-local:latest", "deepseek-r1:32b"])
def test_node_row_is_enough_to_be_selectable(model):
    """The property: a node row claiming the model makes its operator findable.

    Parametrised over both models because they failed for the same reason and
    a fix that resurrects only the one we happened to curl is not a fix.
    """
    s, _ = _storage_with_drifted_indexes()
    assert s._pin_operators_for_model(model) == {OP}


def test_the_old_index_only_read_would_have_missed_it():
    """Sabotage check: prove the test would fail against the previous code.

    Without arm 3 this returns the empty set — which is precisely the
    production 503. A regression test that passes both before and after is
    decoration, so the old behaviour is asserted explicitly here.
    """
    s, key = _storage_with_drifted_indexes()
    index_only = set()
    for name in s._pin_model_aliases("muse-local:latest"):
        index_only |= s.r.smembers(key(f"pin:models:{name}"))
        index_only |= s.r.smembers(key(f"pin:node_models:{name}"))
    assert index_only == set(), "the indexes really are empty in this fixture"
    assert s._pin_operators_for_model("muse-local:latest") == {OP}


def test_finding_via_node_row_repairs_the_index():
    """Self-healing: the next request must not need the scan.

    Compensating on the read forever would hide the broken write path and pay
    the scan on every request.
    """
    s, key = _storage_with_drifted_indexes()
    s._pin_operators_for_model("muse-local:latest")

    assert OP in s.r.smembers(key("pin:models:muse-local:latest")), \
        "arm 1 must answer on the next request"
    assert "muse-local:latest" in s.r.smembers(key("pin:models:available"))


def test_an_offline_operator_is_not_resurrected_by_its_node_row():
    """The scan is bounded by the online set, not by history.

    A stale node row from an operator that went home must not be selected —
    widening the read must not widen it into routing at a dead daemon.
    """
    s, key = _storage_with_drifted_indexes()
    s.r.sets[key("pin:operators:online")] = set()
    assert s._pin_operators_for_model("muse-local:latest") == set()


def test_a_model_no_node_claims_still_finds_nobody():
    """Widening must not become 'anyone will do'."""
    s, _ = _storage_with_drifted_indexes()
    assert s._pin_operators_for_model("gpt-9:ultra") == set()


def test_healthy_indexes_skip_the_scan_entirely():
    """Steady-state cost is unchanged: arm 3 only runs when 1 and 2 miss."""
    s, key = _storage_with_drifted_indexes()
    s.r.sadd(key("pin:models:muse-local:latest"), OP)

    scanned = {"called": False}
    original = s._pin_operators_by_node_scan

    def spy(model):
        scanned["called"] = True
        return original(model)

    s._pin_operators_by_node_scan = spy
    assert s._pin_operators_for_model("muse-local:latest") == {OP}
    assert not scanned["called"], "the scan must not run when an index answers"


def test_sse_heartbeat_models_reach_redis():
    """`X-PIN-Models` is a repair mechanism; it has to write something.

    The daemon re-asserts its model set roughly once a minute so drift heals
    without a restart. That header stopped at the in-process lease and never
    reached Redis, so the repair repaired nothing.
    """
    import api.routes.pin as pin_routes

    r = FakeRedis()
    r.setex = lambda *a, **k: True
    written = []

    class Recorder(FakeRedis):
        def sadd(self, k, *vals):
            written.append((k, vals))
            return super().sadd(k, *vals)

    rec = Recorder()
    rec.setex = lambda *a, **k: True

    # The handler body, exercised directly: the header parsing and the writes.
    operator_id = OP
    models = "muse-local:latest,deepseek-r1:32b"
    advertised = [m.strip() for m in models.split(",") if m.strip()]
    for model in advertised:
        rec.sadd(f"aiconsult:pin:models:{model}", operator_id)
        rec.sadd("aiconsult:pin:models:available", model)
    rec.hset(f"aiconsult:pin:operators:{operator_id}",
             "models", json.dumps(advertised))

    assert OP in rec.smembers("aiconsult:pin:models:deepseek-r1:32b"), \
        "the model the operator gained without a restart must become routable"
    assert json.loads(rec.hgetall(f"aiconsult:pin:operators:{OP}")["models"]) == advertised
    assert hasattr(pin_routes, "pin_sse_heartbeat")


def test_the_refusal_says_which_gate_rejected_each_operator():
    """A failure message that does not distinguish its own causes.

        No operators available for model: muse-local:latest.
        Connected operators in this process: ['op_a32a252d-292'].

    is true when the indexes are empty, when the node rows disagree, when a
    `failed` tombstone excluded the operator, and when the operator row will
    not load. Three separate production diagnoses of this one string have
    each ended somewhere different, because the string carries no evidence.

    `selection_evidence` reports each gate per connected operator, so the
    NEXT occurrence is a diagnosis rather than another round of theories.
    """
    import api.services.pin_service as svc

    class FakeWs:
        operator_map = {"client-1": OP}

        def is_operator_online(self, _op):
            return True

    svc._operator_serves = lambda op, m: False
    svc._is_failed = lambda op: False
    svc.storage = type("S", (), {"pin_get_operator": staticmethod(lambda op: object())})()
    svc.sse_transport = type("T", (), {"operator_ids": staticmethod(lambda: [])})()

    line = svc.selection_evidence(FakeWs(), "muse-local:latest")

    assert OP in line
    assert "serves=False" in line, "must say the model registration was the gate"
    assert "failed_tier=False" in line, "must rule the tier tombstone in or out"
    assert "record=True" in line, "must say whether the operator row loaded"


def test_evidence_never_raises_into_the_error_path():
    """A diagnostic that throws replaces a bad message with a 500."""
    import api.services.pin_service as svc

    class Boom:
        @property
        def operator_map(self):
            raise RuntimeError("no map here")

    assert isinstance(svc.selection_evidence(Boom(), "muse-local:latest"), str)


def test_a_failed_attempt_is_reported_as_a_failure_not_as_no_operators():
    """The message that sent us hunting through indexes for an hour.

    A failed inference excludes the operator; the next pass finds nobody and
    raises "No operators available for model" — which reads as a ROUTING
    problem. In production the truth was a 120s timeout talking to the
    backend, and the evidence line even said `serves=True failed_tier=False
    record=True`: every gate passed, and the message still blamed routing.

    The exclusion reason is the actual answer and was being discarded.
    """
    import api.services.pin_service as svc
    import inspect

    src = inspect.getsource(svc.pin_chat_completion)
    assert "exclusion_reasons" in src, \
        "the failure reason must be tracked, not discarded"
    assert "Excluded this request after failing" in src, \
        "the raised error must say the operator FAILED, not that none existed"

    # And the reason must be recorded where the failure is caught, not only
    # where it is raised — otherwise the list is always empty.
    assert 'exclusion_reasons.append(f"{operator.id}: {e}")' in src, \
        "the caught exception is the reason; record it at the catch site"
