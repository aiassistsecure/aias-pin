"""The advertised model catalog must describe the network NOW, not its history.

THE BUG THIS GUARDS (observed 2026-09-07)

    GET /api/v1/pin/network/models  ->  48 models
    the one online operator actually served 4

The list still carried a CPU node's entire ollama library, `muse-local:latest`
and `deepseek-r1:32b` (evicted from the GPU box weeks earlier), a pile of dead
`:cloud` ids, and a stale cloudflare-tunnel hostname. Nothing ever removes a
model:

    pin:models:available        7 sadd sites, ZERO removers
    pin:models:{model}         14 sadd sites, ZERO removers
    pin:node_models:{model}     1 sadd site,  removed by pin_update_node /
                                              pin_delete_node

Going offline, failing an interview, being reaped for a stale heartbeat, or
deleting the model from the box leaves the first two untouched. So any consumer
reading that endpoint as its menu -- a model picker, GWW, Yente -- selects
something nobody serves and receives "No operators available", the same 503
that took an afternoon to trace.

FILTERED, NOT PRUNED. The stored sets are history and other code reads them, so
this corrects the ANSWER and leaves the data alone. There is a test below that
asserts the stored set is unchanged, byte for byte, after the read.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class FakeRedis:
    """Sets and hashes, enough for the catalog paths."""

    def __init__(self):
        self.sets = {}
        self.hashes = {}

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(str(m) for m in members)
        return len(members)

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(str(field))

    def hset(self, key, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update({str(k): str(v) for k, v in mapping.items()})
        if field is not None:
            h[str(field)] = str(value)
        return 1

    def exists(self, key):
        return 1 if (key in self.sets or key in self.hashes) else 0


def build(online_nodes, catalog):
    """online_nodes: {operator_id: {node_id: [models]}}; catalog: extra history."""
    from api.services.redis_storage import RedisStorage
    from api.config import key

    st = RedisStorage.__new__(RedisStorage)
    st.r = FakeRedis()

    for model in catalog:
        st.r.sadd(key("pin:models:available"), model)

    for op_id, nodes in online_nodes.items():
        st.r.sadd(key("pin:operators:online"), op_id)
        st.r.sadd(key("pin:operators:all"), op_id)
        st.r.hset(key(f"pin:operators:{op_id}"), mapping={
            "id": op_id, "user_id": "u1", "name": "op", "capacity": "7",
            "created_at": "2026-09-07T00:00:00Z",
            "updated_at": "2026-09-07T00:00:00Z",
        })
        for node_id, models in nodes.items():
            st.r.sadd(key(f"pin:operator:{op_id}:nodes"), node_id)
            st.r.hset(key(f"pin:nodes:{node_id}"), mapping={
                "id": node_id, "operator_id": op_id, "alias": "n",
                "endpoint": "http://127.0.0.1:11434",
                "models": __import__("json").dumps(models),
            })
    return st


THE_FOUR = ["GLM-4-32B", "llama-3.1-8b", "qwen-3-8b", "tts:chatterbox-turbo"]

# The real shape from production: the live four plus a CPU node's abandoned
# library and two models evicted from the GPU box.
HISTORY = THE_FOUR + [
    "muse-local:latest", "deepseek-r1:32b", "tinyllama:latest", "codellama:34b",
    "glm-4.6:cloud", "phi3:latest",
    "passion-med-pan-skiing.trycloudflare.com/ns/bleed_509:latest",
]


class CatalogReportsOnlyLiveModels(unittest.TestCase):

    def test_only_models_an_online_operator_serves_are_advertised(self):
        st = build({"op_gpu": {"node_gpu": THE_FOUR}}, HISTORY)

        self.assertEqual(st.pin_list_available_models(), sorted(THE_FOUR))

    def test_an_offline_operators_models_are_not_advertised(self):
        """The CPU node is offline BY DESIGN -- it is meant to fail the
        interview. Its ollama library must not become the network's menu."""
        st = build({"op_gpu": {"node_gpu": THE_FOUR}}, HISTORY)
        # The offline operator exists and owns nodes, but is not in the online
        # set. Nothing it advertised may appear.
        st.r.sadd("aiconsult:pin:operators:all", "op_cpu")
        st.r.sadd("aiconsult:pin:operator:op_cpu:nodes", "node_cpu")
        st.r.hset("aiconsult:pin:nodes:node_cpu", mapping={
            "id": "node_cpu", "operator_id": "op_cpu", "alias": "cpu",
            "endpoint": "http://127.0.0.1:11434",
            "models": '["tinyllama:latest", "codellama:34b"]',
        })

        advertised = st.pin_list_available_models()
        self.assertNotIn("tinyllama:latest", advertised)
        self.assertNotIn("codellama:34b", advertised)
        self.assertEqual(advertised, sorted(THE_FOUR))

    def test_the_stored_ledger_is_not_modified_by_reading(self):
        """FILTERED, NOT PRUNED -- the load-bearing property.

        A read path that deletes what it disagrees with is unrecoverable when
        the disagreement is wrong. The set must come out of a read identical to
        how it went in.
        """
        st = build({"op_gpu": {"node_gpu": THE_FOUR}}, HISTORY)
        before = set(st.r.smembers("aiconsult:pin:models:available"))

        st.pin_list_available_models()
        st.pin_list_available_models()
        st.pin_get_network_status()

        after = set(st.r.smembers("aiconsult:pin:models:available"))
        self.assertEqual(before, after, "reading must not prune the ledger")
        self.assertEqual(len(after), len(HISTORY))

    def test_history_stays_reachable(self):
        st = build({"op_gpu": {"node_gpu": THE_FOUR}}, HISTORY)

        self.assertEqual(sorted(st.pin_list_all_advertised_models()),
                         sorted(HISTORY))

    def test_an_empty_live_scan_falls_back_to_the_ledger(self):
        """A stale online set must not empty the catalog.

        Too long is a nuisance; empty is an outage for every consumer that
        reads this as its menu. The failure direction is deliberate, and this
        is the false-positive case for the whole change.
        """
        st = build({}, HISTORY)          # nobody online

        advertised = st.pin_list_available_models()
        self.assertEqual(sorted(advertised), sorted(HISTORY),
                         "with no live signal, report the ledger rather than "
                         "claiming the network offers nothing")

    def test_network_status_agrees_with_the_models_route(self):
        """Two endpoints disagreeing about what the network offers is its own
        bug class -- and the status route read the raw set directly."""
        st = build({"op_gpu": {"node_gpu": THE_FOUR}}, HISTORY)

        status = st.pin_get_network_status()
        self.assertEqual(sorted(status.models), sorted(st.pin_list_available_models()))
        self.assertEqual(sorted(status.models), sorted(THE_FOUR))

    def test_a_broken_node_row_does_not_take_down_the_catalog(self):
        st = build({"op_gpu": {"node_gpu": THE_FOUR}}, HISTORY)
        st.r.sadd("aiconsult:pin:operator:op_gpu:nodes", "node_corrupt")
        st.r.hset("aiconsult:pin:nodes:node_corrupt", mapping={
            "id": "node_corrupt", "operator_id": "op_gpu", "alias": "x",
            "endpoint": "http://x", "models": "{{{not json",
        })

        # Must still answer, and must still include the healthy node's models.
        advertised = st.pin_list_available_models()
        for m in THE_FOUR:
            self.assertIn(m, advertised)


class ResolutionUsesTheLiveList(unittest.TestCase):
    """`resolve_pin_model` is pure and matches against whatever pool it is
    handed. Feeding it the live list is what stops a request resolving onto a
    model that was evicted weeks ago."""

    def test_a_dead_model_no_longer_resolves(self):
        from api.utils.pin_models import resolve_pin_model

        live = THE_FOUR
        self.assertIsNone(resolve_pin_model("muse-local:latest", live))
        self.assertEqual(resolve_pin_model("GLM-4-32B", live), "GLM-4-32B")

    def test_the_tts_prefixed_name_still_resolves_exactly(self):
        from api.utils.pin_models import resolve_pin_model

        self.assertEqual(
            resolve_pin_model("tts:chatterbox-turbo", THE_FOUR),
            "tts:chatterbox-turbo")


if __name__ == "__main__":
    unittest.main(verbosity=2)
