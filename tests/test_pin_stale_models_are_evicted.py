"""The stored PIN catalog must be pruned, not merely read around.

WHAT WAS ALREADY FIXED, AND WHAT WAS NOT

An earlier change corrected the ANSWER: `pin_list_available_models()` filters
the stored ledger down to what online node rows report, so `/network/models`
stopped advertising 48 models when the one online operator served 4. It
deliberately left the DATA alone.

That is not enough. `api/utils/pin_models.py` resolves a requested id against
the pool, and `_pin_operators_for_model` reads `pin:models:{model}` — so every
un-evicted entry stays a live route onto a model nobody has served for weeks.
The set has seven `sadd` sites and, until `pin_evict_stale_models`, zero
removers: going offline, failing an interview, being reaped for a stale
heartbeat, or deleting the model from the box left it untouched.

THE SAFETY PROPERTY IS THE REFUSAL

A read that comes up empty can fall back to history. A delete cannot — there is
nothing to fall back to afterwards. A stale online set, a Redis blip, or every
operator restarting at once all produce an empty live scan, and pruning against
that would empty the catalog outright. So the writer declines and says why, and
`test_an_empty_live_scan_refuses_to_prune_anything` is the test that matters
most in this file.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class FakeRedis:
    """Sets and hashes, plus the removers the prune needs."""

    def __init__(self):
        self.sets = {}
        self.hashes = {}

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(str(m) for m in members)
        return len(members)

    def srem(self, key, *members):
        s = self.sets.get(key)
        if not s:
            return 0
        removed = 0
        for m in members:
            if str(m) in s:
                s.discard(str(m))
                removed += 1
        return removed

    def delete(self, *keys):
        gone = 0
        for k in keys:
            gone += 1 if (self.sets.pop(k, None) is not None
                          or self.hashes.pop(k, None) is not None) else 0
        return gone

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update({str(k): str(v) for k, v in mapping.items()})
        if field is not None:
            h[str(field)] = str(value)
        return 1

    def exists(self, key):
        return 1 if (key in self.sets or key in self.hashes) else 0


THE_FOUR = ["GLM-4-32B", "llama-3.1-8b", "qwen-3-8b", "tts:chatterbox-turbo"]

# Production shape: the live four, plus a CPU node's abandoned ollama library,
# two models evicted from the GPU box weeks earlier, dead `:cloud` ids, and a
# stale cloudflare-tunnel hostname.
STALE = [
    "muse-local:latest",
    "deepseek-r1:32b",
    "tinyllama:latest",
    "codellama:34b",
    "glm-4.6:cloud",
    "phi3:latest",
    "passion-med-pan-skiing.trycloudflare.com/ns/bleed_509:latest",
]


def build(online_nodes, catalog):
    """online_nodes: {operator_id: {node_id: [models]}}; catalog: stored set."""
    from api.services.redis_storage import RedisStorage
    from api.config import key

    st = RedisStorage.__new__(RedisStorage)
    st.r = FakeRedis()

    for model in catalog:
        st.r.sadd(key("pin:models:available"), model)
        # Each catalog entry also has a per-model operator index, which is the
        # one `_pin_operators_for_model` reads.
        st.r.sadd(key(f"pin:models:{model}"), "op-legacy")

    for op_id, nodes in online_nodes.items():
        st.r.sadd(key("pin:operators:online"), op_id)
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
                "models": json.dumps(models),
            })
    return st


LIVE_ONE_OPERATOR = {"op-1": {"node-1": THE_FOUR}}


class EvictStaleModels(unittest.TestCase):

    def test_the_models_nobody_serves_are_removed(self):
        from api.config import key
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        report = st.pin_evict_stale_models()

        self.assertEqual(sorted(report["evicted"]), sorted(STALE))
        self.assertEqual(
            st.r.smembers(key("pin:models:available")),
            set(THE_FOUR),
            "only what an online node reports may remain",
        )

    def test_the_live_models_are_kept(self):
        from api.config import key
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        st.pin_evict_stale_models()

        for model in THE_FOUR:
            self.assertIn(model, st.r.smembers(key("pin:models:available")))

    def test_the_per_model_operator_index_goes_too(self):
        # Otherwise `_pin_operators_for_model` keeps finding an operator for a
        # model the catalog no longer lists — a route that outlives its entry.
        from api.config import key
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        st.pin_evict_stale_models()

        self.assertEqual(
            st.r.smembers(key("pin:models:muse-local:latest")),
            set(),
            "the stale model's operator index must not survive it",
        )
        self.assertEqual(
            st.r.smembers(key("pin:models:GLM-4-32B")),
            {"op-legacy"},
            "a live model's index is untouched",
        )

    def test_an_empty_live_scan_refuses_to_prune_anything(self):
        # THE ONE THAT MATTERS. No online operator means either a genuinely
        # empty network or — far likelier — a stale online set. Pruning on that
        # basis empties the catalog, and a delete has no fallback.
        from api.config import key
        st = build({}, THE_FOUR + STALE)

        report = st.pin_evict_stale_models()

        self.assertEqual(report["evicted"], [])
        self.assertIn("refused", report)
        self.assertEqual(
            st.r.smembers(key("pin:models:available")),
            set(THE_FOUR + STALE),
            "nothing may be removed when there is nothing to prune against",
        )

    def test_the_refusal_says_why(self):
        st = build({}, THE_FOUR)
        report = st.pin_evict_stale_models()
        self.assertRegex(report["refused"], r"empty scan|stale online set")

    def test_a_dry_run_reports_without_removing(self):
        from api.config import key
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        report = st.pin_evict_stale_models(dry_run=True)

        self.assertEqual(sorted(report["evicted"]), sorted(STALE))
        self.assertEqual(
            st.r.smembers(key("pin:models:available")),
            set(THE_FOUR + STALE),
            "a dry run must not touch the stored set",
        )

    def test_running_twice_is_a_no_op_the_second_time(self):
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        first = st.pin_evict_stale_models()
        second = st.pin_evict_stale_models()

        self.assertEqual(sorted(first["evicted"]), sorted(STALE))
        self.assertEqual(second["evicted"], [])

    def test_a_model_served_by_a_second_operator_is_not_evicted(self):
        # Two operators, disjoint model sets. Anything either serves is live.
        from api.config import key
        st = build(
            {"op-1": {"node-1": ["GLM-4-32B"]},
             "op-2": {"node-2": ["qwen-3-8b"]}},
            ["GLM-4-32B", "qwen-3-8b"] + STALE,
        )

        st.pin_evict_stale_models()

        self.assertEqual(
            st.r.smembers(key("pin:models:available")),
            {"GLM-4-32B", "qwen-3-8b"},
        )

    def test_a_broken_node_row_does_not_evict_the_whole_catalog(self):
        # The guard is per node, not per operator. One unparseable `models`
        # field must not erase the models its siblings report.
        from api.config import key
        st = build(
            {"op-1": {"node-good": ["GLM-4-32B"], "node-bad": []}},
            ["GLM-4-32B"] + STALE,
        )
        st.r.hset(key("pin:nodes:node-bad"), field="models", value="{not json")

        report = st.pin_evict_stale_models()

        self.assertIn("GLM-4-32B", st.r.smembers(key("pin:models:available")))
        self.assertNotIn("GLM-4-32B", report["evicted"])

    def test_a_tts_model_is_not_special_and_survives_if_served(self):
        # It is served, so it stays. The catalog's job is reachability, not
        # taste — filtering speech models out of a text picker is the caller's
        # job, and doing it here would hide a working model from the one
        # consumer that wants it.
        from api.config import key
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        st.pin_evict_stale_models()

        self.assertIn(
            "tts:chatterbox-turbo",
            st.r.smembers(key("pin:models:available")),
        )

    def test_the_read_path_and_the_pruned_set_now_agree(self):
        from api.config import key
        st = build(LIVE_ONE_OPERATOR, THE_FOUR + STALE)

        st.pin_evict_stale_models()

        self.assertEqual(
            sorted(st.pin_list_available_models()),
            sorted(st.r.smembers(key("pin:models:available"))),
            "after a prune, filtering and storage must not disagree",
        )


class PinHasNoStaticCatalogue(unittest.TestCase):

    def test_the_pin_provider_advertises_no_hardcoded_models(self):
        # PIN's models are whatever its operators registered; a static list is
        # wrong by construction rather than merely out of date.
        from api.providers.config import PROVIDERS, ProviderType

        self.assertEqual(PROVIDERS[ProviderType.PIN].default_models, [])

    def test_the_unroutable_ids_are_gone(self):
        from api.providers.config import PROVIDERS, ProviderType

        ids = [m.id for m in PROVIDERS[ProviderType.PIN].default_models]
        for dead in ("pin:auto", "pin:mistral:7b", "pin:phi3:14b",
                     "pin:llama3.3:70b"):
            self.assertNotIn(dead, ids)

    def test_no_pin_prefixed_id_could_ever_have_resolved(self):
        # Why deletion rather than an update: the resolver matches against the
        # live pool of BARE ids and never strips the prefix, so the whole
        # `pin:`-prefixed shape was unroutable regardless of its contents.
        from api.utils.pin_models import resolve_pin_model

        self.assertIsNone(resolve_pin_model("pin:mistral:7b", THE_FOUR))
        self.assertIsNone(resolve_pin_model("pin:auto", THE_FOUR))
        self.assertEqual(resolve_pin_model("GLM-4-32B", THE_FOUR), "GLM-4-32B")

    def test_other_providers_keep_their_catalogues(self):
        # Their lists are vendor-defined and mostly correct; this change must
        # not touch them.
        from api.providers.config import PROVIDERS, ProviderType

        self.assertTrue(PROVIDERS[ProviderType.GROQ].default_models)


if __name__ == "__main__":
    unittest.main()
