"""Tools reach the operator — and a request without tools is unchanged.

WHY THIS EXISTS

The generic `ChatCompletionRequest` has carried `tools` since the beginning.
`PinChatCompletionRequest` did not, so a caller could describe tools to every
hosted provider and NOT to their own operator's model.

That gap forced structured extraction to be done as prose instructions in the
prompt, and it was paid for in exactly the ways prose is: invented relationship
predicates, quotes stitched from two places in a document, malformed blocks, a
model that spent six minutes rehearsing the output rules instead of applying
them, and — measured on this stack — 37 rejected claims out of 63 on one
resume.

THE LOAD-BEARING TEST IS THE NEGATIVE ONE

`tools` must be OMITTED when absent, never sent as null. llama.cpp applies its
chat template differently for a present-but-null `tools`, and several
OpenAI-compatible servers reject the null outright. Every PIN inference today
sends no tools, so if this change alters those payloads at all it is a change
to all existing traffic rather than an addition for the callers that ask.
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.models.schemas import PinChatCompletionRequest  # noqa: E402


class PinRequestCarriesTools(unittest.TestCase):

    def test_tools_are_accepted_on_the_pin_request(self):
        request = PinChatCompletionRequest(
            model="pin:muse-local:latest",
            messages=[{"role": "user", "content": "hi"}],
            tools=[{
                "type": "function",
                "function": {
                    "name": "declare_role",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "role": {
                                "type": "string",
                                # The whole point of a schema over a prompt: a
                                # fifth role is unrepresentable, not rejected
                                # after the fact.
                                "enum": [
                                    "hiring", "seeking_employment",
                                    "seeking_funding", "funding_startups",
                                ],
                            },
                        },
                        "required": ["role"],
                    },
                },
            }],
            tool_choice="auto",
        )
        self.assertEqual(len(request.tools), 1)
        self.assertEqual(request.tool_choice, "auto")
        self.assertEqual(
            request.tools[0]["function"]["parameters"]["properties"]["role"]["enum"][0],
            "hiring",
        )

    def test_absent_tools_default_to_none_not_an_empty_list(self):
        """None and [] are different to a chat template. Keep the distinction."""
        request = PinChatCompletionRequest(
            model="pin:muse-local:latest",
            messages=[{"role": "user", "content": "hi"}],
        )
        self.assertIsNone(request.tools)
        self.assertIsNone(request.tool_choice)

    def test_existing_fields_and_defaults_are_untouched(self):
        """max_tokens 8192 was itself a hard-won fix: 1024 silently truncated a
        reasoning model mid-deliberation. Adding tools must not disturb it."""
        request = PinChatCompletionRequest(
            model="pin:muse-local:latest",
            messages=[{"role": "user", "content": "hi"}],
        )
        self.assertEqual(request.max_tokens, 8192)
        self.assertEqual(request.temperature, 0.7)
        self.assertFalse(request.stream)


class PayloadOmitsToolsWhenAbsent(unittest.TestCase):
    """The payload builders are read as source.

    Both live inside long async functions gated on credits, operator selection
    and a live socket, so driving them end to end here would test the harness
    rather than the rule. What matters is a property of the code: `tools` is
    added under a truthiness check and never assigned unconditionally.

    Reading source is the same technique the Yente suite uses for "the function
    exists, it is tested, and nothing in production calls it" — invisible to any
    test that supplies the missing call itself.
    """

    def setUp(self):
        self.source = (Path(__file__).resolve().parents[1]
                       / "api" / "services" / "pin_service.py").read_text(encoding="utf-8")

    def test_both_payload_builders_guard_on_tools(self):
        # One guard for the non-streaming payload, one for the streaming one.
        self.assertEqual(
            self.source.count('if tools:'), 2,
            "both payload builders must add tools only when the caller sent them",
        )

    def test_tools_is_never_assigned_unconditionally(self):
        for forbidden in ('"tools": tools', "'tools': tools"):
            self.assertNotIn(
                forbidden, self.source,
                "a request without tools must stay byte-identical to today's",
            )

    def test_tool_choice_only_travels_with_tools(self):
        # tool_choice without tools is meaningless and some servers 400 on it.
        guarded = self.source.split("if tools:")
        self.assertEqual(len(guarded), 3, "expected exactly two guarded blocks")
        for block in guarded[1:]:
            head = block[:400]
            self.assertIn("if tool_choice:", head,
                          "tool_choice must be nested inside the tools guard")

    def test_both_entry_points_accept_the_parameters(self):
        for signature in ("tools: Optional[List[dict]] = None",
                          "tool_choice: Optional[str] = None"):
            self.assertEqual(
                self.source.count(signature), 2,
                f"{signature} must be on BOTH pin_chat_completion and "
                f"pin_chat_completion_stream — a streaming path that silently "
                f"drops tools is the same class of bug as the one that stayed "
                f"WebSocket-only after the bus was fixed",
            )


class RouteForwardsTools(unittest.TestCase):

    def setUp(self):
        self.source = (Path(__file__).resolve().parents[1]
                       / "api" / "routes" / "pin.py").read_text(encoding="utf-8")

    def test_the_route_hands_tools_to_both_paths(self):
        # A schema field nothing reads is the "it exists and is never called"
        # failure. Both call sites must pass it through.
        self.assertEqual(
            self.source.count("tools=request.tools"), 2,
            "streaming AND non-streaming must forward what the caller described",
        )
        self.assertEqual(self.source.count("tool_choice=request.tool_choice"), 2)


if __name__ == "__main__":
    unittest.main()
