from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel


class ProviderType(str, Enum):
    # Must stay in sync with api/models/schemas.py:ProviderType, which is what
    # validates incoming requests. They had drifted in BOTH directions -- PIN
    # here but not there (so PIN was usable and unsaveable), TAVILY there but
    # not here (so a saved tavily session would KeyError inside get_provider).
    # tests/test_provider_type_parity.py now fails on either kind of gap.
    PIN = "pin"
    GROQ = "groq"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GEMINI = "gemini"
    MISTRAL = "mistral"
    XAI = "xai"
    TOGETHER = "together"
    OPENROUTER = "openrouter"
    DEEPSEEK = "deepseek"
    FIREWORKS = "fireworks"
    PERPLEXITY = "perplexity"
    # Search, not chat. Present so the enum round-trips every value the API
    # accepts; `supports_chat=False` keeps it out of the completion path.
    TAVILY = "tavily"


class ProviderModel(BaseModel):
    id: str
    name: str
    context_window: int
    max_output: int
    reasoning_efforts: Optional[List[str]] = None
    default_reasoning_effort: Optional[str] = None


class ProviderConfig(BaseModel):
    id: ProviderType
    name: str
    base_url: str
    auth_header: str
    auth_prefix: str
    default_models: List[ProviderModel]
    requires_version_header: bool = False
    version_header: Optional[str] = None
    supports_models_endpoint: bool = False
    timeout: float = 60.0
    docs_url: str
    console_url: str
    is_credit_based: bool = False
    tagline: Optional[str] = None
    # False for providers that exist for something other than chat (search,
    # embeddings). The router refuses them with a readable message instead of
    # a KeyError on the adapter table.
    supports_chat: bool = True


PROVIDERS: Dict[ProviderType, ProviderConfig] = {
    ProviderType.PIN: ProviderConfig(
        id=ProviderType.PIN,
        name="PIN Network",
        base_url="/v1/pin",
        auth_header="",
        auth_prefix="",
        supports_models_endpoint=True,
        timeout=90.0,
        docs_url="/pin/operator",
        console_url="/pin/join",
        is_credit_based=True,
        tagline="No Setup Required - Support Local LLM",
        # DELIBERATELY EMPTY. PIN has no vendor catalogue to hardcode.
        #
        # Every other provider here lists models its VENDOR defines: OpenAI
        # decides what OpenAI serves, the list moves slowly, and a static copy
        # is defensible. PIN's models are whatever ITS OPERATORS registered
        # minutes ago, so a static list cannot be right for it — not stale,
        # wrong by construction. The live set comes from
        # `storage.pin_list_available_models()`, derived from the node rows of
        # online operators.
        #
        # What was here until now: pin:auto, pin:llama3.3:70b, pin:llama3.2:3b,
        # pin:qwen2.5:72b, pin:deepseek-r1:70b, pin:mistral:7b, pin:phi3:14b —
        # while the network was actually serving GLM-4-32B, llama-3.1-8b,
        # qwen-3-8b and tts:chatterbox-turbo. All seven were UNROUTABLE, and
        # not merely out of date: `resolve_pin_model` matches against the live
        # pool of BARE ids and never strips a `pin:` prefix, so `pin:mistral:7b`
        # could not resolve even while an operator served mistral.
        #
        # `pin:auto` was the worst of them. `get_provider_for_model` sends any
        # `pin:*` id to this provider, `choose_pin_model` then fails to resolve
        # it, and the friendliest label in the menu — "Auto (Best Available)" —
        # raised "No operators available for model: pin:auto" every time. There
        # is no `auto` handling anywhere in the codebase. It is not reinstated
        # here on purpose: `choose_pin_model` already picks when the pool holds
        # exactly one model and REFUSES to choose among several, because a
        # silent model substitution is the thing that module exists to prevent.
        # Shipping the label without the behaviour is what produced this bug.
        #
        # Six callers read `default_models[0].id` as a fallback; all six guard
        # with `if pconfig.default_models:`, so an empty list makes them fall
        # through to their real fallback instead of selecting an id that always
        # fails. Verified before emptying this.
        default_models=[],
    ),
    ProviderType.GROQ: ProviderConfig(
        id=ProviderType.GROQ,
        name="Groq",
        base_url="https://api.groq.com/openai/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=30.0,
        docs_url="https://console.groq.com/docs",
        console_url="https://console.groq.com/keys",
        default_models=[
            ProviderModel(
                id="groq/compound",
                name="GroqCompound",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="groq/compound-mini",
                name="GroqCompound Mini",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="llama-3.1-8b-instant",
                name="Llama 3.1 8B",
                context_window=131072,
                max_output=131072,
            ),
            ProviderModel(
                id="llama-3.3-70b-versatile",
                name="Llama 3.3 70B",
                context_window=131072,
                max_output=32768,
            ),
            ProviderModel(
                id="meta-llama/llama-4-scout-17b-16e-instruct",
                name="Llama 4 Scout",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/llama-guard-4-12b",
                name="Llama Guard 4 12B",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/llama-prompt-guard-2-22m",
                name="Llama Prompt Guard 2 22M",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/llama-prompt-guard-2-86m",
                name="Llama Prompt Guard 2 86M",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="openai/gpt-oss-120b",
                name="GPT OSS 120B",
                context_window=131072,
                max_output=65536,
            ),
            ProviderModel(
                id="openai/gpt-oss-20b",
                name="GPT OSS 20B",
                context_window=131072,
                max_output=65536,
            ),
            ProviderModel(
                id="openai/gpt-oss-safeguard-20b",
                name="GPT OSS Safeguard 20B",
                context_window=131072,
                max_output=65536,
            ),
            ProviderModel(
                id="qwen/qwen3-32b",
                name="Qwen3 32B",
                context_window=131072,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.OPENAI: ProviderConfig(
        id=ProviderType.OPENAI,
        name="OpenAI",
        base_url="https://api.openai.com/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=300.0,
        docs_url="https://platform.openai.com/docs",
        console_url="https://platform.openai.com/api-keys",
        default_models=[
            ProviderModel(
                id="gpt-5.6-sol",
                name="GPT-5.6 Sol",
                context_window=1050000,
                max_output=128000,
                reasoning_efforts=["light", "medium", "high", "xhigh"],
                default_reasoning_effort="medium",
            ),
            ProviderModel(
                id="gpt-5.5", name="GPT-5.5", context_window=1050000, max_output=128000
            ),
            ProviderModel(
                id="gpt-5.5-pro",
                name="GPT-5.5 Pro",
                context_window=1050000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5.4", name="GPT-5.4", context_window=1050000, max_output=128000
            ),
            ProviderModel(
                id="gpt-5.4-mini",
                name="GPT-5.4 Mini",
                context_window=400000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5.4-nano",
                name="GPT-5.4 Nano",
                context_window=400000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5.4-2026-03-05",
                name="GPT-5.4 (03-05)",
                context_window=1050000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5.3-chat-latest",
                name="GPT-5.3 Chat Latest (instant)",
                context_window=128000,
                max_output=16384,
            ),
            ProviderModel(
                id="gpt-5.4-pro",
                name="GPT-5.4 Pro",
                context_window=1050000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5.3-codex",
                name="GPT-5.3 Codex",
                context_window=400000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5-mini",
                name="GPT-5 Mini",
                context_window=400000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-5.2", name="GPT-5.2", context_window=200000, max_output=128000
            ),
            ProviderModel(
                id="gpt-5.2-pro",
                name="GPT-5.2 Pro",
                context_window=200000,
                max_output=32768,
            ),
            ProviderModel(
                id="gpt-4.1", name="GPT-4.1", context_window=1000000, max_output=32768
            ),
            ProviderModel(
                id="gpt-3.5-turbo",
                name="GPT-3.5 Turbo",
                context_window=1050000,
                max_output=128000,
            ),
            ProviderModel(
                id="gpt-4.1-mini",
                name="GPT-4.1 Mini",
                context_window=1000000,
                max_output=32768,
            ),
            ProviderModel(
                id="gpt-4.1-nano",
                name="GPT-4.1 Nano",
                context_window=1000000,
                max_output=32768,
            ),
            ProviderModel(
                id="gpt-4o", name="GPT-4o", context_window=128000, max_output=16384
            ),
            ProviderModel(
                id="gpt-4o-mini",
                name="GPT-4o Mini",
                context_window=128000,
                max_output=16384,
            ),
            ProviderModel(id="o3", name="o3", context_window=200000, max_output=100000),
            ProviderModel(
                id="o3-pro", name="o3 Pro", context_window=200000, max_output=100000
            ),
            ProviderModel(
                id="o4-mini", name="o4 Mini", context_window=128000, max_output=65536
            ),
        ],
    ),
    ProviderType.ANTHROPIC: ProviderConfig(
        id=ProviderType.ANTHROPIC,
        name="Anthropic",
        base_url="https://api.anthropic.com/v1",
        auth_header="x-api-key",
        auth_prefix="",
        requires_version_header=True,
        version_header="2023-06-01",
        timeout=90.0,
        docs_url="https://docs.anthropic.com",
        console_url="https://console.anthropic.com/settings/keys",
        default_models=[
            ProviderModel(
                id="claude-fable-5",
                name="Claude Fable 5",
                context_window=1000000,
                max_output=128000,
                # Extended thinking (api/providers/adapters/anthropic.py's
                # _NO_SAMPLING_PREFIXES generation) -- same tier vocabulary
                # as GPT Sol's reasoning_effort, translated server-side into
                # a real thinking.budget_tokens, not just passed through.
                reasoning_efforts=["light", "medium", "high", "xhigh"],
                default_reasoning_effort="medium",
            ),
            ProviderModel(
                id="claude-sonnet-5",
                name="Claude Sonnet 5",
                context_window=1000000,
                max_output=128000,
                reasoning_efforts=["light", "medium", "high", "xhigh"],
                default_reasoning_effort="medium",
            ),
            ProviderModel(
                id="claude-opus-4-8",
                name="Claude Opus 4.8",
                context_window=1000000,
                max_output=128000,
                reasoning_efforts=["light", "medium", "high", "xhigh"],
                default_reasoning_effort="medium",
            ),
            ProviderModel(
                id="claude-opus-4-7",
                name="Claude Opus 4.7",
                context_window=1000000,
                max_output=128000,
                reasoning_efforts=["light", "medium", "high", "xhigh"],
                default_reasoning_effort="medium",
            ),
            ProviderModel(
                id="claude-opus-4-6",
                name="Claude Opus 4.6",
                context_window=1000000,
                max_output=128000,
            ),
            ProviderModel(
                id="claude-sonnet-4-6",
                name="Claude Sonnet 4.6",
                context_window=1000000,
                max_output=128000,
            ),
            ProviderModel(
                id="claude-opus-4-5-20251101",
                name="Claude Opus 4.5",
                context_window=200000,
                max_output=64000,
            ),
            ProviderModel(
                id="claude-sonnet-4-5-20250929",
                name="Claude Sonnet 4.5",
                context_window=1000000,
                max_output=64000,
            ),
            ProviderModel(
                id="claude-haiku-4-5-20251001",
                name="Claude Haiku 4.5",
                context_window=200000,
                max_output=64000,
            ),
            ProviderModel(
                id="claude-opus-4-1-20250805",
                name="Claude Opus 4.1",
                context_window=200000,
                max_output=32000,
            ),
            ProviderModel(
                id="claude-opus-4-20250514",
                name="Claude Opus 4",
                context_window=200000,
                max_output=32000,
            ),
            ProviderModel(
                id="claude-sonnet-4-20250514",
                name="Claude Sonnet 4",
                context_window=1000000,
                max_output=64000,
            ),
            ProviderModel(
                id="claude-3-haiku-20240307",
                name="Claude 3 Haiku",
                context_window=200000,
                max_output=4096,
            ),
        ],
    ),
    ProviderType.GEMINI: ProviderConfig(
        id=ProviderType.GEMINI,
        name="Google Gemini",
        base_url="https://generativelanguage.googleapis.com/v1beta",
        auth_header="x-goog-api-key",
        auth_prefix="",
        timeout=60.0,
        docs_url="https://ai.google.dev/docs",
        console_url="https://aistudio.google.com/app/apikey",
        default_models=[
            ProviderModel(
                id="gemini-3.1-pro-preview",
                name="Gemini 3.1 Pro Preview",
                context_window=2000000,
                max_output=65536,
            ),
            ProviderModel(
                id="gemini-3-flash-preview",
                name="Gemini 3 Flash Preview",
                context_window=1000000,
                max_output=65536,
            ),
            ProviderModel(
                id="gemini-3.1-flash-lite-preview",
                name="Gemini 3.1 Flash-Lite Preview",
                context_window=1000000,
                max_output=65536,
            ),
            ProviderModel(
                id="gemini-2.5-flash",
                name="Gemini 2.5 Flash",
                context_window=1000000,
                max_output=65536,
            ),
            ProviderModel(
                id="gemini-2.5-pro",
                name="Gemini 2.5 Pro",
                context_window=2000000,
                max_output=65536,
            ),
            ProviderModel(
                id="gemini-2.0-flash",
                name="Gemini 2.0 Flash",
                context_window=1000000,
                max_output=8192,
            ),
            ProviderModel(
                id="gemini-2.0-flash-lite",
                name="Gemini 2.0 Flash-Lite",
                context_window=1000000,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.MISTRAL: ProviderConfig(
        id=ProviderType.MISTRAL,
        name="Mistral AI",
        base_url="https://api.mistral.ai/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=60.0,
        docs_url="https://docs.mistral.ai",
        console_url="https://console.mistral.ai/api-keys",
        default_models=[
            ProviderModel(
                id="mistral-large-latest",
                name="Mistral Large 3",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="codestral-latest",
                name="Codestral",
                context_window=256000,
                max_output=8192,
            ),
            ProviderModel(
                id="mistral-small-latest",
                name="Mistral Small 3.2",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="pixtral-large-latest",
                name="Pixtral Large",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="pixtral-12b-2409",
                name="Pixtral 12B",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="ministral-8b-latest",
                name="Ministral 8B",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="ministral-3b-latest",
                name="Ministral 3B",
                context_window=128000,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.XAI: ProviderConfig(
        id=ProviderType.XAI,
        name="xAI (Grok)",
        base_url="https://api.x.ai/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=60.0,
        docs_url="https://docs.x.ai",
        console_url="https://console.x.ai",
        default_models=[
            ProviderModel(
                id="grok-4.1", name="Grok 4.1", context_window=256000, max_output=8192
            ),
            ProviderModel(
                id="grok-4.1-fast",
                name="Grok 4.1 Fast",
                context_window=2000000,
                max_output=8192,
            ),
            ProviderModel(
                id="grok-4", name="Grok 4", context_window=256000, max_output=8192
            ),
            ProviderModel(
                id="grok-4-fast",
                name="Grok 4 Fast",
                context_window=2000000,
                max_output=8192,
            ),
            ProviderModel(
                id="grok-3", name="Grok 3", context_window=131072, max_output=8192
            ),
            ProviderModel(
                id="grok-3-mini",
                name="Grok 3 Mini",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="grok-2-vision-1212",
                name="Grok 2 Vision",
                context_window=32768,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.TOGETHER: ProviderConfig(
        id=ProviderType.TOGETHER,
        name="Together AI",
        base_url="https://api.together.xyz/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=60.0,
        docs_url="https://docs.together.ai",
        console_url="https://api.together.xyz/settings/api-keys",
        default_models=[
            ProviderModel(
                id="meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",
                name="Llama 4 Maverick",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/Llama-4-Scout-17B-16E-Instruct",
                name="Llama 4 Scout",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/Llama-3.3-70B-Instruct-Turbo",
                name="Llama 3.3 70B Turbo",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="deepseek-ai/DeepSeek-R1",
                name="DeepSeek R1",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="deepseek-ai/DeepSeek-V3",
                name="DeepSeek V3",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="Qwen/Qwen2.5-72B-Instruct-Turbo",
                name="Qwen 2.5 72B Turbo",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="Qwen/Qwen3-235B-A22B-fp8-tput",
                name="Qwen 3 235B",
                context_window=262144,
                max_output=8192,
            ),
            ProviderModel(
                id="deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
                name="DeepSeek R1 Distill 70B",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/Meta-Llama-3.1-405B-Instruct-Turbo",
                name="Llama 3.1 405B Turbo",
                context_window=131072,
                max_output=4096,
            ),
        ],
    ),
    ProviderType.OPENROUTER: ProviderConfig(
        id=ProviderType.OPENROUTER,
        name="OpenRouter",
        base_url="https://openrouter.ai/api/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=90.0,
        docs_url="https://openrouter.ai/docs",
        console_url="https://openrouter.ai/keys",
        default_models=[
            ProviderModel(
                id="anthropic/claude-opus-4-6",
                name="Claude Opus 4.6 (via OpenRouter)",
                context_window=200000,
                max_output=128000,
            ),
            ProviderModel(
                id="anthropic/claude-sonnet-4-6",
                name="Claude Sonnet 4.6 (via OpenRouter)",
                context_window=200000,
                max_output=64000,
            ),
            ProviderModel(
                id="anthropic/claude-opus-4-5",
                name="Claude Opus 4.5 (via OpenRouter)",
                context_window=200000,
                max_output=8192,
            ),
            ProviderModel(
                id="anthropic/claude-sonnet-4-5",
                name="Claude Sonnet 4.5 (via OpenRouter)",
                context_window=200000,
                max_output=8192,
            ),
            ProviderModel(
                id="openai/gpt-5.4",
                name="GPT-5.4 (via OpenRouter)",
                context_window=1050000,
                max_output=128000,
            ),
            ProviderModel(
                id="openai/gpt-5.3-codex",
                name="GPT-5.3 Codex (via OpenRouter)",
                context_window=400000,
                max_output=128000,
            ),
            ProviderModel(
                id="openai/gpt-5.2",
                name="GPT-5.2 (via OpenRouter)",
                context_window=200000,
                max_output=32768,
            ),
            ProviderModel(
                id="openai/gpt-4.1",
                name="GPT-4.1 (via OpenRouter)",
                context_window=1000000,
                max_output=32768,
            ),
            ProviderModel(
                id="openai/gpt-4o",
                name="GPT-4o (via OpenRouter)",
                context_window=128000,
                max_output=16384,
            ),
            ProviderModel(
                id="google/gemini-3.1-pro-preview",
                name="Gemini 3.1 Pro (via OpenRouter)",
                context_window=2000000,
                max_output=65536,
            ),
            ProviderModel(
                id="google/gemini-3-flash-preview",
                name="Gemini 3 Flash (via OpenRouter)",
                context_window=1000000,
                max_output=65536,
            ),
            ProviderModel(
                id="google/gemini-2.5-flash",
                name="Gemini 2.5 Flash (via OpenRouter)",
                context_window=1000000,
                max_output=65536,
            ),
            ProviderModel(
                id="deepseek/deepseek-chat",
                name="DeepSeek V3 (via OpenRouter)",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="deepseek/deepseek-reasoner",
                name="DeepSeek R1 (via OpenRouter)",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/llama-4-maverick",
                name="Llama 4 Maverick (via OpenRouter)",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="meta-llama/llama-3.3-70b-instruct",
                name="Llama 3.3 70B (via OpenRouter)",
                context_window=131072,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.DEEPSEEK: ProviderConfig(
        id=ProviderType.DEEPSEEK,
        name="DeepSeek",
        base_url="https://api.deepseek.com",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=90.0,
        docs_url="https://api-docs.deepseek.com",
        console_url="https://platform.deepseek.com/api_keys",
        default_models=[
            ProviderModel(
                id="deepseek-chat",
                name="DeepSeek Chat (V3)",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="deepseek-reasoner",
                name="DeepSeek Reasoner (R1)",
                context_window=128000,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.FIREWORKS: ProviderConfig(
        id=ProviderType.FIREWORKS,
        name="Fireworks AI",
        base_url="https://api.fireworks.ai/inference/v1",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=True,
        timeout=60.0,
        docs_url="https://docs.fireworks.ai",
        console_url="https://fireworks.ai/api-keys",
        default_models=[
            ProviderModel(
                id="accounts/fireworks/models/llama4-maverick-instruct-basic",
                name="Llama 4 Maverick",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="accounts/fireworks/models/llama4-scout-instruct-basic",
                name="Llama 4 Scout",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="accounts/fireworks/models/llama-v3p3-70b-instruct",
                name="Llama 3.3 70B",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="accounts/fireworks/models/deepseek-v3",
                name="DeepSeek V3",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="accounts/fireworks/models/deepseek-r1",
                name="DeepSeek R1",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="accounts/fireworks/models/qwen2p5-72b-instruct",
                name="Qwen 2.5 72B",
                context_window=131072,
                max_output=8192,
            ),
            ProviderModel(
                id="accounts/fireworks/models/qwen3-235b-a22b",
                name="Qwen 3 235B",
                context_window=131072,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.PERPLEXITY: ProviderConfig(
        id=ProviderType.PERPLEXITY,
        name="Perplexity",
        base_url="https://api.perplexity.ai",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=False,
        timeout=90.0,
        docs_url="https://docs.perplexity.ai",
        console_url="https://www.perplexity.ai/settings/api",
        default_models=[
            ProviderModel(
                id="sonar-pro",
                name="Sonar Pro (Search)",
                context_window=200000,
                max_output=8192,
            ),
            ProviderModel(
                id="sonar",
                name="Sonar (Search)",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="sonar-reasoning-pro",
                name="Sonar Reasoning Pro",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="sonar-reasoning",
                name="Sonar Reasoning",
                context_window=128000,
                max_output=8192,
            ),
            ProviderModel(
                id="sonar-deep-research",
                name="Sonar Deep Research",
                context_window=128000,
                max_output=8192,
            ),
        ],
    ),
    ProviderType.TAVILY: ProviderConfig(
        id=ProviderType.TAVILY,
        name="Tavily",
        base_url="https://api.tavily.com",
        auth_header="Authorization",
        auth_prefix="Bearer ",
        supports_models_endpoint=False,
        timeout=30.0,
        docs_url="https://docs.tavily.com",
        console_url="https://app.tavily.com",
        tagline="Web search for grounding",
        # Search provider. Selectable and storable like any other, but never
        # routed a completion -- see supports_chat on ProviderConfig.
        supports_chat=False,
        default_models=[],
    ),
}


def get_provider(provider_type: ProviderType) -> ProviderConfig:
    return PROVIDERS[provider_type]


def get_chat_providers() -> List[ProviderConfig]:
    """Providers that can actually serve a completion."""
    return [p for p in PROVIDERS.values() if p.supports_chat]


def get_all_providers() -> List[ProviderConfig]:
    return list(PROVIDERS.values())


def _is_local_tagged_id(model_id: str) -> bool:
    """Is this an Ollama/hearth-style `name:tag` id rather than a cloud id?

    NO CLOUD VENDOR USES A COLON TAG. Ollama-family runtimes do, and PIN
    operators register whatever their runtime reports — `muse-local:latest`,
    `deepseek-r1:32b`, `glm-4.7:cloud`.

    Without this, the vendor-prefix ladder below claimed such ids by their
    leading characters alone: `deepseek-r1:32b` matched
    `startswith("deepseek-")` and was routed to the DeepSeek CLOUD API, which
    has never heard of it. The caller got "model not found" while the PIN
    operator sat online with that exact model loaded and serving, and PIN was
    never consulted at all. Both statements were true, which is what made it
    expensive to read.

    OpenRouter ids also carry a colon (`meta-llama/llama-3-8b:free`) but always
    carry a slash too, so the slash is the discriminator — matching on the
    colon alone would have broken every OpenRouter route.

    Returns a bool only; resolution stays the caller's job. An unclaimed id
    falls through to None exactly as `muse-local:latest` already did, so this
    narrows a wrong answer without inventing a new one.
    """
    return ":" in model_id and "/" not in model_id


def get_provider_for_model(model_id: str) -> Optional[ProviderType]:
    if model_id.startswith("pin:"):
        return ProviderType.PIN
    # A local `name:tag` id is never a cloud model. Bail before the
    # vendor-prefix ladder can claim it on a leading-substring coincidence.
    if _is_local_tagged_id(model_id):
        return None
    if (
        model_id.startswith("groq/")
        or model_id.startswith("openai/gpt-oss-")
        or model_id.startswith("moonshotai/")
        or model_id.startswith("qwen/")
    ):
        return ProviderType.GROQ
    if (
        model_id.startswith("gpt-")
        or model_id.startswith("o1")
        or model_id.startswith("o3")
        or model_id.startswith("o4")
    ):
        return ProviderType.OPENAI
    if model_id.startswith("claude-"):
        return ProviderType.ANTHROPIC
    if model_id.startswith("gemini-"):
        return ProviderType.GEMINI
    if (
        model_id.startswith("mistral-")
        or model_id.startswith("codestral")
        or model_id.startswith("pixtral")
        or model_id.startswith("ministral")
    ):
        return ProviderType.MISTRAL
    if (
        model_id.startswith("llama-")
        or model_id.startswith("mixtral-")
        or model_id.startswith("qwen-")
    ):
        return ProviderType.GROQ
    if model_id.startswith("grok-"):
        return ProviderType.XAI
    if model_id.startswith("deepseek-"):
        return ProviderType.DEEPSEEK
    if model_id.startswith("sonar"):
        return ProviderType.PERPLEXITY
    if "meta-llama/" in model_id or "Qwen/" in model_id or "deepseek-ai/" in model_id:
        return ProviderType.TOGETHER
    if "accounts/fireworks/" in model_id:
        return ProviderType.FIREWORKS
    if "/" in model_id and model_id.count("/") == 1:
        return ProviderType.OPENROUTER
    return None
