"""Preserve provider-reported token usage without inventing token estimates."""


def normalize_usage(result):
    """Return OpenAI usage and its provenance for response and billing alike.

    Missing/invalid counts retain the legacy zero fallback, but are explicitly
    labelled. Nested token details are kept; native Ollama counts are accepted
    when a provider forwards them instead of an OpenAI usage object.
    """
    raw = result.get("usage")
    usage = dict(raw) if isinstance(raw, dict) else {}
    source = "openai" if usage else "unavailable"

    def valid(value):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    if not usage and any(k in result for k in ("prompt_eval_count", "eval_count")):
        source = "ollama"
        usage = {
            "prompt_tokens": result.get("prompt_eval_count"),
            "completion_tokens": result.get("eval_count"),
        }

    if not usage and isinstance(result.get("usageMetadata"), dict):
        source = "gemini"
        usage = dict(result["usageMetadata"])
        usage["prompt_tokens"] = usage.get("promptTokenCount")
        candidates = usage.get("candidatesTokenCount")
        thoughts = usage.get("thoughtsTokenCount", 0)
        usage["completion_tokens"] = (candidates + thoughts
                                      if valid(candidates) and valid(thoughts) else None)
        usage["total_tokens"] = usage.get("totalTokenCount")
    elif usage and "prompt_tokens" not in usage and "input_tokens" in usage:
        source = "input_output"
        usage["prompt_tokens"] = usage.get("input_tokens")
        usage["completion_tokens"] = usage.get("output_tokens")
        if "input_tokens_details" in usage:
            usage["prompt_tokens_details"] = usage["input_tokens_details"]
        if "output_tokens_details" in usage:
            usage["completion_tokens_details"] = usage["output_tokens_details"]

    complete = all(valid(usage.get(k)) for k in ("prompt_tokens", "completion_tokens"))
    has_count = any(valid(usage.get(k)) for k in
                    ("prompt_tokens", "completion_tokens", "total_tokens"))
    for key in ("prompt_tokens", "completion_tokens"):
        if not valid(usage.get(key)):
            usage[key] = 0
    summed = usage["prompt_tokens"] + usage["completion_tokens"]
    total = usage.get("total_tokens")
    inconsistent = valid(total) and total < summed
    if inconsistent:
        usage["reported_total_tokens"] = total
    if not valid(total) or inconsistent:
        usage["total_tokens"] = summed

    status = "reported" if complete else "partial" if has_count else "unavailable"
    if inconsistent:
        status = "inconsistent"
    elif complete and usage["total_tokens"] == 0:
        status = "reported_zero"
    return usage, {"usage_source": source, "usage_status": status}
