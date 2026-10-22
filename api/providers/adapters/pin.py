from typing import Any, AsyncIterator, Dict, List


class PinAdapter:
    
    def __init__(self):
        self.timeout = 90.0
    
    async def complete(
        self,
        api_key: str,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 1024,
        **kwargs
    ) -> Dict[str, Any]:
        from api.services.pin_service import pin_chat_completion
        
        user_id = api_key
        
        max_price = kwargs.get("max_price")
        
        return await pin_chat_completion(
            user_id=user_id,
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            max_price=max_price
        )
    
    async def stream(
        self,
        *,
        api_key: str,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 1024,
        **kwargs,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Token-by-token delivery from the operator's node.

        Without this method ProviderRouter.route_completion_stream takes its
        `if not hasattr(adapter, 'stream')` fallback: it awaits complete() for
        the entire generation and then yields ONE chunk containing the whole
        answer. The playground was streaming correctly the whole time -- PIN
        was the only provider with nothing to stream.
        """
        from api.services.pin_service import pin_chat_completion_stream

        async for chunk in pin_chat_completion_stream(
            user_id=api_key,          # PIN bills the user; the "key" IS the user id
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            max_price=kwargs.get("max_price"),
        ):
            yield chunk

    def normalize_error(self, error: Exception, status_code: int = 500) -> tuple[str, str]:
        error_msg = str(error)
        
        if "insufficient" in error_msg.lower() or "credits" in error_msg.lower():
            return "insufficient_credits", "Insufficient PIN credits. Please top up your balance."
        if "no operators" in error_msg.lower():
            return "no_operators", "No operators available on the PIN network. Please try again later."
        if "timeout" in error_msg.lower():
            return "timeout", "Request timed out. The operator may be under heavy load."
        
        return "pin_error", f"PIN network error: {error_msg}"
