import os
import logging
from typing import Any, Dict, List, Optional

import openai


class OpenAIClient:
    """Simple wrapper for OpenAI calls used by the score pipeline.

    Usage:
        client = OpenAIClient(api_key=os.getenv('OPENAI_API_KEY'))
        resp = client.chat_complete(messages=[{"role":"user","content":"..."}], model='gpt-4o-mini')
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not api_key:
            logging.warning('OpenAI API key not set; cloud provider will fail if used')
        openai.api_key = api_key
        self.timeout = timeout

    def chat_complete(
        self,
        messages: List[Dict[str, str]],
        model: str = 'gpt-4o-mini',
        max_tokens: int = 512,
        temperature: float = 0.0,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Call OpenAI ChatCompletion and return parsed response.

        Returns the raw response dict. Caller is responsible for schema validation.
        """
        try:
            resp = openai.ChatCompletion.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                request_timeout=self.timeout,
                **kwargs,
            )
            return resp
        except Exception as e:
            logging.exception('OpenAI chat request failed: %s', e)
            raise

    def chat_response_text(self, messages: List[Dict[str, str]], **kwargs) -> str:
        resp = self.chat_complete(messages=messages, **kwargs)
        # Safe access: extract first choice text
        try:
            choice = resp.get('choices', [])[0]
            if 'message' in choice:
                return choice['message'].get('content', '')
            return choice.get('text', '')
        except Exception:
            logging.exception('Failed to parse OpenAI response')
            return ''
