"""AWS Bedrock LLM integration for natural-language to SQL pipeline.

Uses boto3 with IAM role credentials (EC2 instance profile).
Supports model families: Amazon Nova, Anthropic Claude, Meta Llama.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional, List, Dict

import boto3

import config
from schema import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

_client = None
_model_family: str = "nova"


def _detect_model_family() -> str:
    """Detect model family from model ID for correct request/response format."""
    mid = config.BEDROCK_MODEL_ID.lower()
    if "nova" in mid:
        return "nova"
    elif "claude" in mid or "anthropic" in mid:
        return "claude"
    elif "llama" in mid or "meta" in mid:
        return "llama"
    return "nova"


def init_client():
    """Initialize the Bedrock Runtime client using IAM role credentials."""
    global _client, _model_family
    _model_family = _detect_model_family()
    _client = boto3.client("bedrock-runtime", region_name=config.AWS_REGION)
    logger.info(
        "Bedrock client initialized via IAM role (region=%s, model=%s, family=%s)",
        config.AWS_REGION, config.BEDROCK_MODEL_ID, _model_family,
    )


# ── Request/Response format adapters ──────────────────────────────────

def _build_request_body(messages: List[Dict], temperature: float, max_tokens: int) -> str:
    """Build the correct request body based on model family."""
    if _model_family == "nova":
        nova_messages = []
        for msg in messages:
            content = msg["content"]
            if isinstance(content, str):
                content = [{"text": content}]
            nova_messages.append({"role": msg["role"], "content": content})

        return json.dumps({
            "system": [{"text": SYSTEM_PROMPT}],
            "messages": nova_messages,
            "inferenceConfig": {
                "maxTokens": max_tokens,
                "temperature": temperature,
            },
        })

    elif _model_family == "claude":
        return json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": SYSTEM_PROMPT,
            "messages": messages,
        })

    else:
        # Default to Nova format
        nova_messages = []
        for msg in messages:
            content = msg["content"]
            if isinstance(content, str):
                content = [{"text": content}]
            nova_messages.append({"role": msg["role"], "content": content})

        return json.dumps({
            "system": [{"text": SYSTEM_PROMPT}],
            "messages": nova_messages,
            "inferenceConfig": {
                "maxTokens": max_tokens,
                "temperature": temperature,
            },
        })


def _extract_text(response_body: dict) -> str:
    """Extract the text response based on model family."""
    if _model_family == "nova":
        return response_body["output"]["message"]["content"][0]["text"]
    elif _model_family == "claude":
        return response_body["content"][0]["text"]
    elif _model_family == "llama":
        return response_body.get("generation", "")
    # Fallback
    if "output" in response_body:
        return response_body["output"]["message"]["content"][0]["text"]
    if "content" in response_body:
        return response_body["content"][0]["text"]
    return str(response_body)


# ── Invocation ────────────────────────────────────────────────────────

def _invoke(messages: List[Dict], temperature: float = 0.1, max_tokens: int = 4096) -> str:
    """Call Bedrock via boto3 using IAM role credentials."""
    if _client is None:
        raise RuntimeError("LLM client not initialized — call init_client() first")

    body = _build_request_body(messages, temperature, max_tokens)

    response = _client.invoke_model(
        modelId=config.BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=body,
    )

    result = json.loads(response["body"].read())
    return _extract_text(result)


# ── Response parsing ──────────────────────────────────────────────────

def _parse_json_response(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code blocks."""
    text = text.strip()

    if text.startswith("{"):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    start = None

    raise ValueError(f"Could not parse JSON from LLM response: {text[:500]}")


# ── Public API ────────────────────────────────────────────────────────

def generate_sql(conversation_history: List[Dict]) -> dict:
    """Interpret the latest question and generate SQL (or conversational response)."""
    raw = _invoke(conversation_history)
    logger.debug("LLM raw (generate_sql): %s", raw[:500])
    return _parse_json_response(raw)


def synthesize_answer(conversation_history: List[Dict], query_result: dict, original_sql: str) -> dict:
    """Produce a natural-language answer and optional chart configuration."""
    columns = query_result["columns"]
    rows = query_result["rows"]

    if len(rows) > 100:
        data_summary = (
            f"Columns: {columns}\n"
            f"First 50 rows:\n{json.dumps(rows[:50])}\n"
            f"... ({query_result['row_count']} total rows, showing first 50) ...\n"
            f"Last 10 rows:\n{json.dumps(rows[-10:])}"
        )
    else:
        data_summary = f"Columns: {columns}\nRows ({len(rows)}):\n{json.dumps(rows)}"

    synthesis_message = {
        "role": "user",
        "content": (
            f"The SQL query was executed successfully.\n\n"
            f"**SQL:** `{original_sql}`\n\n"
            f"**Results:**\n{data_summary}\n\n"
            f"{'(Results were truncated to ' + str(config.MAX_QUERY_ROWS) + ' rows)' if query_result['truncated'] else ''}\n\n"
            f"Now synthesize a clear, insightful answer. Include:\n"
            f"1. A natural-language answer with key numbers and insights\n"
            f"2. An explanation of what the data shows\n"
            f"3. A chart configuration if visualization would help (or null if not)\n\n"
            f"Respond with JSON: {{\"answer\": \"...\", \"explanation\": \"...\", \"chart\": {{...}} or null}}"
        ),
    }

    messages = conversation_history + [synthesis_message]
    raw = _invoke(messages, temperature=0.2)
    logger.debug("LLM raw (synthesize): %s", raw[:500])
    return _parse_json_response(raw)
