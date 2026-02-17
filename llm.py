"""AWS Bedrock LLM integration for natural-language to SQL pipeline.

Supports three auth modes (checked in order):
  1. BEDROCK_API_KEY env var — direct HTTPS with Bearer token (simplest)
  2. AWS_BEARER_TOKEN_BEDROCK env var — presigned-URL bearer token
  3. Standard IAM credentials via boto3 (fallback)

Supports model families:
  - Amazon Nova (us.amazon.nova-*) — default, no marketplace subscription needed
  - Anthropic Claude (us.anthropic.claude-*) — requires marketplace subscription
"""

from __future__ import annotations

import json
import logging
import os
import re
import ssl
import urllib.parse
import urllib.request
from typing import Any, Optional, List, Dict

import config
from schema import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

_client = None
_auth_mode: str = "none"
_model_family: str = "nova"  # "nova", "claude", or "other"


def _detect_model_family() -> str:
    """Detect model family from model ID for correct request/response format."""
    mid = config.BEDROCK_MODEL_ID.lower()
    if "nova" in mid:
        return "nova"
    elif "claude" in mid or "anthropic" in mid:
        return "claude"
    elif "llama" in mid or "meta" in mid:
        return "llama"
    elif "mistral" in mid:
        return "mistral"
    return "nova"


def init_client():
    """Initialize the Bedrock Runtime client."""
    global _client, _auth_mode, _model_family

    _model_family = _detect_model_family()

    if config.BEDROCK_API_KEY:
        _auth_mode = "api_key"
        logger.info(
            "Bedrock auth: API Key (region=%s, model=%s, family=%s)",
            config.AWS_REGION, config.BEDROCK_MODEL_ID, _model_family,
        )
        return

    bearer = os.environ.get("AWS_BEARER_TOKEN_BEDROCK", "").strip()
    if bearer:
        _auth_mode = "bearer"
        logger.info(
            "Bedrock auth: Bearer Token (region=%s, model=%s, family=%s)",
            config.AWS_REGION, config.BEDROCK_MODEL_ID, _model_family,
        )
        return

    import boto3
    _client = boto3.client("bedrock-runtime", region_name=config.AWS_REGION)
    _auth_mode = "boto3"
    logger.info(
        "Bedrock auth: boto3 IAM (region=%s, model=%s, family=%s)",
        config.AWS_REGION, config.BEDROCK_MODEL_ID, _model_family,
    )


# ── Request/Response format adapters ──────────────────────────────────

def _build_request_body(messages: List[Dict], temperature: float, max_tokens: int) -> bytes:
    """Build the correct request body based on model family."""
    if _model_family == "nova":
        # Nova uses Converse-style: system as list of text objects, messages with content as list
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
        }).encode("utf-8")

    elif _model_family == "claude":
        return json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": SYSTEM_PROMPT,
            "messages": messages,
        }).encode("utf-8")

    else:
        # Generic fallback — Nova format
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
        }).encode("utf-8")


def _extract_text(response_body: dict) -> str:
    """Extract the text response based on model family."""
    if _model_family == "nova":
        return response_body["output"]["message"]["content"][0]["text"]
    elif _model_family == "claude":
        return response_body["content"][0]["text"]
    elif _model_family == "llama":
        return response_body.get("generation", "")
    else:
        # Try common paths
        if "output" in response_body:
            return response_body["output"]["message"]["content"][0]["text"]
        if "content" in response_body:
            return response_body["content"][0]["text"]
        return str(response_body)


# ── Invocation backends ───────────────────────────────────────────────

def _build_bedrock_url() -> str:
    model_enc = urllib.parse.quote(config.BEDROCK_MODEL_ID, safe="")
    return (
        f"https://bedrock-runtime.{config.AWS_REGION}.amazonaws.com"
        f"/model/{model_enc}/invoke"
    )


def _invoke_api_key(messages: List[Dict], temperature: float, max_tokens: int) -> str:
    """Call Bedrock Runtime with API Key bearer auth."""
    url = _build_bedrock_url()
    payload = _build_request_body(messages, temperature, max_tokens)

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {config.BEDROCK_API_KEY}")

    ctx = ssl.create_default_context()
    logger.debug("API-Key invoke: POST %s (%d bytes)", url, len(payload))

    with urllib.request.urlopen(req, context=ctx, timeout=180) as resp:
        result = json.loads(resp.read())
        return _extract_text(result)


def _invoke_bearer(messages: List[Dict], temperature: float, max_tokens: int) -> str:
    """Call Bedrock Runtime with presigned-URL bearer token."""
    bearer = os.environ["AWS_BEARER_TOKEN_BEDROCK"].strip()
    url = _build_bedrock_url()
    payload = _build_request_body(messages, temperature, max_tokens)

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {bearer}")

    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=180) as resp:
        return _extract_text(json.loads(resp.read()))


def _invoke_boto3(messages: List[Dict], temperature: float, max_tokens: int) -> str:
    """Call Bedrock with boto3 (standard IAM auth)."""
    payload = _build_request_body(messages, temperature, max_tokens)

    response = _client.invoke_model(
        modelId=config.BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=payload,
    )

    result = json.loads(response["body"].read())
    return _extract_text(result)


def _invoke(messages: List[Dict], temperature: float = 0.1, max_tokens: int = 4096) -> str:
    """Route to the correct invocation method based on auth mode."""
    if _auth_mode == "api_key":
        return _invoke_api_key(messages, temperature, max_tokens)
    elif _auth_mode == "bearer":
        return _invoke_bearer(messages, temperature, max_tokens)
    elif _auth_mode == "boto3":
        return _invoke_boto3(messages, temperature, max_tokens)
    else:
        raise RuntimeError("LLM client not initialized — call init_client() first")


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
