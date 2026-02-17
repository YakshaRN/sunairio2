"""AWS Bedrock LLM integration for natural-language to SQL pipeline.

Supports two auth modes:
  1. Bearer token via AWS_BEARER_TOKEN_BEDROCK env var (direct HTTPS)
  2. Standard IAM credentials via boto3 (fallback)
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

_client = None  # boto3 client (used only if no bearer token)
_auth_mode: str = "none"


def init_client():
    """Initialize the Bedrock Runtime client."""
    global _client, _auth_mode

    bearer = os.environ.get("AWS_BEARER_TOKEN_BEDROCK", "").strip()
    if bearer:
        _auth_mode = "bearer"
        logger.info(
            "Bedrock auth: Bearer Token (region=%s, model=%s)",
            config.AWS_REGION,
            config.BEDROCK_MODEL_ID,
        )
        return

    # Fallback to boto3 / IAM credentials
    import boto3

    _client = boto3.client("bedrock-runtime", region_name=config.AWS_REGION)
    _auth_mode = "boto3"
    logger.info(
        "Bedrock auth: boto3 IAM (region=%s, model=%s)",
        config.AWS_REGION,
        config.BEDROCK_MODEL_ID,
    )


# ── Invocation ────────────────────────────────────────────────────────


def _invoke_bearer(messages: List[Dict], temperature: float, max_tokens: int) -> str:
    """Call Bedrock Runtime directly via HTTPS with Bearer Token auth."""
    bearer = os.environ["AWS_BEARER_TOKEN_BEDROCK"].strip()

    model_id_encoded = urllib.parse.quote(config.BEDROCK_MODEL_ID, safe="")
    url = (
        f"https://bedrock-runtime.{config.AWS_REGION}.amazonaws.com"
        f"/model/{model_id_encoded}/invoke"
    )

    payload = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": SYSTEM_PROMPT,
        "messages": messages,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {bearer}")

    ctx = ssl.create_default_context()

    logger.debug("Bearer invoke: POST %s (%d bytes)", url, len(payload))
    with urllib.request.urlopen(req, context=ctx, timeout=180) as resp:
        body = resp.read()
        result = json.loads(body)
        return result["content"][0]["text"]


def _invoke_boto3(messages: List[Dict], temperature: float, max_tokens: int) -> str:
    """Call Bedrock with boto3 (standard IAM auth)."""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": SYSTEM_PROMPT,
        "messages": messages,
    })

    response = _client.invoke_model(
        modelId=config.BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=body,
    )

    result = json.loads(response["body"].read())
    return result["content"][0]["text"]


def _invoke(messages: List[Dict], temperature: float = 0.1, max_tokens: int = 4096) -> str:
    """Route to the correct invocation method based on auth mode."""
    if _auth_mode == "bearer":
        return _invoke_bearer(messages, temperature, max_tokens)
    elif _auth_mode == "boto3":
        return _invoke_boto3(messages, temperature, max_tokens)
    else:
        raise RuntimeError("LLM client not initialized — call init_client() first")


# ── Response parsing ──────────────────────────────────────────────────


def _parse_json_response(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code blocks."""
    text = text.strip()

    # Try direct parse
    if text.startswith("{"):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    # Extract from ```json ... ``` block
    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # Last resort: find outermost { ... }
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
    """
    Given conversation history, ask the LLM to interpret the latest question
    and generate SQL (or a conversational response).
    """
    raw = _invoke(conversation_history)
    logger.debug("LLM raw (generate_sql): %s", raw[:500])
    return _parse_json_response(raw)


def synthesize_answer(conversation_history: List[Dict], query_result: dict, original_sql: str) -> dict:
    """
    Given query results, produce a natural-language answer
    and optional chart configuration.
    """
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
