"""AWS Bedrock LLM integration for natural-language to SQL pipeline.

Uses boto3 with IAM role credentials (EC2 instance profile).
Supports model families: Amazon Nova, Anthropic Claude, Meta Llama.
Uses a faster model for synthesis to reduce latency.
"""

from __future__ import annotations

import json
import logging
import re
import statistics
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional, List, Dict

import boto3


class _SafeEncoder(json.JSONEncoder):
    """Handle Decimal, date, and datetime values from PostgreSQL results."""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        return super().default(o)

import config
from schema import get_system_prompt

logger = logging.getLogger(__name__)

_client = None
_model_family: str = "nova"
_synth_model_family: str = "claude"


def _detect_family(model_id: str) -> str:
    mid = model_id.lower()
    if "nova" in mid:
        return "nova"
    elif "claude" in mid or "anthropic" in mid:
        return "claude"
    elif "llama" in mid or "meta" in mid:
        return "llama"
    return "nova"


def init_client():
    """Initialize the Bedrock Runtime client using IAM role credentials."""
    global _client, _model_family, _synth_model_family
    _model_family = _detect_family(config.BEDROCK_MODEL_ID)
    _synth_model_family = _detect_family(config.BEDROCK_SYNTH_MODEL_ID)
    _client = boto3.client("bedrock-runtime", region_name=config.AWS_REGION)
    logger.info(
        "Bedrock client initialized (region=%s, gen_model=%s [%s], synth_model=%s [%s])",
        config.AWS_REGION,
        config.BEDROCK_MODEL_ID, _model_family,
        config.BEDROCK_SYNTH_MODEL_ID, _synth_model_family,
    )


# ── Request/Response format adapters ──────────────────────────────────

def _build_request_body(messages: List[Dict], temperature: float, max_tokens: int,
                        family: str = None, system_prompt: str = None) -> str:
    """Build the correct request body based on model family."""
    family = family or _model_family
    sys_prompt = system_prompt or get_system_prompt()

    if family == "claude":
        return json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": sys_prompt,
            "messages": messages,
        })
    else:
        nova_messages = []
        for msg in messages:
            content = msg["content"]
            if isinstance(content, str):
                content = [{"text": content}]
            nova_messages.append({"role": msg["role"], "content": content})
        return json.dumps({
            "system": [{"text": sys_prompt}],
            "messages": nova_messages,
            "inferenceConfig": {
                "maxTokens": max_tokens,
                "temperature": temperature,
            },
        })


def _extract_text(response_body: dict, family: str = None) -> str:
    """Extract the text response based on model family."""
    family = family or _model_family
    if family == "claude":
        return response_body["content"][0]["text"]
    elif family == "nova":
        return response_body["output"]["message"]["content"][0]["text"]
    elif family == "llama":
        return response_body.get("generation", "")
    if "content" in response_body:
        return response_body["content"][0]["text"]
    if "output" in response_body:
        return response_body["output"]["message"]["content"][0]["text"]
    return str(response_body)


# ── Invocation ────────────────────────────────────────────────────────

def _invoke(messages: List[Dict], temperature: float = 0.1, max_tokens: int = 6144,
            model_id: str = None, family: str = None, system_prompt: str = None) -> str:
    """Call Bedrock. Defaults to the primary (SQL generation) model."""
    if _client is None:
        raise RuntimeError("LLM client not initialized — call init_client() first")

    mid = model_id or config.BEDROCK_MODEL_ID
    fam = family or _model_family

    body = _build_request_body(messages, temperature, max_tokens,
                               family=fam, system_prompt=system_prompt)

    response = _client.invoke_model(
        modelId=mid,
        contentType="application/json",
        accept="application/json",
        body=body,
    )

    result = json.loads(response["body"].read())
    return _extract_text(result, family=fam)


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

    # Last resort: try to salvage truncated JSON (LLM hit max_tokens mid-response)
    if start is not None:
        partial = text[start:]
        # Try closing open strings and braces
        if partial.count('"') % 2 == 1:
            partial += '"'
        while partial.count('{') > partial.count('}'):
            partial += '}'
        try:
            return json.loads(partial)
        except json.JSONDecodeError:
            pass

        # Extract known fields from truncated JSON with regex
        sql_match = re.search(r'"sql"\s*:\s*"((?:[^"\\]|\\.)*)"', partial, re.DOTALL)
        needs_data = '"needs_data": true' in partial or '"needs_data":true' in partial
        if sql_match and needs_data:
            logger.warning("Recovered SQL from truncated LLM response")
            return {
                "thinking": "(truncated)",
                "sql": sql_match.group(1).replace('\\n', '\n').replace('\\"', '"'),
                "explanation": "Query recovered from truncated response",
                "needs_data": True,
            }
        answer_match = re.search(r'"answer"\s*:\s*"((?:[^"\\]|\\.)*)"', partial, re.DOTALL)
        if answer_match and not needs_data:
            return {
                "thinking": "(truncated)",
                "answer": answer_match.group(1).replace('\\n', '\n').replace('\\"', '"'),
                "needs_data": False,
            }

    raise ValueError(f"Could not parse JSON from LLM response: {text[:500]}")


# ── Public API ────────────────────────────────────────────────────────

def generate_sql(conversation_history: List[Dict]) -> dict:
    """Interpret the latest question and generate SQL (or conversational response)."""
    raw = _invoke(conversation_history)
    logger.debug("LLM raw (generate_sql): %s", raw[:500])
    return _parse_json_response(raw)


def _compute_column_stats(columns: List[str], rows: List[list]) -> str:
    """Pre-compute per-column statistics to reduce tokens sent to the LLM."""
    if not rows:
        return "No data rows returned."

    parts = [f"Columns: {columns}", f"Row count: {len(rows)}"]

    for ci, col in enumerate(columns):
        vals = [r[ci] for r in rows if r[ci] is not None]
        if not vals:
            continue

        numerics = []
        for v in vals:
            try:
                numerics.append(float(v))
            except (TypeError, ValueError):
                pass

        if numerics and len(numerics) > 3:
            numerics.sort()
            parts.append(
                f"  {col}: min={numerics[0]:.4g}, max={numerics[-1]:.4g}, "
                f"mean={statistics.mean(numerics):.4g}, median={statistics.median(numerics):.4g}, "
                f"n={len(numerics)}"
            )
        elif numerics:
            parts.append(f"  {col}: values={[round(v, 4) for v in numerics]}")

    return "\n".join(parts)


def synthesize_answer(conversation_history: List[Dict], query_result: dict, original_sql: str) -> dict:
    """Produce a natural-language answer and optional chart configuration.
    Uses a faster model and pre-computed statistics to reduce latency."""
    columns = query_result["columns"]
    rows = query_result["rows"]
    row_count = len(rows)

    col_stats = _compute_column_stats(columns, rows)

    if row_count <= 30:
        data_section = f"{col_stats}\n\nFull data:\n{json.dumps(rows, cls=_SafeEncoder)}"
    elif row_count <= 100:
        data_section = (
            f"{col_stats}\n\n"
            f"First 20 rows:\n{json.dumps(rows[:20], cls=_SafeEncoder)}\n"
            f"Last 5 rows:\n{json.dumps(rows[-5:], cls=_SafeEncoder)}"
        )
    else:
        data_section = (
            f"{col_stats}\n\n"
            f"First 15 rows:\n{json.dumps(rows[:15], cls=_SafeEncoder)}\n"
            f"Last 5 rows:\n{json.dumps(rows[-5:], cls=_SafeEncoder)}"
        )

    synthesis_message = {
        "role": "user",
        "content": (
            f"The SQL query was executed successfully.\n\n"
            f"**SQL:** `{original_sql}`\n\n"
            f"**Results ({row_count} rows):**\n{data_section}\n\n"
            f"{'(Results were truncated to ' + str(config.MAX_QUERY_ROWS) + ' rows)' if query_result.get('truncated') else ''}\n\n"
            f"Synthesize a clear, insightful answer with key numbers. "
            f"Respond with JSON: {{\"answer\": \"...\", \"explanation\": \"...\", \"chart\": {{...}} or null}}"
        ),
    }

    messages = conversation_history + [synthesis_message]
    raw = _invoke(messages, temperature=0.2, max_tokens=6144,
                  model_id=config.BEDROCK_SYNTH_MODEL_ID,
                  family=_synth_model_family)
    logger.debug("LLM raw (synthesize): %s", raw[:500])
    return _parse_json_response(raw)
