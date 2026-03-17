"""
Forecast Query Application — FastAPI backend.

Natural-language → SQL → Visualization pipeline
backed by AWS Bedrock (Claude) and Amazon Aurora PostgreSQL.

SECURITY: All database access is strictly read-only.
No data modification or deletion is possible under any circumstances.
"""

from __future__ import annotations

import json
import logging
import time
import traceback
import csv
import io
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import hashlib

import config
import db
import llm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Response cache (30-min TTL by default)
# ---------------------------------------------------------------------------
_cache: Dict[str, dict] = {}  # key -> {"response": QueryResponse dict, "ts": float}


def _cache_key(question: str) -> str:
    return hashlib.sha256(question.strip().lower().encode()).hexdigest()


def _cache_get(question: str) -> Optional[dict]:
    key = _cache_key(question)
    entry = _cache.get(key)
    if entry and (time.monotonic() - entry["ts"]) < config.CACHE_TTL_SEC:
        logger.info("Cache HIT for question (key=%s)", key[:12])
        return entry["response"]
    if entry:
        del _cache[key]
    return None


def _cache_put(question: str, response_dict: dict):
    key = _cache_key(question)
    _cache[key] = {"response": response_dict, "ts": time.monotonic()}
    if len(_cache) > 500:
        oldest = min(_cache, key=lambda k: _cache[k]["ts"])
        del _cache[oldest]

app = FastAPI(title="Forecast Query Engine", version="1.0.0")

# ---------------------------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    db.init_pool()
    llm.init_client()
    logger.info("Application started (READ-ONLY mode enforced)")


@app.on_event("shutdown")
async def shutdown():
    db.close_pool()
    logger.info("Application shut down")


# ---------------------------------------------------------------------------
# In-memory session store (conversation history per session)
# ---------------------------------------------------------------------------

_sessions: Dict[str, List[Dict]] = {}

MAX_HISTORY_TURNS = 20

# Track cancelled requests so pipeline steps can check and abort
_cancelled_requests: Set[str] = set()


def _get_history(session_id: str) -> List[Dict]:
    if session_id not in _sessions:
        _sessions[session_id] = []
    return _sessions[session_id]


def _trim_history(history: List[Dict]):
    while len(history) > MAX_HISTORY_TURNS * 2:
        history.pop(0)


def _is_cancelled(request_id: str) -> bool:
    return request_id in _cancelled_requests


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str
    session_id: str = "default"
    request_id: Optional[str] = None


class QueryMetrics(BaseModel):
    total_time_ms: Optional[float] = None
    query_time_ms: Optional[float] = None
    row_count: Optional[int] = None
    data_volume_bytes: Optional[int] = None
    cached: Optional[bool] = None


class QueryResponse(BaseModel):
    answer: str
    explanation: Optional[str] = None
    sql: Optional[str] = None
    sql_explanation: Optional[str] = None
    data: Optional[dict] = None
    chart: Optional[dict] = None
    error: Optional[str] = None
    request_id: Optional[str] = None
    metrics: Optional[QueryMetrics] = None


# ---------------------------------------------------------------------------
# Main query endpoint
# ---------------------------------------------------------------------------

@app.post("/api/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    """Process a natural-language question through the full pipeline."""
    t_start = time.monotonic()
    request_id = req.request_id or str(uuid.uuid4())
    history = _get_history(req.session_id)
    metrics = QueryMetrics()

    # Check cache before doing any work
    cached = _cache_get(req.question)
    if cached:
        cached_resp = cached.copy()
        cached_resp["request_id"] = request_id
        if "metrics" not in cached_resp or cached_resp["metrics"] is None:
            cached_resp["metrics"] = {}
        cached_resp["metrics"]["total_time_ms"] = 0.1
        cached_resp["metrics"]["cached"] = True
        return QueryResponse(**cached_resp)

    history.append({"role": "user", "content": req.question})
    _trim_history(history)

    try:
        # Check cancellation before LLM call
        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        # Step 1: Ask LLM to interpret and generate SQL
        llm_response = llm.generate_sql(history)
        t_after_gen = time.monotonic()
        logger.info("LLM generate_sql completed in %.2fs", t_after_gen - t_start)

        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        if not llm_response.get("needs_data", True):
            answer = llm_response.get("answer", "I'm not sure how to answer that.")
            history.append({"role": "assistant", "content": answer})
            metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)
            return QueryResponse(answer=answer, request_id=request_id, metrics=metrics)

        sql = llm_response.get("sql", "")
        sql_explanation = llm_response.get("explanation", "")

        if not sql:
            answer = "I couldn't generate a query for that question. Could you rephrase?"
            history.append({"role": "assistant", "content": answer})
            metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)
            return QueryResponse(answer=answer, request_id=request_id, metrics=metrics)

        logger.info("Generated SQL: %s", sql)

        # Step 2: Execute the SQL query (read-only enforced by db module)
        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        try:
            query_result = db.execute_query(sql, llm_response.get("sql_params"), request_id=request_id)
        except Exception as e:
            if _is_cancelled(request_id):
                raise InterruptedError("Request cancelled by user")

            error_msg = str(e)
            logger.error("SQL execution error: %s", error_msg)

            history.append({
                "role": "assistant",
                "content": json.dumps({"sql": sql, "error": error_msg}),
            })
            history.append({
                "role": "user",
                "content": (
                    f"The SQL query failed with error: {error_msg}\n"
                    f"Please fix the query and try again. Respond with the same JSON format."
                ),
            })

            retry_response = llm.generate_sql(history)
            sql = retry_response.get("sql", "")
            sql_explanation = retry_response.get("explanation", sql_explanation)

            if not sql:
                answer = f"I tried to query the database but encountered an error: {error_msg}"
                history.append({"role": "assistant", "content": answer})
                metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)
                return QueryResponse(answer=answer, sql=llm_response.get("sql"), error=error_msg, request_id=request_id, metrics=metrics)

            logger.info("Retry SQL: %s", sql)
            try:
                query_result = db.execute_query(sql, retry_response.get("sql_params"), request_id=request_id)
            except Exception as e2:
                error_msg2 = str(e2)
                logger.error("SQL retry also failed: %s", error_msg2)
                answer = f"I tried two queries but both failed. Last error: {error_msg2}"
                history.append({"role": "assistant", "content": answer})
                metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)
                return QueryResponse(answer=answer, sql=sql, sql_explanation=sql_explanation, error=error_msg2, request_id=request_id, metrics=metrics)

        # Step 3: Synthesize answer from results
        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        metrics.query_time_ms = query_result.get("query_time_ms")
        metrics.row_count = query_result.get("row_count")
        metrics.data_volume_bytes = query_result.get("data_volume_bytes")

        t_after_query = time.monotonic()
        row_count = query_result.get("row_count", 0)
        col_count = len(query_result.get("columns", []))

        # Skip LLM synthesis for very simple results (1-3 rows, few columns)
        if row_count <= 3 and col_count <= 6 and row_count > 0:
            cols = query_result["columns"]
            rows = query_result["rows"]
            lines = []
            for row in rows:
                parts = [f"**{cols[i]}:** {row[i]}" for i in range(len(cols)) if row[i] is not None]
                lines.append(" | ".join(parts))
            answer = "\n".join(lines)
            explanation = sql_explanation
            chart_config = None
            logger.info("Skipped LLM synthesis (simple result: %d rows, %d cols)", row_count, col_count)
            synth_time = 0.0
        else:
            synthesis = llm.synthesize_answer(history, query_result, sql)
            t_after_synth = time.monotonic()
            synth_time = t_after_synth - t_after_query
            logger.info("LLM synthesize completed in %.2fs", synth_time)

            answer = synthesis.get("answer", "Query executed successfully.")
            explanation = synthesis.get("explanation", "")
            chart_config = synthesis.get("chart")

        history.append({"role": "assistant", "content": answer})

        metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)

        gen_time = t_after_gen - t_start
        query_time_s = t_after_query - t_after_gen
        total = time.monotonic() - t_start
        logger.info("Pipeline complete: total=%.1fs, llm_gen=%.1fs, query=%.1fs, llm_synth=%.1fs, rows=%d",
                     total, gen_time, query_time_s, synth_time, row_count)

        resp = QueryResponse(
            answer=answer,
            explanation=explanation,
            sql=sql,
            sql_explanation=sql_explanation,
            data=query_result,
            chart=chart_config,
            request_id=request_id,
            metrics=metrics,
        )

        _cache_put(req.question, resp.dict())
        return resp

    except InterruptedError:
        _cancelled_requests.discard(request_id)
        answer = "Request was cancelled."
        history.append({"role": "assistant", "content": answer})
        metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)
        return QueryResponse(answer=answer, request_id=request_id, metrics=metrics)

    except Exception as e:
        logger.error("Pipeline error: %s\n%s", str(e), traceback.format_exc())
        error_answer = f"An error occurred processing your question: {str(e)}"
        history.append({"role": "assistant", "content": error_answer})
        metrics.total_time_ms = round((time.monotonic() - t_start) * 1000, 1)
        return QueryResponse(answer=error_answer, error=str(e), request_id=request_id, metrics=metrics)

    finally:
        _cancelled_requests.discard(request_id)


# ---------------------------------------------------------------------------
# Streaming query endpoint (SSE)
# ---------------------------------------------------------------------------

@app.post("/api/query/stream")
async def query_stream(req: QueryRequest):
    """Stream the query pipeline via Server-Sent Events.
    Events: sql_ready, data_ready, answer_chunk, complete, error."""
    import asyncio

    request_id = req.request_id or str(uuid.uuid4())

    async def event_generator():
        t_start = time.monotonic()

        cached = _cache_get(req.question)
        if cached:
            cached["metrics"] = cached.get("metrics") or {}
            cached["metrics"]["total_time_ms"] = 0.1
            cached["metrics"]["cached"] = True
            cached["event"] = "complete"
            yield f"data: {json.dumps(cached)}\n\n"
            return

        history = _get_history(req.session_id)
        history.append({"role": "user", "content": req.question})
        _trim_history(history)

        try:
            llm_response = llm.generate_sql(history)
            t_after_gen = time.monotonic()
            gen_time = t_after_gen - t_start
            logger.info("Stream LLM generate_sql: %.2fs", gen_time)

            if not llm_response.get("needs_data", True):
                answer = llm_response.get("answer", "I'm not sure how to answer that.")
                history.append({"role": "assistant", "content": answer})
                yield f"data: {json.dumps({'event': 'complete', 'answer': answer, 'metrics': {'total_time_ms': round(gen_time * 1000, 1)}})}\n\n"
                return

            sql = llm_response.get("sql", "")
            if not sql:
                yield f"data: {json.dumps({'event': 'error', 'message': 'Could not generate SQL'})}\n\n"
                return

            yield f"data: {json.dumps({'event': 'sql_ready', 'sql': sql, 'explanation': llm_response.get('explanation', '')})}\n\n"
            await asyncio.sleep(0)

            try:
                query_result = db.execute_query(sql, llm_response.get("sql_params"), request_id=request_id)
            except Exception as e:
                yield f"data: {json.dumps({'event': 'error', 'message': str(e)})}\n\n"
                return

            t_after_query = time.monotonic()
            row_count = query_result.get("row_count", 0)
            col_count = len(query_result.get("columns", []))

            yield f"data: {json.dumps({'event': 'data_ready', 'row_count': row_count, 'columns': query_result.get('columns', [])})}\n\n"
            await asyncio.sleep(0)

            if row_count <= 3 and col_count <= 6 and row_count > 0:
                cols = query_result["columns"]
                rows = query_result["rows"]
                lines = []
                for row in rows:
                    parts = [f"**{cols[i]}:** {row[i]}" for i in range(len(cols)) if row[i] is not None]
                    lines.append(" | ".join(parts))
                answer = "\n".join(lines)
                explanation = llm_response.get("explanation", "")
                chart_config = None
                synth_time = 0.0
                logger.info("Stream: skipped synthesis (simple %d rows)", row_count)
            else:
                synthesis = llm.synthesize_answer(history, query_result, sql)
                synth_time = time.monotonic() - t_after_query
                logger.info("Stream LLM synthesize: %.2fs", synth_time)
                answer = synthesis.get("answer", "Query executed successfully.")
                explanation = synthesis.get("explanation", "")
                chart_config = synthesis.get("chart")

            history.append({"role": "assistant", "content": answer})
            total = time.monotonic() - t_start
            query_time_s = t_after_query - t_after_gen

            logger.info("Stream pipeline: total=%.1fs, gen=%.1fs, query=%.1fs, synth=%.1fs, rows=%d",
                        total, gen_time, query_time_s, synth_time, row_count)

            resp_data = {
                'event': 'complete', 'answer': answer, 'explanation': explanation,
                'chart': chart_config, 'sql': sql, 'sql_explanation': llm_response.get('explanation', ''),
                'data': query_result,
                'metrics': {
                    'total_time_ms': round(total * 1000, 1),
                    'query_time_ms': query_result.get('query_time_ms'),
                    'row_count': row_count,
                    'data_volume_bytes': query_result.get('data_volume_bytes'),
                },
            }
            _cache_put(req.question, resp_data)
            yield f"data: {json.dumps(resp_data)}\n\n"

        except Exception as e:
            logger.error("Stream error: %s\n%s", str(e), traceback.format_exc())
            yield f"data: {json.dumps({'event': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Cancel endpoint
# ---------------------------------------------------------------------------

@app.post("/api/cancel")
async def cancel_request(request: Request):
    """Cancel an in-flight query request. Stops both the SQL query and pipeline."""
    body = await request.json()
    request_id = body.get("request_id", "")
    if not request_id:
        return {"status": "error", "message": "request_id is required"}

    _cancelled_requests.add(request_id)
    db_cancelled = db.cancel_query(request_id)

    logger.info("Cancel requested for %s (db_cancelled=%s)", request_id, db_cancelled)
    return {"status": "ok", "request_id": request_id, "db_query_cancelled": db_cancelled}


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@app.post("/api/clear")
async def clear_session(session_id: str = "default"):
    if session_id in _sessions:
        _sessions[session_id] = []
    return {"status": "ok", "message": "Session cleared"}


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

@app.post("/api/export/csv")
async def export_csv(request: Request):
    body = await request.json()
    columns = body.get("columns", [])
    rows = body.get("rows", [])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=forecast_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"},
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    try:
        result = db.execute_query("SELECT 1 AS ok")
        return {"status": "healthy", "database": "connected", "mode": "read-only", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(e)})


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(html_path.read_text())


app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

# Avahi 50 query results (report + JSON)
_screenshots_dir = Path(__file__).parent / "screenshots"
if _screenshots_dir.exists():
    app.mount("/screenshots", StaticFiles(directory=_screenshots_dir), name="screenshots")
