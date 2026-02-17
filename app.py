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

import config
import db
import llm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

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


class QueryResponse(BaseModel):
    answer: str
    explanation: Optional[str] = None
    sql: Optional[str] = None
    sql_explanation: Optional[str] = None
    data: Optional[dict] = None
    chart: Optional[dict] = None
    error: Optional[str] = None
    request_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Main query endpoint
# ---------------------------------------------------------------------------

@app.post("/api/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    """Process a natural-language question through the full pipeline."""
    request_id = req.request_id or str(uuid.uuid4())
    history = _get_history(req.session_id)

    history.append({"role": "user", "content": req.question})
    _trim_history(history)

    try:
        # Check cancellation before LLM call
        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        # Step 1: Ask LLM to interpret and generate SQL
        llm_response = llm.generate_sql(history)

        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        if not llm_response.get("needs_data", True):
            answer = llm_response.get("answer", "I'm not sure how to answer that.")
            history.append({"role": "assistant", "content": answer})
            return QueryResponse(answer=answer, request_id=request_id)

        sql = llm_response.get("sql", "")
        sql_explanation = llm_response.get("explanation", "")

        if not sql:
            answer = "I couldn't generate a query for that question. Could you rephrase?"
            history.append({"role": "assistant", "content": answer})
            return QueryResponse(answer=answer, request_id=request_id)

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
                return QueryResponse(answer=answer, sql=llm_response.get("sql"), error=error_msg, request_id=request_id)

            logger.info("Retry SQL: %s", sql)
            try:
                query_result = db.execute_query(sql, retry_response.get("sql_params"), request_id=request_id)
            except Exception as e2:
                error_msg2 = str(e2)
                logger.error("SQL retry also failed: %s", error_msg2)
                answer = f"I tried two queries but both failed. Last error: {error_msg2}"
                history.append({"role": "assistant", "content": answer})
                return QueryResponse(answer=answer, sql=sql, sql_explanation=sql_explanation, error=error_msg2, request_id=request_id)

        # Step 3: Synthesize answer from results
        if _is_cancelled(request_id):
            raise InterruptedError("Request cancelled by user")

        synthesis = llm.synthesize_answer(history, query_result, sql)

        answer = synthesis.get("answer", "Query executed successfully.")
        explanation = synthesis.get("explanation", "")
        chart_config = synthesis.get("chart")

        history.append({"role": "assistant", "content": answer})

        return QueryResponse(
            answer=answer,
            explanation=explanation,
            sql=sql,
            sql_explanation=sql_explanation,
            data=query_result,
            chart=chart_config,
            request_id=request_id,
        )

    except InterruptedError:
        _cancelled_requests.discard(request_id)
        answer = "Request was cancelled."
        history.append({"role": "assistant", "content": answer})
        return QueryResponse(answer=answer, request_id=request_id)

    except Exception as e:
        logger.error("Pipeline error: %s\n%s", str(e), traceback.format_exc())
        error_answer = f"An error occurred processing your question: {str(e)}"
        history.append({"role": "assistant", "content": error_answer})
        return QueryResponse(answer=error_answer, error=str(e), request_id=request_id)

    finally:
        _cancelled_requests.discard(request_id)


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
