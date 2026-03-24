# Forecast Query Engine — Quick usage

## What it is

A web app that answers natural-language questions about ERCOT and PJM forecast data. You type a question; it returns an answer, optional chart, SQL, and raw rows.

## Run locally

1. **Dependencies:** `pip install -r requirements.txt`
2. **Config:** Copy `.env.example` to `.env` and set `DB_*`, `AWS_REGION`, and optional `BEDROCK_MODEL_ID` / `BEDROCK_SYNTH_MODEL_ID`.
3. **AWS:** The host needs IAM permission to call Amazon Bedrock (e.g. instance profile on EC2).
4. **Start:** `./run.sh` or `python3 -m uvicorn app:app --host 0.0.0.0 --port 8000`
5. **Open:** `http://localhost:8000` (or your host’s IP and `PORT` if set).

## Using the UI

- Ask questions in plain English; follow-up questions use the same **session** (default session unless the client sends another `session_id`).
- Use **Clear** (or `POST /api/clear`) to reset conversation context.
- Toggle panels for SQL, explanation, table, and chart where the UI exposes them.
- **Export:** download CSV from the UI when offered, or `POST /api/export/csv` with `columns` and `rows`.

## Tips

- Be specific about region (ERCOT/PJM), variable (e.g. load, wind), and time range.
- Long-running queries can be cancelled if the client sends `request_id` and calls `POST /api/cancel` with that id.

## Health

`GET /api/health` — checks DB connectivity (read-only).
