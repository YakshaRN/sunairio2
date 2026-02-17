# Forecast Query Engine

Natural-language forecasting query engine for ERCOT & PJM energy and weather ensemble data. Converts conversational questions into SQL, executes against Amazon Aurora PostgreSQL, and returns answers with interactive visualizations.

## Architecture

```
User Question → LLM (Bedrock Claude) → SQL Generation → Aurora PostgreSQL → Data → LLM Synthesis → Answer + Chart
```

### Backend (FastAPI)
- **app.py** — API server, pipeline orchestration, session management
- **llm.py** — AWS Bedrock integration (Bearer Token + IAM auth), NL→SQL + answer synthesis
- **db.py** — Connection pool, query execution, safety guards
- **schema.py** — Complete schema knowledge base, domain expertise, query patterns
- **config.py** — Environment-based configuration

### Frontend (Single-page HTML + Plotly.js)
- Chat-style interface with conversation context
- Response panels: text answer, SQL query, explanation, data table (all toggleable)
- Interactive Plotly charts (line, bar, scatter, area)
- CSV data export and PNG chart download

## Data Sources

| Table | Description | Scale |
|---|---|---|
| `weather_forecast_ensemble` | Short-range weather forecasts (7-14 day) | ~258B rows |
| `weather_seasonal_ensemble` | Seasonal weather forecasts (months ahead) | ~3.6B rows |
| `energy_base_ensemble` | Baseline energy scenarios | ~5.7B rows |
| `energy_forecast_ensemble` | Active energy forecasts | ~125B rows |

Each table has 1,000 ensemble members (paths 0–999) enabling probabilistic analysis.

## Quick Start

```bash
# 1. Clone
git clone https://github.com/YakshaRN/sunairio2.git
cd sunairio2

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure (copy and fill in .env.example)
cp .env.example .env
# Edit .env with your credentials

# 4. Source env and run
source .env  # or: export $(cat .env | xargs)
./run.sh
```

Open `http://localhost:8000` in your browser.

## Example Questions

- "What is the average forecasted temperature in Houston for the next 7 days?"
- "Compare wind energy generation vs. load for ERCOT west zone this week"
- "What's the probability that temperature exceeds 35°C in any ERCOT zone next week?"
- "Show the P10, P50, and P90 solar generation forecast for PJM over the next 5 days"
- "What's the relationship between forecasted peak load and temperature for next month?"

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Description | Default |
|---|---|---|
| `DB_HOST` | Aurora/RDS Proxy endpoint | (required) |
| `DB_USER` | Database username | (required) |
| `DB_PASSWORD` | Database password | (required) |
| `AWS_REGION` | AWS region for Bedrock | us-east-2 |
| `BEDROCK_MODEL_ID` | Claude model ID | anthropic.claude-3-sonnet-20240229-v1:0 |
| `MAX_QUERY_ROWS` | Max rows per query | 5000 |
| `QUERY_TIMEOUT_SEC` | SQL timeout (seconds) | 120 |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Frontend UI |
| `POST` | `/api/query` | Submit natural-language query |
| `POST` | `/api/clear` | Clear conversation session |
| `POST` | `/api/export/csv` | Export data as CSV |
| `GET` | `/api/health` | Health check |
