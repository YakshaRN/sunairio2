#!/bin/bash
# Forecast Query Engine — Startup Script
#
# Auth options (pick one):
#
#   Option A — Bearer Token (recommended):
#     export AWS_BEARER_TOKEN_BEDROCK=bedrock-api-key-...
#
#   Option B — IAM Credentials:
#     export AWS_ACCESS_KEY_ID=your_key
#     export AWS_SECRET_ACCESS_KEY=your_secret
#     export AWS_REGION=us-east-2
#
# Usage:
#   chmod +x run.sh
#   ./run.sh

set -e

cd "$(dirname "$0")"

echo "=== Forecast Query Engine ==="
echo ""

# Check auth
if [ -n "$AWS_BEARER_TOKEN_BEDROCK" ]; then
    echo "Auth: Bearer Token detected"
elif [ -n "$AWS_ACCESS_KEY_ID" ]; then
    echo "Auth: IAM credentials detected"
else
    echo "WARNING: No AWS credentials found."
    echo "  Set AWS_BEARER_TOKEN_BEDROCK or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
    echo ""
fi

echo "Configuration:"
echo "  DB Host:  ${DB_HOST:-forecastproxy-read-only.endpoint.proxy-cxs3s5zv5hek.us-east-2.rds.amazonaws.com}"
echo "  DB Name:  ${DB_NAME:-forecast}"
echo "  Region:   ${AWS_REGION:-us-east-2}"
echo "  Model:    ${BEDROCK_MODEL_ID:-anthropic.claude-3-sonnet-20240229-v1:0}"
echo ""
echo "Starting server on http://0.0.0.0:${PORT:-8000} ..."
echo ""

python3 -m uvicorn app:app --host 0.0.0.0 --port "${PORT:-8000}" --workers 1
