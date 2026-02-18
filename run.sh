#!/bin/bash
# Forecast Query Engine — Startup Script
#
# Prerequisites:
#   1. EC2 instance must have an IAM role with AmazonBedrockFullAccess policy
#   2. Set database credentials via environment variables or .env file
#
# Usage:
#   chmod +x run.sh
#   ./run.sh

set -e

cd "$(dirname "$0")"

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

echo "=== Forecast Query Engine ==="
echo ""
echo "Configuration:"
echo "  DB Host:  ${DB_HOST:-(not set)}"
echo "  DB Name:  ${DB_NAME:-forecast}"
echo "  Region:   ${AWS_REGION:-us-east-2}"
echo "  Model:    ${BEDROCK_MODEL_ID:-us.amazon.nova-pro-v1:0}"
echo "  Auth:     IAM Role (EC2 instance profile)"
echo ""
echo "Starting server on http://0.0.0.0:${PORT:-8000} ..."
echo ""

python3 -m uvicorn app:app --host 0.0.0.0 --port "${PORT:-8000}" --workers 1
