"""Application configuration — loaded from environment variables."""

import os

# Database
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "forecast")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

# AWS Bedrock
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "us.amazon.nova-pro-v1:0")

# Query safety
MAX_QUERY_ROWS = int(os.getenv("MAX_QUERY_ROWS", "5000"))
QUERY_TIMEOUT_SEC = int(os.getenv("QUERY_TIMEOUT_SEC", "120"))
