"""
config.py — Centralised configuration loaded from environment variables.

All secrets and environment-specific settings live in .env / Airflow Variables.
Nothing is hard-coded except safe defaults.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class PipelineConfig:
    # Alpha Vantage
    alpha_vantage_api_key: str = field(
        default_factory=lambda: _require_env("ALPHA_VANTAGE_API_KEY")
    )
    alpha_vantage_base_url: str = "https://www.alphavantage.co/query"

    # Symbols to ingest
    symbols: List[str] = field(default_factory=lambda: ["AAPL", "TSLA", "SPY"])

    # GCP
    gcp_project_id: str = field(
        default_factory=lambda: _require_env("GCP_PROJECT_ID")
    )
    gcs_bucket_name: str = field(
        default_factory=lambda: _require_env("GCS_BUCKET_NAME")
    )
    google_application_credentials: str = field(
        default_factory=lambda: os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/opt/airflow/credentials/service_account.json",
        )
    )

    # BigQuery
    bq_dataset: str = field(
        default_factory=lambda: os.getenv("BQ_DATASET", "market_analytics")
    )
    bq_staging_table: str = "raw_stock_prices"

    # Local staging directory inside the container
    local_data_dir: str = field(
        default_factory=lambda: os.getenv("LOCAL_DATA_DIR", "/tmp/market_data")
    )

    # Alpha Vantage rate-limit: free tier = 25 req/day, premium = 75/min
    api_sleep_seconds: float = field(
        default_factory=lambda: float(os.getenv("API_SLEEP_SECONDS", "15"))
    )


def _require_env(name: str) -> str:
    """Raise a clear error if a required environment variable is missing."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "Check your .env file and Docker Compose configuration."
        )
    return value


# Module-level singleton — import this everywhere
config = PipelineConfig()
