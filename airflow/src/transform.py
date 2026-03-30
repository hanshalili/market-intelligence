"""
transform.py — Convert raw JSON/CSV data to Parquet and upload to GCS.

Design decisions:
- Parquet is chosen because it is columnar, compressed, and optimised for
  analytical workloads (BigQuery loads Parquet ~3× faster than CSV).
- Files are written with Hive-style partitioning: curated/stock_prices/date=YYYY-MM-DD/data.parquet
  This allows BigQuery external tables and dbt to use partition pruning.
- The function is idempotent: uploading the same file twice overwrites safely.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import storage

from src.config import config

logger = logging.getLogger(__name__)


def dataframe_to_parquet_local(
    df: pd.DataFrame,
    execution_date: date,
    local_dir: Optional[str] = None,
) -> str:
    """
    Write a DataFrame to a local Parquet file using the Hive partition path.

    Args:
        df:             DataFrame to persist.
        execution_date: Trading date — used as the partition value.
        local_dir:      Override for the local staging directory.

    Returns:
        Absolute local path of the written file.
    """
    local_dir = local_dir or config.local_data_dir
    partition_dir = (
        Path(local_dir)
        / "curated"
        / "stock_prices"
        / f"date={execution_date.isoformat()}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_path = partition_dir / "data.parquet"

    df.to_parquet(
        file_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    file_size_kb = file_path.stat().st_size / 1024
    logger.info(
        "Written Parquet file: %s (%.1f KB, %d rows)", file_path, file_size_kb, len(df)
    )

    return str(file_path)


def upload_raw_json_to_gcs(
    raw_data: dict,
    symbol: str,
    execution_date: date,
) -> str:
    """
    Upload the raw Alpha Vantage JSON response to GCS for auditability.

    Path pattern: raw/{symbol_lower}/date={YYYY-MM-DD}/data.json

    Args:
        raw_data:       The raw JSON dict from the API response.
        symbol:         Ticker symbol.
        execution_date: Trading date.

    Returns:
        GCS URI of the uploaded object (gs://bucket/path).
    """
    import json

    client = storage.Client()
    bucket = client.bucket(config.gcs_bucket_name)

    gcs_path = f"raw/{symbol.lower()}/date={execution_date.isoformat()}/data.json"
    blob = bucket.blob(gcs_path)

    blob.upload_from_string(
        json.dumps(raw_data, indent=2),
        content_type="application/json",
    )

    gcs_uri = f"gs://{config.gcs_bucket_name}/{gcs_path}"
    logger.info("Uploaded raw JSON to %s", gcs_uri)
    return gcs_uri


def upload_parquet_to_gcs(
    local_parquet_path: str,
    execution_date: date,
) -> str:
    """
    Upload the curated Parquet file to GCS.

    Path pattern: curated/stock_prices/date=YYYY-MM-DD/data.parquet

    Args:
        local_parquet_path: Local path to the .parquet file.
        execution_date:     Trading date — used as the partition in GCS.

    Returns:
        GCS URI of the uploaded object.
    """
    client = storage.Client()
    bucket = client.bucket(config.gcs_bucket_name)

    gcs_path = f"curated/stock_prices/date={execution_date.isoformat()}/data.parquet"
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_parquet_path)

    gcs_uri = f"gs://{config.gcs_bucket_name}/{gcs_path}"
    logger.info("Uploaded curated Parquet to %s", gcs_uri)
    return gcs_uri


def load_raw_to_dataframe(symbol: str, execution_date: date) -> pd.DataFrame:
    """
    Download the raw JSON from GCS and parse it back to a DataFrame.

    Used by the transform step to re-read what was stored in the raw layer
    without re-calling the API, keeping steps cleanly separated.
    """
    import json

    client = storage.Client()
    bucket = client.bucket(config.gcs_bucket_name)

    gcs_path = f"raw/{symbol.lower()}/date={execution_date.isoformat()}/data.json"
    blob = bucket.blob(gcs_path)

    if not blob.exists():
        raise FileNotFoundError(
            f"Raw file not found at gs://{config.gcs_bucket_name}/{gcs_path}. "
            "Ensure the extract step ran successfully."
        )

    raw_json = json.loads(blob.download_as_text())

    # Re-parse from the same structure as extract.py produces
    from src.extract import _cast_schema, _AV_COLUMN_MAP

    ts_data = raw_json.get("Time Series (Daily)", {})
    rows = []
    for date_str, values in ts_data.items():
        row = {"date": date_str, "symbol": symbol.upper()}
        for av_col, our_col in _AV_COLUMN_MAP.items():
            row[our_col] = values.get(av_col)
        rows.append(row)

    df = pd.DataFrame(rows)
    df = _cast_schema(df)

    # Filter to the single execution date for incremental loads
    date_str = execution_date.isoformat()
    df = df[df["date"] == date_str].copy()

    logger.info(
        "Loaded %d rows from raw GCS for symbol=%s date=%s", len(df), symbol, date_str
    )
    return df
