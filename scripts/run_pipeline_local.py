"""
run_pipeline_local.py — Standalone pipeline runner (no Docker/Airflow required).

Steps:
  1. Fetch 100 days of OHLCV from Alpha Vantage for all 5 symbols
  2. Load directly into BigQuery raw_stock_prices (date-partitioned)
  3. Print a summary of what was loaded
  4. Export a JSON snapshot for the dashboard

Usage:
    cd /Users/hanshalili/Developer/market-intelligence
    python3 scripts/run_pipeline_local.py
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

SA_FILE = os.path.join(PROJECT_ROOT, "airflow", "credentials", "service_account.json")
if os.path.exists(SA_FILE):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_FILE

# ── Add src to path ────────────────────────────────────────
sys.path.insert(0, os.path.join(PROJECT_ROOT, "airflow"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from google.cloud import bigquery  # noqa: E402

# ── Config ─────────────────────────────────────────────────
SYMBOLS    = ["NVDA", "AAPL", "GOOGL", "TSLA", "VOO"]
API_KEY    = os.environ["ALPHA_VANTAGE_API_KEY"]
BASE_URL   = "https://www.alphavantage.co/query"
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = os.environ.get("BQ_DATASET", "market_analytics")
TABLE      = "raw_stock_prices"
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"
SLEEP_SEC  = 15   # free tier: 5 req/min


# ── Step 1: Fetch ──────────────────────────────────────────
def fetch(symbol: str) -> pd.DataFrame:
    log.info("Fetching %s from Alpha Vantage ...", symbol)
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     symbol,
        "outputsize": "compact",   # last 100 trading days
        "apikey":     API_KEY,
        "datatype":   "json",
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    if "Error Message" in payload:
        raise RuntimeError(f"AV error for {symbol}: {payload['Error Message']}")
    if "Information" in payload:
        raise RuntimeError(
            f"Alpha Vantage daily quota exhausted (25 req/day free tier).\n"
            f"Message: {payload['Information']}"
        )
    if "Note" in payload:
        log.warning("Rate limit hit — sleeping 65s then retrying %s", symbol)
        time.sleep(65)
        return fetch(symbol)

    ts = payload.get("Time Series (Daily)", {})
    if not ts:
        raise RuntimeError(f"No time series data for {symbol}. Keys: {list(payload.keys())}")

    rows = []
    for date_str, vals in ts.items():
        rows.append({
            "date":           date_str,
            "symbol":         symbol,
            "open":           float(vals.get("1. open", 0) or 0),
            "high":           float(vals.get("2. high", 0) or 0),
            "low":            float(vals.get("3. low", 0)  or 0),
            "close":          float(vals.get("4. close", 0) or 0),
            "adjusted_close": float(vals.get("4. close", 0) or 0),
            "volume":         int(vals.get("5. volume", 0)  or 0),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date").reset_index(drop=True)
    log.info("  → %d rows  (%s – %s)", len(df), df["date"].min(), df["date"].max())
    return df


# ── Step 2: Load to BigQuery ───────────────────────────────
BQ_SCHEMA = [
    bigquery.SchemaField("date",           "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("symbol",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("open",           "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("high",           "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("low",            "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("close",          "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("adjusted_close", "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("volume",         "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("ingested_at",    "TIMESTAMP", mode="REQUIRED"),
]


def load_to_bq(df: pd.DataFrame, client: bigquery.Client):
    df = df.copy()
    df["ingested_at"] = datetime.now(tz=timezone.utc)
    # Keep date as Python date objects — pyarrow converts correctly
    df["date"] = pd.to_datetime(df["date"]).dt.date

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=BQ_SCHEMA,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
        clustering_fields=["symbol"],
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    job = client.load_table_from_dataframe(df, FULL_TABLE, job_config=job_config)
    job.result()
    log.info("  → Loaded %d rows into %s", len(df), FULL_TABLE)


# ── Step 3: Query summary for dashboard ───────────────────
def query_summary(client: bigquery.Client) -> pd.DataFrame:
    sql = f"""
        WITH deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY ingested_at DESC) AS rn
            FROM `{FULL_TABLE}`
            WHERE symbol IN ('NVDA','AAPL','GOOGL','TSLA','VOO')
        ),
        latest AS (
            SELECT date, symbol, open, close, adjusted_close, volume
            FROM deduped
            WHERE rn = 1
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rk,
                FIRST_VALUE(adjusted_close) OVER (
                    PARTITION BY symbol ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_close
            FROM latest
        )
        SELECT
            symbol,
            MAX(CASE WHEN rk = 1 THEN date END)           AS latest_date,
            MAX(CASE WHEN rk = 1 THEN open END)           AS latest_open,
            MAX(CASE WHEN rk = 1 THEN adjusted_close END)  AS latest_close,
            MAX(CASE WHEN rk = 1 THEN volume END)          AS latest_volume,
            ROUND(
                (MAX(CASE WHEN rk = 1 THEN adjusted_close END) -
                 MIN(first_close)) / MIN(first_close) * 100, 2
            )                                               AS cumulative_return_pct,
            ROUND(STDDEV(adjusted_close), 2)               AS price_stddev,
            COUNT(*)                                        AS trading_days,
            MIN(date)                                       AS start_date,
            MAX(date)                                       AS end_date
        FROM ranked
        GROUP BY symbol
        ORDER BY symbol
    """
    log.info("Querying BigQuery for summary ...")
    df = client.query(sql).to_dataframe(create_bqstorage_client=False)
    return df


def query_price_history(client: bigquery.Client) -> pd.DataFrame:
    """Get weekly close + open prices for the dashboard charts."""
    sql = f"""
        WITH deduped AS (
            SELECT date, symbol, open, adjusted_close AS close,
                ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY ingested_at DESC) AS rn
            FROM `{FULL_TABLE}`
            WHERE symbol IN ('NVDA','AAPL','GOOGL','TSLA','VOO')
        )
        SELECT date, symbol, open, close
        FROM deduped
        WHERE rn = 1
        ORDER BY symbol, date
    """
    log.info("Querying price history ...")
    df = client.query(sql).to_dataframe(create_bqstorage_client=False)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%b %-d")
    return df


# ── Step 4: Export JSON for dashboard ────────────────────
def export_dashboard_data(summary: pd.DataFrame, history: pd.DataFrame):
    out = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "tickers": {},
        "dates": [],
        "close": {},
        "open":  {},
    }

    for _, row in summary.iterrows():
        sym = row["symbol"]
        out["tickers"][sym] = {
            "latest_date":          str(row["latest_date"]),
            "latest_open":          round(float(row["latest_open"] or 0), 2),
            "latest_close":         round(float(row["latest_close"] or 0), 2),
            "latest_volume":        int(row["latest_volume"] or 0),
            "cumulative_return_pct":round(float(row["cumulative_return_pct"] or 0), 2),
            "trading_days":         int(row["trading_days"]),
            "start_date":           str(row["start_date"]),
            "end_date":             str(row["end_date"]),
        }

    if not history.empty:
        out["dates"] = history[history["symbol"] == history["symbol"].iloc[0]]["date"].tolist()
        for sym, grp in history.groupby("symbol"):
            out["close"][sym] = [round(float(v), 2) for v in grp["close"]]
            out["open"][sym]  = [round(float(v), 2) for v in grp["open"]]

    data_path = os.path.join(PROJECT_ROOT, "dashboard", "data.json")
    with open(data_path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    log.info("Dashboard data exported → %s", data_path)
    return out


# ── Main ──────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Market Intelligence — Local Pipeline Runner")
    print(f"  Tickers: {', '.join(SYMBOLS)}")
    print("=" * 60)

    all_frames = []
    for i, sym in enumerate(SYMBOLS):
        if i > 0:
            log.info("Sleeping %ss (rate-limit protection) ...", SLEEP_SEC)
            time.sleep(SLEEP_SEC)
        df = fetch(sym)
        all_frames.append(df)

    combined = pd.concat(all_frames, ignore_index=True)
    log.info("Total rows fetched: %d", len(combined))

    client = bigquery.Client(project=PROJECT_ID)

    log.info("Loading all data into BigQuery ...")
    load_to_bq(combined, client)

    summary = query_summary(client)
    history = query_price_history(client)

    print("\n── Summary ──────────────────────────────────────────")
    print(summary[["symbol","latest_date","latest_open","latest_close",
                   "cumulative_return_pct","trading_days"]].to_string(index=False))
    print()

    data = export_dashboard_data(summary, history)

    print("=" * 60)
    print("[DONE] BigQuery loaded. Dashboard data written to dashboard/data.json")
    print("       Now run: python3 scripts/update_website.py")
    print("=" * 60)
    return data


if __name__ == "__main__":
    main()
