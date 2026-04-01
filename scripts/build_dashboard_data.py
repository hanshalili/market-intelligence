"""
build_dashboard_data.py — Query BigQuery mart_daily_metrics and write dashboard/data.json
"""
import json, os, sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "airflow"))

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"))

SA = os.path.join(ROOT, "airflow", "credentials", "service_account.json")
if os.path.exists(SA):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA

import warnings
warnings.filterwarnings("ignore")

from google.cloud import bigquery
import pandas as pd

PROJECT = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "market_analytics")
TABLE   = f"`{PROJECT}.{DATASET}.mart_daily_metrics`"

client = bigquery.Client(project=PROJECT)

# ── 1. Latest KPIs per ticker ─────────────────────────────────────────
kpi_sql = f"""
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rk
  FROM {TABLE}
  WHERE symbol IN ('NVDA','AAPL','GOOGL','TSLA','VOO')
)
SELECT
  symbol,
  date,
  open,
  adjusted_close   AS close,
  volume,
  daily_return,
  cumulative_return,
  rolling_volatility_20d AS volatility,
  drawdown,
  sma_20,
  sma_50
FROM ranked
WHERE rk = 1
ORDER BY symbol
"""

# ── 2. Full price history for charts ─────────────────────────────────
hist_sql = f"""
SELECT date, symbol, open, adjusted_close AS close
FROM {TABLE}
WHERE symbol IN ('NVDA','AAPL','GOOGL','TSLA','VOO')
ORDER BY symbol, date
"""

print("Querying KPIs ...")
kpi_df  = client.query(kpi_sql).to_dataframe(create_bqstorage_client=False)
print("Querying price history ...")
hist_df = client.query(hist_sql).to_dataframe(create_bqstorage_client=False)

hist_df["date"] = pd.to_datetime(hist_df["date"]).dt.strftime("%b %-d")

SYMBOLS = ["NVDA", "AAPL", "GOOGL", "TSLA", "VOO"]

# Build output structure
dates = []
close = {s: [] for s in SYMBOLS}
open_ = {s: [] for s in SYMBOLS}

for sym in SYMBOLS:
    grp = hist_df[hist_df["symbol"] == sym]
    if dates == []:
        dates = grp["date"].tolist()
    close[sym] = [round(float(v), 2) for v in grp["close"]]
    open_[sym] = [round(float(v), 2) for v in grp["open"]]

tickers = {}
for _, row in kpi_df.iterrows():
    sym = row["symbol"]
    tickers[sym] = {
        "date":             str(row["date"]),
        "open":             round(float(row["open"] or 0), 2),
        "close":            round(float(row["close"] or 0), 2),
        "volume":           int(row["volume"] or 0),
        "daily_return":     round(float(row["daily_return"] or 0), 2),
        "cumulative_return":round(float(row["cumulative_return"] or 0), 2),
        "volatility":       round(float(row["volatility"] or 0), 2),
        "drawdown":         round(float(row["drawdown"] or 0), 2),
        "sma_20":           round(float(row["sma_20"] or 0), 2),
        "sma_50":           round(float(row["sma_50"] or 0), 2),
    }

out = {
    "generated_at": datetime.now(tz=timezone.utc).isoformat(),
    "tickers": tickers,
    "dates":   dates,
    "close":   close,
    "open":    open_,
}

path = os.path.join(ROOT, "dashboard", "data.json")
with open(path, "w") as f:
    json.dump(out, f, indent=2, default=str)

print(f"Written → {path}")
print("\n── KPI snapshot ──────────────────────────────")
print(kpi_df[["symbol","date","open","close","cumulative_return","volatility","drawdown"]].to_string(index=False))
