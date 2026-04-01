# Market Intelligence

A production-grade daily market data pipeline and dashboard. Ingests OHLCV price data for **NVDA, AAPL, GOOGL, TSLA, and VOO** from Alpha Vantage, stores and transforms it in Google Cloud, and serves a minimal futuristic web dashboard built from live BigQuery data.

---

## Why This Exists

Most market dashboards are either expensive SaaS tools or one-off scripts that break. This project is a fully orchestrated, idempotent batch pipeline — built with the same tools used in production data engineering — that runs automatically every weekday after NYSE close and keeps the dashboard fresh without manual intervention.

---

## Features

- **Daily ingestion** — Airflow DAG triggers at 21:00 UTC (Mon–Fri), 30 min after NYSE close
- **5 tickers** — NVDA, AAPL, GOOGL, TSLA, VOO (100 trading days of OHLCV history)
- **Full ELT stack** — Alpha Vantage → GCS (raw JSON + Parquet) → BigQuery → dbt mart
- **Financial metrics** — daily return, cumulative return, SMA-20/50/200, 20-day annualised volatility, drawdown, excess return vs VOO benchmark
- **Live dashboard** — generated from BigQuery; shows close price, cumulative return %, open vs close, and a KPI strip per ticker
- **Infrastructure as code** — GCS bucket, BigQuery dataset, IAM, and service accounts managed by Terraform
- **Data quality** — dbt tests enforce not-null, uniqueness, positive prices, and non-positive drawdown
- **Idempotent** — every pipeline step is safe to re-run; partitioned BigQuery loads use `WRITE_TRUNCATE`

---

## Tech Stack

| Layer | Tools |
|---|---|
| Orchestration | Apache Airflow 2.8 (Docker Compose) |
| Extraction | Python · Alpha Vantage API |
| Storage | Google Cloud Storage (raw JSON + Parquet) |
| Warehouse | BigQuery (partitioned by date, clustered by symbol) |
| Transformation | dbt (staging view + analytics mart) |
| Infrastructure | Terraform |
| Dashboard | Chart.js · Vanilla HTML/CSS |
| Containerisation | Docker |

---

## Architecture

```
Alpha Vantage API
      │  TIME_SERIES_DAILY (5 symbols × 100 days)
      ▼
  extract.py  ──►  GCS raw/  (JSON, partitioned by symbol/date)
      │
  transform.py ──►  GCS curated/  (Parquet, Snappy)
      │
  load.py  ──►  BigQuery: market_analytics.raw_stock_prices
                          (partitioned by date, clustered by symbol)
      │
  dbt run  ──►  stg_stock_prices  (VIEW — cast, dedupe, validate)
      │
             ──►  mart_daily_metrics  (TABLE — returns, SMAs,
                                       volatility, drawdown, VOO benchmark)
      │
  scripts/  ──►  dashboard/index.html  (static, Chart.js, real BQ data)
```

![Architecture](architecture.png)

Airflow DAG: `daily_market_pipeline` — 8 tasks, runs Mon–Fri at 21:00 UTC.

---

## Project Structure

```
market-intelligence/
├── airflow/
│   ├── dags/daily_market_pipeline.py   # Airflow DAG (8 tasks)
│   ├── src/
│   │   ├── config.py                   # Centralised config (env vars)
│   │   ├── extract.py                  # Alpha Vantage client
│   │   ├── transform.py                # JSON → Parquet, GCS upload
│   │   └── load.py                     # Parquet → BigQuery
│   ├── docker-compose.yml
│   └── requirements.txt
├── dbt/
│   ├── models/
│   │   ├── staging/stg_stock_prices.sql
│   │   └── marts/mart_daily_metrics.sql
│   └── tests/generic/                  # Custom dbt tests
├── dashboard/
│   └── index.html                      # Static dashboard (BigQuery data embedded)
├── scripts/
│   ├── build_dashboard_data.py         # Query BQ → dashboard/data.json
│   └── generate_index.py              # data.json → index.html
├── terraform/                          # GCS, BigQuery, IAM
├── tests/                              # Airflow DAG unit tests
└── Makefile                            # All commands
```

---

## Setup

### Prerequisites

- Docker Desktop (running)
- Terraform ≥ 1.5
- GCP project with billing enabled
- Alpha Vantage API key ([free tier](https://www.alphavantage.co/): 25 req/day)

### 1. Clone and configure

```bash
git clone https://github.com/hanshalili/market-intelligence.git
cd market-intelligence
cp .env.example .env
# Edit .env — fill in: ALPHA_VANTAGE_API_KEY, GCP_PROJECT_ID, GCS_BUCKET_NAME
```

### 2. Provision GCP infrastructure

```bash
make tf-init
make tf-apply
# Creates: GCS bucket, BigQuery dataset, service account + JSON key
```

Place the downloaded service account JSON at `airflow/credentials/service_account.json`.

### 3. Start Airflow

```bash
make airflow-init   # one-time DB setup
make airflow-up     # starts webserver + scheduler
# UI → http://localhost:8080  (admin / admin)
```

### 4. Run the pipeline

```bash
make pipeline-trigger
# Monitor at http://localhost:8080
```

### 5. Refresh the dashboard

```bash
python3 scripts/build_dashboard_data.py   # query BigQuery → data.json
python3 scripts/generate_index.py         # data.json → dashboard/index.html
open dashboard/index.html
```

---

## Usage

| Command | Description |
|---|---|
| `make airflow-up` | Start Airflow (webserver + scheduler) |
| `make airflow-down` | Stop Airflow |
| `make pipeline-trigger` | Manually trigger the DAG |
| `make pipeline-status` | Show recent DAG run states |
| `make dbt-run` | Run dbt models inside the scheduler container |
| `make dbt-test` | Run dbt data quality tests |
| `make tf-apply` | Provision / update GCP infrastructure |

---

## Dashboard

The dashboard (`dashboard/index.html`) is a self-contained static page powered by Chart.js with data embedded directly from BigQuery. No server required — open in any browser.

**Three charts:**
1. **Adjusted Close Price** — 100-day daily close per ticker (dual y-axis)
2. **Cumulative Return %** — normalised from first trading day
3. **Open vs Close** — dashed = open, solid = close, full history

**KPI strip** — per ticker: latest open, close, cumulative return, annualised volatility, drawdown.

---

## dbt Metrics (`mart_daily_metrics`)

| Column | Description |
|---|---|
| `open` | Opening price |
| `adjusted_close` | Closing price (split-adjusted) |
| `daily_return` | % change vs prior trading day |
| `cumulative_return` | % gain from first row per symbol |
| `sma_20 / sma_50 / sma_200` | Simple moving averages |
| `rolling_volatility_20d` | Annualised StdDev of returns (20-day window) |
| `drawdown` | % below trailing 52-week high (always ≤ 0) |
| `voo_daily_return` | VOO benchmark return on same date |
| `excess_return_vs_voo` | Alpha proxy vs VOO |

---

## Resume-Ready Bullets

- Built an end-to-end daily market data pipeline (Alpha Vantage → GCS → BigQuery → dbt) orchestrated with Apache Airflow on Docker, processing 500 rows/day across 5 tickers
- Designed a partitioned, clustered BigQuery schema and dbt analytics mart computing financial metrics (SMAs, volatility, drawdown, excess return vs benchmark)
- Provisioned GCP infrastructure (GCS, BigQuery, IAM, service accounts) with Terraform; all secrets managed via environment variables with zero hardcoded credentials
- Built a static market intelligence dashboard (Chart.js) generated from live BigQuery data, displaying close prices, cumulative returns, and open/close comparison across NVDA, AAPL, GOOGL, TSLA, and VOO
- Implemented idempotent pipeline steps using `WRITE_TRUNCATE` date partitioning, dbt deduplication, and Airflow retry logic with exponential backoff
