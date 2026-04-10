# MarketIntel

**A production-grade daily market data pipeline and dashboard — Alpha Vantage → GCS → BigQuery → dbt → Chart.js.**

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?style=flat-square&logo=dbt&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?style=flat-square&logo=google-cloud&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-1.5+-623CE4?style=flat-square&logo=terraform&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

---

## What It Does

MarketIntel ingests daily OHLCV price data for **NVDA, AAPL, GOOGL, TSLA, and VOO** from the Alpha Vantage API, stores raw and curated files in Google Cloud Storage, loads them into a partitioned BigQuery warehouse, and runs dbt transformations to compute financial metrics. A static dashboard generated from live BigQuery data visualises close prices, cumulative returns, and open/close comparisons — no server required.

```
Alpha Vantage API
      │  TIME_SERIES_DAILY (5 symbols × 100 days)
      ▼
  extract.py  ──►  GCS  raw/       (JSON, partitioned by symbol/date)
      │
  transform.py ──►  GCS  curated/  (Parquet, Snappy-compressed)
      │
  load.py  ──►  BigQuery: market_analytics.raw_stock_prices
                          (partitioned by date, clustered by symbol)
      │
  dbt run  ──►  stg_stock_prices      (VIEW — cast, dedupe, validate)
             ──►  mart_daily_metrics   (TABLE — returns, SMAs,
                                        volatility, drawdown, VOO benchmark)
      │
  scripts/  ──►  dashboard/index.html  (static, Chart.js, BQ data embedded)
```

Airflow DAG `daily_market_pipeline` — 8 tasks, scheduled Mon–Fri at **21:00 UTC** (30 min after NYSE close).

---

## Why This Architecture

Most market dashboards are expensive SaaS tools or one-off scripts that break. MarketIntel is a fully orchestrated, idempotent ELT pipeline built with the same toolchain used in production data engineering — demonstrating end-to-end ownership from raw API ingestion to a queryable analytics mart and a rendered dashboard.

Key decisions:
- **GCS as a landing zone** — raw JSON is preserved before any transformation; Parquet in `curated/` decouples ingestion from loading
- **BigQuery partitioned by date, clustered by symbol** — query cost scales with date range, not full table scans
- **dbt staging → mart pattern** — staging view casts and deduplicates; mart materialises financial metrics once per day
- **Static dashboard** — no runtime server needed; BigQuery data is embedded at generation time, making it trivially hostable anywhere

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Orchestration | Apache Airflow | 2.8 | Scheduled DAG, retries, task dependencies |
| Extraction | Python + Alpha Vantage API | 3.11 | Daily OHLCV ingestion |
| Storage | Google Cloud Storage | — | Raw JSON + Parquet landing zone |
| Warehouse | BigQuery | — | Partitioned, clustered OHLCV store |
| Transformation | dbt | 1.x | Staging view + analytics mart |
| Infrastructure | Terraform | ≥ 1.5 | GCS, BigQuery, IAM, service accounts |
| Dashboard | Chart.js + HTML/CSS | — | Static, browser-only market charts |
| Containerisation | Docker Compose | — | Airflow webserver + scheduler |

---

## dbt Metrics — `mart_daily_metrics`

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker (NVDA, AAPL, etc.) |
| `date` | DATE | Trading date |
| `open` | FLOAT | Opening price |
| `adjusted_close` | FLOAT | Split-adjusted closing price |
| `daily_return` | FLOAT | % change vs prior trading day |
| `cumulative_return` | FLOAT | % gain from first row per symbol |
| `sma_20 / sma_50 / sma_200` | FLOAT | Simple moving averages |
| `rolling_volatility_20d` | FLOAT | Annualised StdDev of returns (20-day window) |
| `drawdown` | FLOAT | % below trailing 52-week high (always ≤ 0) |
| `voo_daily_return` | FLOAT | VOO benchmark return on same date |
| `excess_return_vs_voo` | FLOAT | Alpha proxy vs VOO benchmark |

---

## Project Structure

```
market-intel/
├── airflow/
│   ├── dags/daily_market_pipeline.py   # 8-task Airflow DAG, Mon–Fri 21:00 UTC
│   ├── src/
│   │   ├── config.py                   # Centralised config (env vars only)
│   │   ├── extract.py                  # Alpha Vantage client → GCS raw/
│   │   ├── transform.py                # JSON → Parquet, upload to GCS curated/
│   │   └── load.py                     # Parquet → BigQuery (WRITE_TRUNCATE)
│   ├── docker-compose.yml
│   └── requirements.txt
├── dbt/
│   ├── models/
│   │   ├── staging/stg_stock_prices.sql   # Cast, dedupe, validate
│   │   └── marts/mart_daily_metrics.sql   # Returns, SMAs, volatility, drawdown
│   └── tests/generic/                     # Custom dbt data quality tests
├── dashboard/
│   └── index.html                         # Static Chart.js dashboard (BQ data embedded)
├── scripts/
│   ├── build_dashboard_data.py            # Query BigQuery → dashboard/data.json
│   └── generate_index.py                  # data.json → index.html
├── terraform/                             # GCS bucket, BigQuery dataset, IAM
├── tests/                                 # Airflow DAG unit tests
└── Makefile                               # All commands
```

---

## Quickstart

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | latest | Must be running |
| Terraform | ≥ 1.5 | `brew install terraform` |
| GCP project | — | Billing must be enabled |
| Alpha Vantage API key | — | Free tier: 25 req/day |

### Credentials

| Variable | Where to get it | Example |
|---|---|---|
| `ALPHA_VANTAGE_API_KEY` | [alphavantage.co](https://www.alphavantage.co/) | `ABC123XYZ` |
| `GCP_PROJECT_ID` | GCP Console → Project selector | `my-project-123` |
| `GCS_BUCKET_NAME` | After `make tf-apply` | `marketintel-raw-abc123` |
| `BIGQUERY_DATASET` | After `make tf-apply` | `market_analytics` |
| Service account JSON | After `make tf-apply` | `airflow/credentials/service_account.json` |

### Steps

**1. Clone and configure**
```bash
git clone https://github.com/hanshalili/MarketIntel.git
cd market-intel
cp .env.example .env
# Edit .env — fill in API key, GCP project, bucket name
```

**2. Provision GCP infrastructure**
```bash
make tf-init
make tf-apply
# Creates: GCS bucket, BigQuery dataset, service account + downloads JSON key
```
Place the downloaded key at `airflow/credentials/service_account.json`.

**3. Start Airflow**
```bash
make airflow-init   # One-time DB setup
make airflow-up     # Starts webserver + scheduler
# UI → http://localhost:8080  (admin / admin)
```

**4. Trigger the pipeline**
```bash
make pipeline-trigger
# Monitor run at http://localhost:8080
```

**5. Generate the dashboard**
```bash
python3 scripts/build_dashboard_data.py   # BigQuery → dashboard/data.json
python3 scripts/generate_index.py         # data.json → dashboard/index.html
open dashboard/index.html
```

---

## Commands

| Command | Description |
|---|---|
| `make airflow-up` | Start Airflow (webserver + scheduler) |
| `make airflow-down` | Stop Airflow |
| `make pipeline-trigger` | Manually trigger the DAG |
| `make pipeline-status` | Show recent DAG run states |
| `make dbt-run` | Run dbt models inside the scheduler container |
| `make dbt-test` | Run dbt data quality tests |
| `make tf-init` | Initialise Terraform |
| `make tf-apply` | Provision / update GCP infrastructure |

---

## Dashboard

`dashboard/index.html` is a self-contained static page powered by Chart.js — open it in any browser, no server required.

**Charts:**
1. **Adjusted Close Price** — 100-day daily close per ticker (dual y-axis for scale differences)
2. **Cumulative Return %** — normalised from first trading day per symbol
3. **Open vs Close** — dashed = open, solid = close, full 100-day history

**KPI strip** — per ticker: latest open, close, cumulative return %, annualised volatility, drawdown.

---

## Resume Bullets

- Built an end-to-end daily market data pipeline (Alpha Vantage → GCS → BigQuery → dbt) orchestrated with Apache Airflow on Docker Compose, processing 500 rows/day across 5 tickers with full idempotency via `WRITE_TRUNCATE` partitioning
- Designed a partitioned (by date), clustered (by symbol) BigQuery schema and dbt analytics mart computing SMA-20/50/200, 20-day annualised volatility, drawdown, and excess return vs VOO benchmark
- Provisioned GCP infrastructure (GCS, BigQuery, IAM, service accounts) with Terraform following least-privilege principles; all secrets managed via environment variables with zero hardcoded credentials
- Built a static market intelligence dashboard (Chart.js) generated from live BigQuery data, displaying close prices, cumulative returns, and open/close comparisons across NVDA, AAPL, GOOGL, TSLA, and VOO
- Implemented idempotent pipeline steps using `WRITE_TRUNCATE` date partitioning, dbt deduplication logic, and Airflow retry with exponential backoff

---

## License

MIT — see [LICENSE](LICENSE)
