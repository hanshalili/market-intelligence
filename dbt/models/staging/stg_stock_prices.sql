/*
  stg_stock_prices.sql — Staging model: lightly-cleaned stock prices

  Source:  stg_stock_prices BigQuery table (loaded by Airflow)
  Purpose: Apply a consistent schema, cast types, and remove obvious bad data
           before the mart layer computes financial metrics.

  Materialisation: VIEW (always reads the latest data; no storage cost)
  Partition / Cluster: inherited from the underlying table via the view

  Why a staging view?
  - Views are cheap: no storage, no refresh lag.
  - They act as a stable contract between the raw load and the marts.
  - Any column renames or casts happen in one place.
*/

WITH source AS (
    SELECT
        date,
        UPPER(TRIM(symbol))                          AS symbol,
        CAST(open           AS FLOAT64)              AS open,
        CAST(high           AS FLOAT64)              AS high,
        CAST(low            AS FLOAT64)              AS low,
        CAST(close          AS FLOAT64)              AS close,
        CAST(adjusted_close AS FLOAT64)              AS adjusted_close,
        CAST(volume         AS INT64)                AS volume,
        ingested_at
    FROM {{ source('market_analytics', 'raw_stock_prices') }}
    WHERE
        -- Drop rows with obviously invalid prices
        adjusted_close IS NOT NULL
        AND adjusted_close > 0
        AND volume IS NOT NULL
        AND volume >= 0
),

deduplicated AS (
    -- In the unlikely case of duplicate loads, keep the most recently ingested row
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date, symbol
        ORDER BY ingested_at DESC
    ) = 1
)

SELECT * FROM deduplicated
