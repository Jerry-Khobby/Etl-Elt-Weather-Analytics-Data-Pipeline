# ETL vs ELT — Pattern Selection and Tradeoffs

## Overview

This project implements **both** ETL and ELT pipelines against the same Open-Meteo API and the same analytics star schema. The two patterns serve distinct architectural purposes and each reflects deliberate tradeoffs given the project context.

---

## What Was Built

| | ETL Pipeline | ELT Pipeline |
|---|---|---|
| **Location** | `jobs/etl/` | `jobs/elt/` |
| **Orchestration** | Airflow DAG (`airflow/dags/weather_etl_dag.py`) + standalone container | Standalone container |
| **Extraction** | `WeatherExtractor` (7-day lookback) | Same `WeatherExtractor` (reused) |
| **Transform location** | Python / Pandas (`jobs/etl/transform.py`) | PostgreSQL SQL (`jobs/elt/transform.py`) |
| **Staging** | CSV files in `data/raw/` | `weather_raw_staging` table (port 5434) |
| **Analytics target** | `postgres_analytics` star schema (port 5433) | Same `postgres_analytics` star schema |

---

## ETL — Extract, Transform, Load

### How It Works

The ETL pipeline transforms data **before** it reaches the database. The sequence is:

```
Open-Meteo API
    │
    ▼
WeatherExtractor          (jobs/etl/extraction.py)
    │  → Retries with exponential backoff (3 retries: 2s / 4s / 8s)
    │  → Validates API response schema
    │  → Saves raw DataFrame to data/raw/weather_raw_{date}.csv
    ▼
WeatherDataTransformer    (jobs/etl/transform.py)
    │  → Renames columns (e.g. temperature_2m → temperature_celsius)
    │  → Casts types (string timestamp → datetime, numerics → float)
    │  → Validates ranges (temp: -90 to 60°C, humidity: 0-100%)
    │  → Fills nulls (precipitation, rain, snowfall → 0.0)
    │  → Deduplicates by (timestamp, location_name, location_country)
    │  → Derives fields: date parts, temperature_fahrenheit, period_of_day,
    │    weather_description, weather_category (WMO code mapping)
    ▼
WeatherDataLoader         (jobs/etl/load.py)
    │  → Upserts dim_location, dim_date, dim_time, dim_weather_condition
    │  → Inserts into fact_weather with ON CONFLICT DO NOTHING
    ▼
postgres_analytics (star schema)
```

### When ETL Is Appropriate Here

ETL was chosen for the **Airflow-orchestrated daily path** because:

1. **Application-layer logic is richer**: The WMO weather code mapping (100+ codes → human-readable descriptions and categories), the period-of-day classification, and the temperature range validations are easier to express, test, and maintain in Python than in SQL.
2. **Intermediate audit trail**: The raw CSV saved before transformation gives a recovery point. If the transformation step crashes, the raw file exists for debugging without re-calling the API.
3. **Type safety before touching the DB**: By the time data reaches `WeatherDataLoader`, it has already been validated and typed by Pandas. The database sees only clean, correctly-typed records.
4. **Testability**: Each transformation step is a Python function with unit tests (`tests/etl/test_transform.py`). SQL transformations are harder to unit-test in isolation.

### ETL Tradeoffs

| Tradeoff | Detail |
|---|---|
| **Pro** | Transformation logic testable with pure unit tests (no DB required) |
| **Pro** | Rich Python libraries available (Pandas, NumPy) for complex logic |
| **Pro** | Bad data never reaches the database |
| **Con** | Transformation happens on the application server, not the DB server |
| **Con** | Large datasets must pass through application memory |
| **Con** | CSV staging is not queryable; debugging requires opening files |

---

## ELT — Extract, Load, Transform

### How It Works

The ELT pipeline loads raw data **first**, then transforms it using SQL inside the database. The sequence is:

```
Open-Meteo API
    │
    ▼
extract_raw()             (jobs/elt/extract.py — reuses WeatherExtractor)
    │  → Same extraction, same 7-day lookback, same 5 locations
    ▼
StagingLoader             (jobs/elt/load.py)
    │  → Inserts raw, untyped rows into weather_raw_staging
    │  → All numeric columns stored as NUMERIC (no constraints)
    │  → timestamp stored as VARCHAR(50) — no casting yet
    │  → is_processed = FALSE by default (idempotency marker)
    │  → loaded_at = NOW() (audit timestamp)
    ▼
postgres_staging          (weather_raw_staging table, port 5434)
    │
    ▼
StagingTransformer        (jobs/elt/transform.py)
    │  → SQL SELECT with explicit type casts:
    │    "timestamp"::TIMESTAMP, temperature_2m::FLOAT, etc.
    │  → Derived fields via SQL:
    │    EXTRACT(HOUR/YEAR/MONTH/DAY/DOW/WEEK/QUARTER FROM ...)
    │    (temperature_2m * 9.0 / 5.0 + 32)::NUMERIC (Fahrenheit)
    │    CASE WHEN hour < 6 THEN 'Night' ... END (period_of_day)
    │  → Filters: WHERE is_processed = FALSE
    │  → Returns (transformed_df, staging_row_ids)
    ▼
WeatherDataLoader         (shared with ETL — jobs/etl/load.py)
    │  → Same star schema upserts and ON CONFLICT DO NOTHING
    ▼
StagingLoader.mark_processed(row_ids)
    │  → UPDATE weather_raw_staging SET is_processed = TRUE WHERE id IN (...)
    │  → Only called after analytics load succeeds
    ▼
postgres_analytics (same star schema as ETL)
```

### When ELT Is Appropriate Here

ELT was chosen for the **standalone container path** because:

1. **Database as the transformation engine**: PostgreSQL can perform type casting, `EXTRACT`, `CASE WHEN`, and arithmetic directly on the stored raw data. For simple column-level transformations this is concise and fast.
2. **Persistent staging layer**: The `weather_raw_staging` table accumulates all raw rows. This makes the raw data **queryable** — you can `SELECT` from staging at any time to inspect what was loaded, debug issues, or reprocess rows.
3. **Explicit incremental control**: The `is_processed` flag gives precise, row-level control over which staging rows have been promoted to analytics. This makes the incremental loading story completely explicit.
4. **Database-native scalability**: If the dataset were to grow by 100x, pushing transformation work to the database avoids bottlenecks in Python/Pandas memory.

### ELT Tradeoffs

| Tradeoff | Detail |
|---|---|
| **Pro** | Raw data is persistently queryable in staging table |
| **Pro** | Transformation scales with the database, not the application server |
| **Pro** | Explicit row-level idempotency via `is_processed` flag |
| **Con** | SQL transformation is harder to unit test (requires a live DB or mock) |
| **Con** | Complex logic (WMO code mapping) still requires Python post-processing |
| **Con** | Two databases to manage (staging + analytics) adds operational overhead |
| **Con** | Schema of staging table must accommodate all raw columns |

---

## Why Both Patterns in One Project

The project is intentionally structured as a **learning reference** that demonstrates both approaches side by side. In a production system, you would typically choose one:

- **Choose ETL** when your transformation logic is complex (machine learning, advanced business rules), when you need strong data quality guarantees before database writes, or when your database resources are constrained.
- **Choose ELT** when your database is powerful (modern cloud DWH like Snowflake, BigQuery, Redshift), when you want to preserve raw data in its original form, or when your transformations are primarily column casts and aggregations.

In this project, the **ELT pipeline is closer to production best practices** for a data warehouse because it preserves all raw data in the staging table, giving a complete audit log of everything loaded from the API.

---

## Key Files

| File | Role |
|---|---|
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | ETL extractor with retry logic |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | Python/Pandas transformation |
| [jobs/etl/load.py](../jobs/etl/load.py) | Star schema loader |
| [jobs/etl/main.py](../jobs/etl/main.py) | ETL pipeline orchestrator |
| [jobs/elt/extract.py](../jobs/elt/extract.py) | ELT extractor (reuses ETL) |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | SQL-based transformation |
| [jobs/elt/load.py](../jobs/elt/load.py) | Staging loader + mark_processed |
| [jobs/elt/main.py](../jobs/elt/main.py) | ELT pipeline orchestrator |
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | Airflow ETL DAG |
| [sql/init_staging.sql](../sql/init_staging.sql) | Staging schema DDL |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | Analytics star schema DDL |
