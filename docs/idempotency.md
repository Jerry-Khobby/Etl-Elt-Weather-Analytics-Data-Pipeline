# Idempotency — Safe Reruns Without Duplication or Corruption

## Overview

A pipeline is **idempotent** when running it multiple times with the same input produces exactly the same result as running it once. This matters because pipelines fail and need to be retried — Airflow retries tasks automatically, Docker containers restart, and engineers re-run jobs to recover from errors. Without idempotency, every retry risks inserting duplicate rows, corrupting aggregates, or producing inconsistent state.

This project implements idempotency at multiple layers, with different strategies in the ETL and ELT pipelines.

---

## ETL Idempotency — Database-Level Unique Constraint

The ETL pipeline's idempotency is enforced entirely at the **database layer** via a unique constraint on the fact table combined with `ON CONFLICT DO NOTHING`.

### The Unique Constraint

Defined in [sql/init_analytics.sql](../sql/init_analytics.sql), the fact table has:

```sql
CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id        BIGSERIAL PRIMARY KEY,
    location_id    INTEGER NOT NULL REFERENCES dim_location(location_id),
    date_id        INTEGER NOT NULL REFERENCES dim_date(date_id),
    time_id        INTEGER NOT NULL REFERENCES dim_time(time_id),
    condition_id   INTEGER REFERENCES dim_weather_condition(condition_id),
    -- ... measurement columns ...
    UNIQUE (location_id, date_id, time_id)
);
```

The composite unique constraint `(location_id, date_id, time_id)` means there can be **exactly one row per city per date per hour**. The grain of the fact table enforces this at the schema level — the database will reject any attempt to insert a duplicate even if the application layer sends one.

### The ON CONFLICT Clause

In [jobs/etl/load.py](../jobs/etl/load.py), the fact table insert uses:

```python
INSERT INTO fact_weather (
    location_id, date_id, time_id, condition_id,
    temperature_celsius, ...
) VALUES %s
ON CONFLICT (location_id, date_id, time_id) DO NOTHING;
```

`DO NOTHING` means: if a row with the same (location, date, hour) already exists, silently skip it. The insert does not raise an error and does not modify the existing row.

**What this guarantees**: Running the ETL pipeline twice on the same day inserts each hourly reading once. The second run extracts the same 7-day window from the API, the same rows attempt to insert, and every insert is silently dropped by the conflict clause.

### Dimension Table Idempotency

Dimension tables also use upserts with `ON CONFLICT DO NOTHING`:

```sql
-- dim_location: natural key is (location_name, country)
INSERT INTO dim_location (location_name, country, latitude, longitude)
VALUES (%s, %s, %s, %s)
ON CONFLICT (location_name, country) DO NOTHING;

-- dim_date: natural key is full_date
INSERT INTO dim_date (full_date, year, month, ...)
VALUES (%s, %s, %s, ...)
ON CONFLICT (full_date) DO NOTHING;

-- dim_time: natural key is hour
INSERT INTO dim_time (hour, period_of_day)
VALUES (%s, %s)
ON CONFLICT (hour) DO NOTHING;

-- dim_weather_condition: natural key is weather_code
INSERT INTO dim_weather_condition (weather_code, description, category)
VALUES (%s, %s, %s)
ON CONFLICT (weather_code) DO NOTHING;
```

Each dimension has a natural unique key that prevents duplicate dimension rows regardless of how many times the pipeline runs.

### The DDL Itself Is Idempotent

All schema creation uses `CREATE TABLE IF NOT EXISTS` and `CREATE OR REPLACE VIEW`. Re-running the schema initialization script has no effect if the tables already exist.

---

## ELT Idempotency — Row-Level Processing Flag

The ELT pipeline's idempotency is enforced via an `is_processed` boolean flag on the staging table. This gives **explicit, row-level control** over which data has already been promoted to analytics.

### The is_processed Flag

The staging table definition in [sql/init_staging.sql](../sql/init_staging.sql):

```sql
CREATE TABLE IF NOT EXISTS weather_raw_staging (
    id           BIGSERIAL PRIMARY KEY,
    -- ... raw measurement columns ...
    loaded_at    TIMESTAMPTZ DEFAULT NOW(),
    is_processed BOOLEAN DEFAULT FALSE
);
```

Every row inserted into staging starts with `is_processed = FALSE`.

### The Processing Lifecycle

```
INSERT into staging        →  is_processed = FALSE
                                    │
                                    ▼
SQL transform filters      →  WHERE is_processed = FALSE
                                    │
                                    ▼
Load to analytics          →  ON CONFLICT DO NOTHING (same as ETL)
                                    │
                                    ▼ (only if analytics load succeeds)
mark_processed(row_ids)    →  UPDATE SET is_processed = TRUE
```

In [jobs/elt/load.py](../jobs/elt/load.py):

```python
def fetch_unprocessed_ids(self):
    return pd.read_sql(
        "SELECT id FROM weather_raw_staging WHERE is_processed = FALSE",
        self.engine
    )["id"].tolist()

def mark_processed(self, row_ids):
    with self.engine.begin() as conn:
        conn.execute(
            text("UPDATE weather_raw_staging SET is_processed = TRUE WHERE id = ANY(:ids)"),
            {"ids": row_ids}
        )
```

And in [jobs/elt/transform.py](../jobs/elt/transform.py), the transformation SQL filters only unprocessed rows:

```sql
SELECT
    "timestamp"::TIMESTAMP AS timestamp,
    temperature_2m::FLOAT  AS temperature_celsius,
    -- ... all other columns ...
FROM weather_raw_staging
WHERE is_processed = FALSE
  AND "timestamp" IS NOT NULL
  AND temperature_2m IS NOT NULL
  AND location_name IS NOT NULL;
```

### Transactional Safety

In [jobs/elt/main.py](../jobs/elt/main.py), `mark_processed` is called **only after the analytics load succeeds**:

```python
transformed_df, staging_ids = self.transformer.transform()
if transformed_df.empty:
    return  # nothing to process — exit early

self.loader.load_to_analytics(transformed_df)  # raises on failure
self.staging_loader.mark_processed(staging_ids)  # only reached if above succeeds
```

**What this guarantees**: If the analytics load fails mid-way, `mark_processed` is never called. On the next run, the same unprocessed rows are picked up again. A row is never marked processed unless it has successfully landed in the analytics database.

### Combined Defense

The ELT pipeline also inherits the ETL's fact table `ON CONFLICT DO NOTHING` as a **second layer** of protection. Even if a bug caused `mark_processed` to fail silently, the fact table's unique constraint would prevent duplicate rows from appearing in analytics.

---

## Idempotency Summary by Layer

| Layer | Mechanism | Pipeline |
|---|---|---|
| API extraction | 7-day lookback window (same data re-fetched on retry) | Both |
| Python deduplication | `drop_duplicates(subset=[timestamp, location_name, country])` | ETL |
| Staging filter | `WHERE is_processed = FALSE` | ELT |
| Dimension upsert | `ON CONFLICT (natural_key) DO NOTHING` | Both |
| Fact table upsert | `ON CONFLICT (location_id, date_id, time_id) DO NOTHING` | Both |
| Schema DDL | `CREATE TABLE IF NOT EXISTS` | Both |
| Analytics views | `CREATE OR REPLACE VIEW` | Both |

---

## What Is Not Idempotent

- **Staging table inserts in ELT**: The staging table has no unique constraint. Re-running the ELT pipeline on the same data inserts duplicate raw rows into staging. This is intentional — staging is an append-only log. The `is_processed` flag ensures these duplicates are never promoted to analytics a second time.
- **Raw CSV files in ETL**: Each run writes a new CSV to `data/raw/` with a timestamp suffix. Multiple runs produce multiple files. These files are audit records, not the source of truth.

---

## Key Files

| File | Idempotency Role |
|---|---|
| [sql/init_analytics.sql](../sql/init_analytics.sql) | UNIQUE constraint on fact_weather; IF NOT EXISTS on all DDL |
| [sql/init_staging.sql](../sql/init_staging.sql) | is_processed flag; IF NOT EXISTS on DDL |
| [jobs/etl/load.py](../jobs/etl/load.py) | ON CONFLICT DO NOTHING on all inserts |
| [jobs/elt/load.py](../jobs/elt/load.py) | fetch_unprocessed_ids, mark_processed |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | WHERE is_processed = FALSE filter |
| [jobs/elt/main.py](../jobs/elt/main.py) | mark_processed called only after analytics load succeeds |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | drop_duplicates before DB write |
