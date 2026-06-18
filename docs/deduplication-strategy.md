# Deduplication Strategy

## Overview

Duplicates in a weather pipeline come from two sources: the **API** can return overlapping time windows across runs, and the **loading logic** can be re-run on already-loaded data (retries, backfills). The project applies a layered deduplication strategy — Python-level deduplication before writing and database-level constraint enforcement at write time — so that duplicates are impossible in the analytics layer regardless of how the pipeline runs.

---

## Where Duplicates Come From

### Source 1 — Overlapping Extraction Windows

The ETL and ELT pipelines both use a 7-day rolling lookback window (`LOOKBACK_DAYS = 7` in [jobs/config.py](../jobs/config.py)). On consecutive days, the windows overlap by 6 days:

```
Monday run:   [Mon Jun 9 → Mon Jun 16]   (7 days)
Tuesday run:  [Tue Jun 10 → Tue Jun 17]  (7 days)
Overlap:      [Tue Jun 10 → Mon Jun 16]  (6 days of duplicate rows)
```

Every hourly reading for June 10–16 is fetched again on Tuesday's run. Without deduplication, 6 × 24 × 5 = 720 duplicate rows would be inserted per pipeline run.

### Source 2 — Airflow Retries and Manual Re-runs

The Airflow DAG has `retries=1` per task. If the load task fails and Airflow retries it, the same transformed data is passed to the loader a second time. Without deduplication, every row already written on the first attempt would be written again on the retry.

### Source 3 — API Response Duplicates

In rare cases, the Open-Meteo API may return the same timestamp twice within a single response (e.g., at a DST boundary or due to an API bug). These are intra-response duplicates that exist before any cross-run consideration.

---

## Layer 1 — Python-Level Deduplication (ETL Transformer)

**Where**: [jobs/etl/transform.py](../jobs/etl/transform.py)

After type casting and before validation, the transformer removes duplicates within the in-memory DataFrame:

```python
DEDUP_SUBSET = ["timestamp", "location_name", "location_country"]

before_count = len(df)
df = df.drop_duplicates(subset=DEDUP_SUBSET, keep="first")
after_count = len(df)

if before_count > after_count:
    logger.info(f"Removed {before_count - after_count} duplicate rows")
```

**Deduplication key**: `(timestamp, location_name, location_country)`

- `timestamp`: The hourly reading time — one row per hour per city
- `location_name`: City name (e.g., "London")
- `location_country`: Country (e.g., "United Kingdom") — disambiguates cities with the same name

**`keep="first"`**: When duplicates exist, the first occurrence is retained and all others are dropped. Order within the DataFrame follows the API response order.

**Scope**: This handles **intra-batch** duplicates — rows that are identical within a single pipeline run's extracted data. It runs once per full extraction across all locations before any database write.

**What it does not handle**: Cross-run duplicates (the same hourly reading from a previous run already in the database). That is handled by Layer 2.

---

## Layer 2 — Database Constraint Enforcement (Both Pipelines)

**Where**: [sql/init_analytics.sql](../sql/init_analytics.sql) and [jobs/etl/load.py](../jobs/etl/load.py)

The `fact_weather` table has a composite unique constraint that makes cross-run duplicates impossible:

```sql
CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id     BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES dim_location(location_id),
    date_id     INTEGER NOT NULL REFERENCES dim_date(date_id),
    time_id     INTEGER NOT NULL REFERENCES dim_time(time_id),
    -- ... measurements ...
    UNIQUE (location_id, date_id, time_id)
);
```

The `UNIQUE (location_id, date_id, time_id)` constraint mirrors the business rule: there can be exactly one weather reading per city per hour per day. The database itself enforces this invariant — no application-layer bug can violate it.

The insert uses `ON CONFLICT DO NOTHING`:

```python
INSERT INTO fact_weather (
    location_id, date_id, time_id, condition_id,
    temperature_celsius, temperature_fahrenheit, ...
) VALUES %s
ON CONFLICT (location_id, date_id, time_id) DO NOTHING;
```

`DO NOTHING` means: if a row with the same (location, date, hour) already exists, skip it silently. No error is raised, no existing data is modified, and the insert count returned by the database reflects only the rows that were actually new.

**Scope**: This handles **cross-run duplicates** — a row from the current run that was already loaded on a previous run. The database rejects it without the application needing to check first.

### Dimension Table Deduplication

The same `ON CONFLICT DO NOTHING` pattern applies to all dimension tables:

```python
# dim_location — natural key: (location_name, country)
INSERT INTO dim_location (location_name, country, latitude, longitude)
VALUES (%s, %s, %s, %s)
ON CONFLICT (location_name, country) DO NOTHING;

# dim_date — natural key: full_date
INSERT INTO dim_date (full_date, year, month, ...)
VALUES (%s, ...)
ON CONFLICT (full_date) DO NOTHING;

# dim_time — natural key: hour
INSERT INTO dim_time (hour, period_of_day)
VALUES (%s, %s)
ON CONFLICT (hour) DO NOTHING;

# dim_weather_condition — natural key: weather_code
INSERT INTO dim_weather_condition (weather_code, description, category)
VALUES (%s, %s, %s)
ON CONFLICT (weather_code) DO NOTHING;
```

Dimension rows are written once and then silently skipped on all subsequent runs. The 24 rows in `dim_time` and the 5 rows in `dim_location` are inserted on the first pipeline run and never re-inserted.

---

## Layer 3 — ELT Staging Deduplication (is_processed Flag)

**Where**: [jobs/elt/load.py](../jobs/elt/load.py) and [jobs/elt/transform.py](../jobs/elt/transform.py)

The ELT staging table intentionally does **not** have a unique constraint. Staging is an append-only log — re-running the pipeline adds new rows with `is_processed = FALSE` alongside old rows with `is_processed = TRUE`.

The deduplication at the ELT staging layer is handled by the processing flag:

```sql
-- SQL transformation only reads unprocessed rows
WHERE is_processed = FALSE
  AND "timestamp" IS NOT NULL
  AND temperature_2m IS NOT NULL
  AND location_name IS NOT NULL
```

A staging row that has already been promoted to analytics (`is_processed = TRUE`) will never be selected by the transformation query, so it can never be inserted into analytics a second time.

After transformation and analytics load:

```python
def mark_processed(self, row_ids: list[int]) -> None:
    with self.engine.begin() as conn:
        conn.execute(
            text("UPDATE weather_raw_staging SET is_processed = TRUE WHERE id = ANY(:ids)"),
            {"ids": row_ids}
        )
```

The `id` values are the BIGSERIAL primary keys of the staging rows. Using explicit row IDs rather than a timestamp or status query ensures that exactly the rows that were transformed are marked — no more, no less.

**What this means for duplicates in staging**: If the ELT pipeline is re-run before `mark_processed` is called (e.g., the analytics load succeeded but the mark-processed step crashed), the same staging rows will be selected again on the next run and re-inserted into analytics — but `ON CONFLICT DO NOTHING` on the fact table will silently drop the duplicates. Both layers work together.

---

## Deduplication Decision Summary

| Duplicate Source | Layer That Handles It | Mechanism |
|---|---|---|
| Intra-response API duplicates (same timestamp twice in one response) | ETL Python transformer | `drop_duplicates(subset=[timestamp, location, country])` |
| Overlapping 7-day extraction windows (previous run's data re-extracted) | Database constraint | `ON CONFLICT (location_id, date_id, time_id) DO NOTHING` |
| Airflow task retry (same transformed data sent to loader again) | Database constraint | `ON CONFLICT DO NOTHING` |
| ELT staging re-run (staging rows already promoted re-selected) | ELT processing flag | `WHERE is_processed = FALSE` + `mark_processed()` |
| Dimension row re-insertion (same city/date/hour dimension on every run) | Database constraint | `ON CONFLICT (natural_key) DO NOTHING` on each dimension |

---

## What Deduplication Does Not Do

**It does not modify existing data**. `ON CONFLICT DO NOTHING` skips the insert silently — it does not update the existing row with new values. If the API corrects a previously-returned temperature for a past hour, the corrected value will not overwrite what is already in `fact_weather`. The original value is kept permanently. Updating existing fact rows requires a deliberate `UPDATE` statement, which is not part of the current pipeline.

**It does not deduplicate at the ELT staging level**. The staging table accumulates duplicate raw rows across runs. This is intentional — staging is a historical log, and the `is_processed` flag is the mechanism that prevents these duplicates from propagating to analytics.

---

## Key Files

| File | Deduplication Role |
|---|---|
| [jobs/etl/transform.py](../jobs/etl/transform.py) | `drop_duplicates(subset=DEDUP_SUBSET)` — intra-batch dedup |
| [jobs/etl/load.py](../jobs/etl/load.py) | `ON CONFLICT DO NOTHING` on fact and all dimension inserts |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | `UNIQUE (location_id, date_id, time_id)` on fact_weather |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | `WHERE is_processed = FALSE` — ELT staging dedup |
| [jobs/elt/load.py](../jobs/elt/load.py) | `mark_processed(row_ids)` — advances the processed boundary |
| [jobs/config.py](../jobs/config.py) | `LOOKBACK_DAYS = 7` — controls the extent of window overlap |
