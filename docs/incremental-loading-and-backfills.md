# Incremental Loading & Backfills

## Overview

**Incremental loading** means pulling only new or changed data on each pipeline run rather than re-fetching everything from the beginning. This keeps runs fast, reduces API calls, and prevents unnecessary database churn. **Backfilling** is the complementary operation: replaying historical data into the pipeline when you need to populate a gap, fix corrupt data, or onboard a new dimension.

This project implements incremental loading through two complementary strategies — a **time-window approach** in the ETL pipeline and an **explicit processing flag** in the ELT pipeline.

---

## Incremental Loading in the ETL Pipeline

### The Time Window Strategy

The ETL extractor pulls data from the Open-Meteo API using a **rolling lookback window**. Configured in [jobs/config.py](../jobs/config.py):

```python
LOOKBACK_DAYS = 7
```

In [jobs/etl/extraction.py](../jobs/etl/extraction.py), each extraction call computes the date range dynamically at runtime:

```python
end_date = datetime.now().date()
start_date = end_date - timedelta(days=self.lookback_days)

params = {
    "latitude":  location.latitude,
    "longitude": location.longitude,
    "hourly":    ",".join(HOURLY_VARIABLES),
    "start_date": start_date.isoformat(),
    "end_date":   end_date.isoformat(),
    "timezone":   location.timezone,
}
```

**What this means in practice**: On any given day, the pipeline fetches 7 days of hourly data per location (7 days × 24 hours × 5 locations = 840 rows per run). Because the fact table has `ON CONFLICT (location_id, date_id, time_id) DO NOTHING`, rows that were already loaded on a previous run are silently skipped. Only genuinely new hourly readings — typically the most recent 24 hours — are inserted.

### Why a 7-Day Window Instead of 1 Day

A single-day window would be fragile:
- If the pipeline misses a day (Airflow downtime, API outage), that day's data is permanently lost.
- A 7-day overlap provides a **self-healing buffer**: any day that was missed in the last week is automatically caught by the next successful run.

The cost of this overlap is negligible because `ON CONFLICT DO NOTHING` makes the redundant inserts a no-op.

### Deduplication Before the Database

In [jobs/etl/transform.py](../jobs/etl/transform.py), the transformer deduplicates the in-memory DataFrame before hitting the database:

```python
df = df.drop_duplicates(subset=["timestamp", "location_name", "location_country"])
```

This removes any API-level duplicates (e.g., if the same timestamp appears twice in the response) before the data reaches the loader. Combined with `ON CONFLICT DO NOTHING`, this creates two layers of deduplication.

---

## Incremental Loading in the ELT Pipeline

### The is_processed Flag Strategy

The ELT pipeline implements **true incremental loading** with explicit row-level tracking. The `weather_raw_staging` table is an **append-only log**: new data is always appended, and `is_processed` tracks which rows have already been promoted to analytics.

#### Phase 1: Load Everything to Staging (Append)

In [jobs/elt/load.py](../jobs/elt/load.py), raw data is inserted into staging without any deduplication:

```python
df.to_sql(
    "weather_raw_staging",
    con=self.engine,
    if_exists="append",
    index=False,
    method="multi",
)
```

Staging accumulates every row ever loaded. Each row gets `is_processed = FALSE` and a `loaded_at` timestamp. The staging table grows over time as new data arrives.

#### Phase 2: Transform Only Unprocessed Rows (Incremental)

In [jobs/elt/transform.py](../jobs/elt/transform.py), the SQL transformation explicitly filters to unprocessed rows:

```sql
SELECT
    "timestamp"::TIMESTAMP AS timestamp,
    temperature_2m::FLOAT  AS temperature_celsius,
    -- ... all transformations ...
FROM weather_raw_staging
WHERE is_processed = FALSE
  AND "timestamp" IS NOT NULL
  AND temperature_2m IS NOT NULL
  AND location_name IS NOT NULL;
```

The `WHERE is_processed = FALSE` clause is what makes this incremental. On the first run, all rows qualify. On every subsequent run, only rows added since the last successful run qualify.

#### Phase 3: Mark as Processed (Advance the Watermark)

In [jobs/elt/load.py](../jobs/elt/load.py), after the analytics load succeeds:

```python
def mark_processed(self, row_ids: list[int]) -> None:
    with self.engine.begin() as conn:
        conn.execute(
            text("UPDATE weather_raw_staging SET is_processed = TRUE WHERE id = ANY(:ids)"),
            {"ids": row_ids}
        )
```

The `id` values are captured before transformation and passed through after the analytics write succeeds. This acts as a **watermark** — it advances the boundary between "already processed" and "needs processing."

#### Early Exit When Nothing Is New

In [jobs/elt/main.py](../jobs/elt/main.py):

```python
transformed_df, staging_ids = self.transformer.transform()
if transformed_df.empty:
    logger.info("No new data to process. Exiting.")
    return
```

If all staging rows are already processed, the transformer returns an empty DataFrame and the pipeline exits without touching the analytics database.

---

## Comparison: ETL vs ELT Incremental Strategies

| Aspect | ETL (Time Window) | ELT (is_processed Flag) |
|---|---|---|
| **Mechanism** | Rolling 7-day API lookback | Row-level boolean flag in staging |
| **New data detection** | Implicit: DB dedup silently drops old rows | Explicit: SQL filter selects only new rows |
| **Historical data** | Available via API for the lookback window | Persists in staging table permanently |
| **Recovery on failure** | Next run re-fetches the same window | Next run picks up unprocessed rows |
| **Transparency** | Opaque — hard to tell from DB what was skipped | Transparent — `SELECT count(*) WHERE is_processed = FALSE` |
| **Staging storage** | None (raw CSVs only) | Staging table grows unbounded |

---

## Backfilling Historical Gaps

### What Is a Backfill

A backfill is needed when:
- The pipeline was down and missed days of data
- A bug corrupted the analytics data and it needs to be reloaded
- A new city was added and its historical data must be populated
- The star schema changed and old data must be reloaded with the new structure

### How to Backfill in the ETL Pipeline

The ETL pipeline's backfill lever is the `LOOKBACK_DAYS` configuration in [jobs/config.py](../jobs/config.py). The Open-Meteo API supports historical queries going back years.

To backfill, temporarily change the lookback window:

```python
# In jobs/config.py — temporary backfill configuration
LOOKBACK_DAYS = 90  # pull last 90 days instead of 7
```

Then re-run the pipeline. The fact table's `ON CONFLICT DO NOTHING` ensures:
- Rows that already exist in analytics are silently skipped
- Only genuinely missing rows are inserted
- No data is duplicated

After the backfill completes, restore `LOOKBACK_DAYS = 7`.

**Airflow backfill**: The Airflow DAG has `catchup=False` in [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py), which means missed DAG runs are not automatically retried. To backfill via Airflow, use the CLI:

```bash
airflow dags backfill -s 2026-01-01 -e 2026-06-01 weather_etl_pipeline
```

This triggers the DAG for each missed interval, and `ON CONFLICT DO NOTHING` makes it safe to overlap with existing data.

### How to Backfill in the ELT Pipeline

The ELT backfill strategy uses the staging table directly. Since staging is an append-only log and `is_processed` is the gate to analytics:

**Option 1 — Reset the processed flag**:
```sql
-- Reset specific rows to re-process them
UPDATE weather_raw_staging
SET is_processed = FALSE
WHERE loaded_at BETWEEN '2026-05-01' AND '2026-05-31';
```

The next pipeline run will pick up these rows as if they were new.

**Option 2 — Increase the extraction window**:
Same as ETL — temporarily set `LOOKBACK_DAYS = 90`, run the pipeline to load historical data into staging, and the SQL transformation will promote the unprocessed rows to analytics.

**Option 3 — Direct staging insert** (for emergency backfills):
If the API no longer has the data, historical CSVs from `data/raw/` can be re-inserted into staging with `is_processed = FALSE` and the pipeline will process them normally.

### Backfill Safety

In both pipelines, backfills are safe by design:
- `ON CONFLICT DO NOTHING` on the fact table prevents duplication of already-loaded data
- The `is_processed` flag in ELT prevents re-processing of already-promoted rows
- The idempotent schema DDL means schema initialization can be re-run at any point

---

## Key Files

| File | Incremental Loading Role |
|---|---|
| [jobs/config.py](../jobs/config.py) | `LOOKBACK_DAYS = 7` — controls the extraction window |
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | Computes `start_date` and `end_date` dynamically per run |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | `drop_duplicates` before DB write |
| [jobs/etl/load.py](../jobs/etl/load.py) | `ON CONFLICT DO NOTHING` on all inserts |
| [jobs/elt/load.py](../jobs/elt/load.py) | `fetch_unprocessed_ids`, `mark_processed` |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | `WHERE is_processed = FALSE` in SQL |
| [jobs/elt/main.py](../jobs/elt/main.py) | Early exit when no new data; mark_processed after success |
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | `catchup=False`; retry on failure |
| [sql/init_staging.sql](../sql/init_staging.sql) | `is_processed BOOLEAN DEFAULT FALSE` |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | `UNIQUE (location_id, date_id, time_id)` on fact table |
