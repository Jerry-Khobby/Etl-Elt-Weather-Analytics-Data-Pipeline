# Data Quality Monitoring — How Failures Are Surfaced

## Overview

Catching a bad value inside a `try/except` is not monitoring — it is just error handling. **Monitoring** means making failures visible to a human or an automated system without that person having to actively look. This project surfaces data quality failures through four mechanisms: structured logging with severity levels, Airflow task state (the UI equivalent of a dashboard), exception re-raising (which stops the pipeline and triggers Airflow retries), and a dedicated validation task that acts as an explicit quality gate.

---

## Mechanism 1 — Structured Log Messages at the Right Level

Every quality event that occurs during extraction, transformation, or loading is assigned a log level that reflects its operational severity. The project uses five levels consistently across all pipeline modules.

### Log Level Semantics Used in This Project

| Level | Meaning | Example |
|---|---|---|
| `DEBUG` | Developer detail — verbose, off by default | Raw DataFrame preview, column dtypes, missing value counts |
| `INFO` | Normal operational event | "Extracted 168 records for London", "Loaded 840 rows to fact_weather" |
| `WARNING` | Anomaly that did not stop the pipeline | "Column 'pressure_hpa': 3 out-of-range values set to NaN" |
| `ERROR` | Failure that caused a step to abort | "Missing required field 'hourly' in API response" |
| `EXCEPTION` | Error with full Python stack trace | Any caught exception that is re-raised |

### Quality-Specific Log Events

**In [jobs/etl/extraction.py](../jobs/etl/extraction.py)**:

```python
# INFO — successful extraction
logger.info(f"Extracted {result.record_count} records for {location.name}, {location.country}")

# ERROR — structural problem in API response
logger.error(f"Missing required field '{field}' in API response for {location.name}")

# ERROR — network/HTTP failure
logger.error(f"Connection error for {location.name}: {e}")
logger.error(f"Request timeout for {location.name}: {e}")
logger.error(f"HTTP error for {location.name}: {e}")
logger.error(f"JSON decode error for {location.name}: {e}")
```

**In [jobs/etl/transform.py](../jobs/etl/transform.py)**:

```python
# DEBUG — data profile before transformation (only visible if LOG_LEVEL=DEBUG)
logger.debug(f"Raw data preview:\n{df.head()}")
logger.debug(f"Column dtypes:\n{df.dtypes}")
logger.debug(f"Missing values before transform:\n{df.isnull().sum()}")

# WARNING — quality anomaly: measurement out of range
logger.warning(f"Column '{col}': {count} out-of-range values set to NaN")

# WARNING — rows dropped due to critical nulls
logger.warning(f"Dropped {dropped_count} rows due to null values in critical columns")

# INFO — deduplication outcome
logger.info(f"Removed {duplicate_count} duplicate rows")

# INFO — final record count after all validation
logger.info(f"Transformation complete: {len(df)} records retained")
```

**In [jobs/etl/load.py](../jobs/etl/load.py)**:

```python
# INFO — successful dimension loads
logger.info(f"Loaded/updated {len(df)} records in dim_location")

# INFO — fact table load with skipped-row visibility
logger.info(f"Attempted to insert {len(df)} rows into fact_weather")
# Note: ON CONFLICT DO NOTHING means fewer actual inserts; the gap is implicitly tracked
```

**Why structured messages matter**: Every warning and error message includes the column name, count of affected rows, or location name. A vague log like `"validation failed"` gives no actionable information. A specific log like `"Column 'pressure_hpa': 3 out-of-range values set to NaN for Tokyo on 2026-06-15"` tells you exactly where to look and how many rows were affected.

---

## Mechanism 2 — The Airflow Validation Task as an Explicit Quality Gate

**Where**: [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py)

The DAG has a dedicated `validate_weather_data` task positioned between transform and load:

```
extract_weather_data → transform_weather_data → validate_weather_data → load_weather_data
```

The validation task reads the processed CSV and checks:
1. All required columns are present
2. The DataFrame is non-empty (at least one valid row survived transformation)

```python
def validate_weather_data(**context):
    processed_path = context["ti"].xcom_pull(task_ids="transform_weather_data")
    df = pd.read_csv(processed_path)

    required_columns = [
        "timestamp", "temperature_celsius", "relative_humidity_pct",
        "precipitation_mm", "wind_speed_kmh", "location_name", "location_country"
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Validation failed — missing columns: {missing_columns}")

    if df.empty:
        raise ValueError("Validation failed — transformed DataFrame is empty")

    logger.info(f"Validation passed: {len(df)} rows, all required columns present")
```

**What this surfaces in the Airflow UI**: If validation fails, the `validate_weather_data` task turns **red** in the Airflow graph view. The `load_weather_data` task is never triggered. This makes the quality failure visible:

- **Where** it failed (the validate task, not the load task)
- **When** it failed (the task's start/end timestamp)
- **Why** it failed (the ValueError message appears in the Airflow task log)

Without a separate validation task, a quality problem would either be silently swallowed (no visibility) or cause the load task to fail with a confusing database error. The separate task makes the quality gate explicit in the pipeline graph.

---

## Mechanism 3 — Exception Re-Raising Stops the Pipeline Visibly

Every `except` block in the project follows the same pattern: log the error with full context, then re-raise:

**In [jobs/etl/main.py](../jobs/etl/main.py)**:

```python
try:
    raw_df = self.extractor.extract_all()
except Exception as e:
    logger.exception("Extraction step failed")
    raise
```

**In [jobs/elt/main.py](../jobs/elt/main.py)**:

```python
try:
    self.staging_loader.load(raw_df)
except Exception as e:
    logger.exception("Staging load failed")
    raise
```

`logger.exception()` (not `logger.error()`) logs the full Python stack trace alongside the message. This means the log file contains the exact line of code that failed, the call chain, and the exception type and message — everything needed to debug without reproducing the failure.

Re-raising the exception propagates the failure to Airflow, which marks the task as **failed** and triggers the retry policy. A caught-and-suppressed exception would leave Airflow thinking the task succeeded while data quality was silently compromised.

---

## Mechanism 4 — Container Exit Codes (Docker)

For the standalone ETL and ELT containers in [docker-compose.yml](../docker-compose.yml):

```yaml
etl:
  command: python -m jobs.etl.main
  restart: "no"

elt:
  command: python -m jobs.elt.main
  restart: "no"
```

When the pipeline raises an uncaught exception, Python exits with a non-zero exit code. Docker logs this as a container exit event. `docker ps` will show the container in `Exited (1)` state rather than `Up`. This is visible without opening log files — a monitoring tool watching container health will see the failure.

The `restart: "no"` (also written as `restart: never`) policy ensures that a failing ETL/ELT run does not loop forever. The container fails, stops, and waits for a human or orchestrator to investigate before re-running.

---

## Data Quality Metrics Currently Observable

Without a dedicated monitoring tool, the following quality metrics can be derived from the existing logs and database:

### From the Log File (`logs/pipeline.log`)

| Query | What it tells you |
|---|---|
| `grep "WARNING" pipeline.log \| grep "out-of-range"` | How many measurements were nullified by range validation, per column |
| `grep "Dropped.*rows due to null" pipeline.log` | How many rows were discarded by critical-null filtering |
| `grep "Removed.*duplicate" pipeline.log` | How many rows were deduplicated |
| `grep "ERROR\|EXCEPTION" pipeline.log` | Any extraction or load failures |
| `grep "Transformation complete" pipeline.log` | Record count retained after all validation steps |

### From the Analytics Database

```sql
-- How many hourly readings do we have per location per date?
SELECT l.location_name, d.full_date, COUNT(*) AS hourly_readings
FROM fact_weather f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date     d ON f.date_id     = d.date_id
GROUP BY l.location_name, d.full_date
ORDER BY d.full_date DESC, l.location_name;
-- Expected: 24 per location per day. Gaps indicate missing extractions or dropped rows.

-- How many NULL measurements are in the fact table?
SELECT
    COUNT(*) FILTER (WHERE temperature_celsius IS NULL) AS null_temperature,
    COUNT(*) FILTER (WHERE pressure_hpa        IS NULL) AS null_pressure,
    COUNT(*) FILTER (WHERE uv_index            IS NULL) AS null_uv
FROM fact_weather;
-- Non-zero counts indicate records where range validation nullified a measurement.

-- Are there any date gaps in the data?
SELECT full_date
FROM dim_date d
WHERE NOT EXISTS (
    SELECT 1 FROM fact_weather f WHERE f.date_id = d.date_id
)
ORDER BY full_date;
-- Rows returned indicate dates where no data was loaded (pipeline was down or all rows were dropped).
```

### From the ELT Staging Table

```sql
-- How many rows are still unprocessed? (should be 0 after a successful run)
SELECT COUNT(*) FROM weather_raw_staging WHERE is_processed = FALSE;

-- How many rows were loaded in the last run?
SELECT COUNT(*), MAX(loaded_at)
FROM weather_raw_staging
WHERE loaded_at > NOW() - INTERVAL '1 day';
```

---

## What Is Missing (Conceptual — Not Implemented)

The current monitoring approach is **reactive** — it surfaces failures through logs and Airflow task states that someone must check. A production system would add **proactive monitoring**:

1. **Row count assertions**: Fail the pipeline if fewer than N rows were extracted (e.g., `< 100` rows for a 7-day window across 5 cities is a signal something is wrong with the API).
2. **Null rate thresholds**: Fail or alert if more than 5% of temperature readings are NULL after range validation.
3. **Freshness checks**: Alert if `MAX(extracted_at)` in `fact_weather` is more than 26 hours old.
4. **Schema drift detection**: Compare the set of columns in the API response against the expected `HOURLY_VARIABLES` list and alert on any new or removed fields before they cause failures.
5. **External alerting**: Send a Slack message or email when a pipeline task fails (via Airflow's email operator or a webhook callback).

See [alerting-and-failure-notification.md](alerting-and-failure-notification.md) for the full design of these additions.

---

## Key Files

| File | Monitoring Role |
|---|---|
| [jobs/utils/logger.py](../jobs/utils/logger.py) | Centralized logger with rotating file + console handlers |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | WARNING logs for range violations and null drops |
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | ERROR logs for all API failure types |
| [jobs/etl/main.py](../jobs/etl/main.py) | `logger.exception()` with re-raise at each pipeline step |
| [jobs/elt/main.py](../jobs/elt/main.py) | Same exception + re-raise pattern |
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | `validate_weather_data` task — explicit quality gate in DAG graph |
| [docker-compose.yml](../docker-compose.yml) | Container exit codes + health checks for service visibility |
