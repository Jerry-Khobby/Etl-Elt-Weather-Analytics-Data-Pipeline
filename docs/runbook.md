# Runbook — Pipeline Failure Troubleshooting

## Overview

This runbook covers what to do when the ETL or ELT pipeline fails. Each section identifies the failure symptom, the most likely causes ranked by frequency, the diagnostic commands to run, and the remediation steps. Follow the sections top-to-bottom for a given symptom.

---

## Quick Reference: Failure Triage

| Symptom | Start here |
|---|---|
| Airflow task shows red (failed) | [§1 — Airflow Task Failure](#1--airflow-task-failure) |
| ELT container exited unexpectedly | [§2 — ELT Container Failure](#2--elt-container-failure) |
| No new rows in fact_weather | [§3 — Silent Pipeline — No Data in Analytics](#3--silent-pipeline--no-data-in-analytics) |
| Data looks wrong (bad temperatures, missing fields) | [§4 — Data Quality Issues](#4--data-quality-issues) |
| Database won't accept connections | [§5 — Database Connection Failures](#5--database-connection-failures) |
| Airflow webserver not loading | [§6 — Airflow Infrastructure Issues](#6--airflow-infrastructure-issues) |

---

## §1 — Airflow Task Failure

### Diagnosis

**Step 1**: Open the Airflow UI at `http://localhost:8080`. Navigate to the `weather_etl_pipeline` DAG and click the failed DAG run. Identify which task is red.

**Step 2**: Click the red task → **Logs** tab. Read the full log. The last few lines will contain the exception message and stack trace.

**Step 3**: Identify the task that failed:

| Failed task | Likely cause |
|---|---|
| `extract_weather_data` | API unreachable, rate limited, or response malformed |
| `transform_weather_data` | All rows dropped by validation, or a code bug in transformer |
| `validate_weather_data` | Transformed DataFrame is empty or missing required columns |
| `load_weather_data` | Analytics database unreachable or schema mismatch |

### Remediation by Failed Task

**extract_weather_data fails — API unreachable**:
```bash
# Test API reachability from the ETL container
docker exec -it <etl_container> curl -s \
  "https://api.open-meteo.com/v1/forecast?latitude=5.6037&longitude=-0.1870&hourly=temperature_2m&forecast_days=1" \
  | python -c "import sys, json; print(json.load(sys.stdin).keys())"
```
If this fails: Open-Meteo is down or the container has no outbound internet. Check container network: `docker network inspect weather_network`.

If curl succeeds but Airflow still fails: The error may be a 429 (rate limit). urllib3 retries 3 times. If all retries fail, wait 10 minutes and re-trigger the DAG run manually.

**extract_weather_data fails — JSON decode error**:
The API returned a non-JSON response (HTML error page). Check `https://status.open-meteo.com` for an ongoing outage. Re-trigger the run after the outage is resolved.

**transform_weather_data fails — ValueError: Missing required columns**:
```bash
# Check what columns the raw CSV actually contains
docker exec -it <etl_container> python -c "
import pandas as pd
import glob, os
files = sorted(glob.glob('/app/data/raw/weather_raw_*.csv'))
df = pd.read_csv(files[-1])
print(df.columns.tolist())
"
```
If columns from the API have changed names, update `COLUMN_RENAME_MAP` in `jobs/etl/transform.py`. See [schema-evolution.md](schema-evolution.md).

**validate_weather_data fails — DataFrame is empty**:
All rows were dropped during transformation. Check `logs/pipeline.log` for WARNING lines:
```bash
grep "WARNING" logs/pipeline.log | tail -20
```
Look for: `Dropped N rows due to null values` or `out-of-range values set to NaN`. If every row failed validation, the API may have returned corrupt data. Inspect the raw CSV:
```bash
docker exec -it <etl_container> python -c "
import pandas as pd, glob
f = sorted(glob.glob('/app/data/raw/weather_raw_*.csv'))[-1]
df = pd.read_csv(f)
print(df[['timestamp','temperature_2m','location_name']].head(10))
print(df.isnull().sum())
"
```

**load_weather_data fails — OperationalError**:
The analytics database is unreachable. Jump to [§5 — Database Connection Failures](#5--database-connection-failures).

### Triggering a Manual Re-run

After resolving the root cause, re-trigger the failed DAG run:

**Airflow UI**: Click the DAG run → **Clear** on the failed task (and all downstream tasks) → Airflow will re-run from the cleared task.

**Airflow CLI**:
```bash
docker exec -it airflow-scheduler airflow tasks clear \
  weather_etl_pipeline \
  --start-date 2026-06-18 \
  --end-date 2026-06-18
```

The pipeline is idempotent — re-running it against data already in the database is safe. `ON CONFLICT DO NOTHING` prevents duplicate rows.

---

## §2 — ELT Container Failure

### Diagnosis

```bash
# Check exit code and last log lines
docker ps -a | grep elt
docker logs weather_analytics_pipeline-elt-1 --tail 50
```

If the container exited with code 1, there is an unhandled exception in the ELT pipeline. The last lines of `docker logs` will show the stack trace.

### Common Causes

**Staging DB unreachable**:
```bash
docker exec -it <elt_container> python -c "
from sqlalchemy import create_engine, text
from jobs.config import STAGING_DB_URL
engine = create_engine(STAGING_DB_URL)
with engine.connect() as conn:
    print(conn.execute(text('SELECT 1')).fetchone())
"
```
If this fails: check `docker ps | grep postgres_staging`.

**Analytics DB unreachable**: Same as above but with `ANALYTICS_DB_URL`.

**SQL transformation error**:
```bash
grep "ERROR\|EXCEPTION\|Traceback" logs/pipeline.log | tail -30
```

### Re-running the ELT Pipeline

```bash
docker compose run elt
```

Because unprocessed staging rows have `is_processed = FALSE`, re-running the ELT pipeline will pick up from where it left off. No data from previous partial runs is lost or duplicated.

---

## §3 — Silent Pipeline — No Data in Analytics

The pipeline ran successfully (no red tasks) but `fact_weather` has no new rows for today.

### Diagnosis

**Step 1**: Check if the Airflow DAG actually ran today:
```bash
docker exec -it airflow-scheduler airflow dags list-runs \
  --dag-id weather_etl_pipeline \
  --limit 3
```

**Step 2**: Check how many rows were loaded:
```bash
docker exec -it postgres_analytics psql -U analytics_user -d weather_analytics -c "
SELECT d.full_date, COUNT(*) as rows
FROM fact_weather f
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.full_date >= CURRENT_DATE - 3
GROUP BY d.full_date
ORDER BY d.full_date DESC;
"
```
Expected: ~24 rows × 5 locations = 120 per day (API provides hourly data, 5 cities, 24 hours).

**Step 3**: Check the pipeline log for the record count INFO message:
```bash
grep "Transformation complete\|Attempted to insert\|Pipeline complete" logs/pipeline.log | tail -10
```

**Step 4**: Check if rows are stuck in staging (ELT only):
```bash
docker exec -it postgres_staging psql -U staging_user -d staging_db -c "
SELECT COUNT(*), MAX(loaded_at), SUM(CASE WHEN is_processed THEN 1 ELSE 0 END) as processed
FROM weather_raw_staging;
"
```
If `COUNT(*) > 0` and `processed = 0`, the ELT transform/load step failed silently. Re-run `docker compose run elt`.

### Common Causes of Zero Rows

| Cause | Diagnostic query |
|---|---|
| All rows deduplicated (already loaded) | `grep "Removed.*duplicate" logs/pipeline.log` — if count = total rows, they were all already in the DB |
| All rows dropped by validation | `grep "Dropped.*rows" logs/pipeline.log` — null in critical column for every row |
| DAG ran but Airflow task used a stale code image | `docker images` — check image build date vs last code commit |
| Catchup disabled and DAG run was missed | Start a manual backfill (see below) |

### Manual Backfill for Missing Dates

```bash
# Airflow backfill for a specific date range
docker exec -it airflow-scheduler airflow dags backfill \
  --start-date 2026-06-10 \
  --end-date 2026-06-17 \
  weather_etl_pipeline

# Or increase LOOKBACK_DAYS temporarily and re-run the standalone ETL:
docker exec -it <etl_container> python -c "
import os; os.environ['LOOKBACK_DAYS'] = '30'
from jobs.etl.main import WeatherEtlPipeline
WeatherEtlPipeline().run()
"
```

See [incremental-loading-and-backfills.md](incremental-loading-and-backfills.md) for the full backfill procedure.

---

## §4 — Data Quality Issues

### Unexpected NULL Values in fact_weather

```bash
docker exec -it postgres_analytics psql -U analytics_user -d weather_analytics -c "
SELECT
  l.location_name,
  COUNT(*) FILTER (WHERE f.temperature_celsius IS NULL) AS null_temp,
  COUNT(*) FILTER (WHERE f.pressure_hpa IS NULL)        AS null_pressure,
  COUNT(*) FILTER (WHERE f.uv_index IS NULL)            AS null_uv
FROM fact_weather f
JOIN dim_location l ON f.location_id = l.location_id
WHERE f.extracted_at >= NOW() - INTERVAL '1 day'
GROUP BY l.location_name;
"
```

Non-zero counts indicate that range validation nullified those measurements. Check the pipeline log:
```bash
grep "out-of-range" logs/pipeline.log | tail -20
```

**If nulls are unexpected**: Inspect the raw CSV for the affected date/location to see the original API values:
```bash
python -c "
import pandas as pd
df = pd.read_csv('data/raw/weather_raw_20260618_095056.csv')
print(df[df['location_name'] == 'Tokyo'][['timestamp', 'pressure', 'uv_index']].head(5))
"
```

Compare against the validation range in [jobs/etl/transform.py](../jobs/etl/transform.py). If the API is consistently returning values outside the configured range, the range thresholds may need adjustment.

### Unexpected Row Count (Fewer Rows Than Expected)

```bash
# Expected: 5 locations × 24 hours × 7 days = 840 rows per week
docker exec -it postgres_analytics psql -U analytics_user -d weather_analytics -c "
SELECT l.location_name, d.full_date, COUNT(*) as hourly_readings
FROM fact_weather f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d     ON f.date_id = d.date_id
WHERE d.full_date >= CURRENT_DATE - 7
GROUP BY l.location_name, d.full_date
HAVING COUNT(*) < 24
ORDER BY d.full_date, l.location_name;
"
```

Rows returned indicate dates/locations where fewer than 24 hourly readings exist. Causes:
- Rows dropped by validation (check WARNING logs for that date)
- API returned fewer than 24 hours of data
- Data loaded in an incomplete run that was not retried

To fill gaps: the 7-day lookback window will automatically fill any gaps within the last 7 days on the next pipeline run. For older gaps, run a manual backfill.

### Wrong Values (Data Looks Incorrect)

Trace the value from `fact_weather` back to the raw CSV using the procedure in [data-lineage.md](data-lineage.md). The raw CSV contains the original API values before any transformation. If the raw value is also wrong, the issue is in the API data source — file a report with Open-Meteo.

---

## §5 — Database Connection Failures

### Check Database Container Health

```bash
docker ps | grep postgres
# Should show: Up X minutes (healthy)
```

If a database shows `Exited` or `unhealthy`:
```bash
docker logs postgres_analytics --tail 30
```

Common causes:
- `FATAL: password authentication failed` → `.env` credentials do not match what the database was initialized with
- `FATAL: database does not exist` → the init script did not run, or `ANALYTICS_DB_NAME` env var is wrong
- Port conflict → another process is using port 5433 or 5434

### Restart a Database Service

```bash
docker compose restart postgres_analytics
# Wait for healthy:
docker compose ps postgres_analytics
```

### Manual Connection Test

```bash
# Test analytics DB
docker exec -it postgres_analytics psql \
  -U analytics_user \
  -d weather_analytics \
  -c "SELECT COUNT(*) FROM fact_weather;"

# Test staging DB
docker exec -it postgres_staging psql \
  -U staging_user \
  -d staging_db \
  -c "SELECT COUNT(*), MAX(loaded_at) FROM weather_raw_staging;"
```

### Schema Missing (First Run or Volume Deleted)

If the database is healthy but tables don't exist:
```bash
# Re-run schema initialization via the pipeline
docker compose run etl python -c "
from jobs.etl.load import WeatherDataLoader
WeatherDataLoader().initialize_schema()
print('Schema initialized')
"

# Or apply SQL directly
docker exec -i postgres_analytics psql -U analytics_user -d weather_analytics \
  < sql/init_analytics.sql
docker exec -i postgres_analytics psql -U analytics_user -d weather_analytics \
  < sql/views_analytics.sql
```

---

## §6 — Airflow Infrastructure Issues

### Airflow Webserver Not Loading (localhost:8080)

```bash
docker compose ps airflow-server
docker logs airflow-server --tail 30
```

If the container is not running:
```bash
docker compose up airflow-server -d
```

If the container is running but the UI is not responding, the webserver health check may be failing:
```bash
# Health check endpoint
curl http://localhost:8080/health
# Expected: {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
```

If `metadatabase.status` is unhealthy: the Airflow metadata DB (`postgres_airflow`) is down. Check `docker ps | grep postgres_airflow`.

### DAG Not Appearing in UI

The DAG file may have a syntax error that prevents it from loading:
```bash
docker exec -it airflow-scheduler airflow dags list
docker exec -it airflow-scheduler airflow dags list-import-errors
```

If there's an import error, fix the syntax in `airflow/dags/weather_etl_dag.py`. The scheduler picks up file changes within 30 seconds.

### Scheduler Not Processing DAGs

```bash
docker exec -it airflow-scheduler airflow scheduler --help  # sanity check
docker logs airflow-scheduler --tail 30
```

If the scheduler stopped processing: restart it:
```bash
docker compose restart airflow-scheduler
```

---

## §7 — Full Stack Restart

When in doubt about the state of the entire system:

```bash
# Stop everything cleanly
docker compose down

# Start fresh (preserves database volumes — data not lost)
docker compose up -d

# Wait for all services to be healthy (~60 seconds)
docker compose ps

# Verify pipeline can run
docker compose run etl
```

**To start completely fresh** (destroys all data — use only in development):
```bash
docker compose down -v  # -v removes named volumes including database data
docker compose up -d
```

---

## Useful Diagnostic Commands Reference

```bash
# --- Log inspection ---
tail -f logs/pipeline.log                          # live log stream
grep "ERROR\|WARNING\|EXCEPTION" logs/pipeline.log # filter quality events
grep "Transformation complete" logs/pipeline.log   # record counts per run

# --- Container status ---
docker compose ps                                  # all service health
docker logs <service> --tail 50                    # last 50 lines per service

# --- Database record counts ---
docker exec -it postgres_analytics psql -U analytics_user -d weather_analytics -c \
  "SELECT COUNT(*) FROM fact_weather;"
docker exec -it postgres_staging psql -U staging_user -d staging_db -c \
  "SELECT COUNT(*), SUM(CASE WHEN is_processed THEN 1 ELSE 0 END) FROM weather_raw_staging;"

# --- Airflow task management ---
docker exec -it airflow-scheduler airflow dags list
docker exec -it airflow-scheduler airflow dags list-runs --dag-id weather_etl_pipeline --limit 5
docker exec -it airflow-scheduler airflow tasks list weather_etl_pipeline

# --- Re-run ELT ---
docker compose run elt

# --- Run tests ---
docker compose run test
```

---

## Key Files Referenced in This Runbook

| File | Runbook Reference |
|---|---|
| [logs/pipeline.log](../logs/pipeline.log) | Primary diagnostic log for all pipeline runs |
| [docker-compose.yml](../docker-compose.yml) | Service definitions, health checks, restart policies |
| [jobs/config.py](../jobs/config.py) | `LOOKBACK_DAYS` — adjust for manual backfill |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | `VALID_RANGES` — adjust thresholds if API values change |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | Re-apply if schema is missing |
| [sql/views_analytics.sql](../sql/views_analytics.sql) | Re-apply if views are missing |
| [data/raw/](../data/raw/) | Raw CSV files — ETL lineage anchor for value tracing |
