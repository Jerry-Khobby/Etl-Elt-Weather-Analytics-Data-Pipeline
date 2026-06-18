# Orchestration & Scheduling — Airflow DAG Structure, Task Dependencies, Retry Policy, and Timeout Config

## Overview

The ETL pipeline is orchestrated by Apache Airflow 2.9.2, running as a set of Docker services (`airflow-server`, `airflow-scheduler`) defined in [docker-compose.yml](../docker-compose.yml). The DAG defines the sequence of pipeline steps, their dependencies, retry behaviour, and timing. The ELT pipeline runs as a standalone container outside Airflow and relies on Docker's own lifecycle management.

---

## The Airflow DAG

**File**: [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py)

### DAG-Level Configuration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner":          "weather_pipeline",
    "start_date":     datetime(2026, 1, 1),
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "depends_on_past": False,
}

dag = DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline to extract, transform, and load weather data",
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "etl", "analytics"],
)
```

| Parameter | Value | Meaning |
|---|---|---|
| `dag_id` | `weather_etl_pipeline` | Unique identifier in the Airflow UI |
| `start_date` | `2026-01-01` | Airflow will not schedule runs before this date |
| `schedule_interval` | `@daily` | Runs once per day, at midnight UTC |
| `catchup` | `False` | Missed runs (while the scheduler was down) are **not** automatically backfilled |
| `retries` | `1` | Each task gets one automatic retry if it fails |
| `retry_delay` | `5 minutes` | Wait time between the initial failure and the retry attempt |
| `depends_on_past` | `False` | Today's run does not wait for yesterday's run to succeed |

**Why `catchup=False`**: The extraction window (`LOOKBACK_DAYS=7`) already covers any gaps from missed days. Enabling catchup would trigger duplicate DAG runs for every missed day, all fetching the same 7-day window and all competing to insert the same rows (safely handled by `ON CONFLICT DO NOTHING`, but wasteful). Manual backfills are performed using the `airflow dags backfill` CLI when needed.

---

## Task Graph

The DAG defines four tasks with a strict linear dependency chain:

```
extract_weather_data
        │
        │ (XCom: raw_csv_path)
        ▼
transform_weather_data
        │
        │ (XCom: processed_csv_path)
        ▼
validate_weather_data
        │
        │ (no output — raises ValueError on failure)
        ▼
load_weather_data
```

### Task 1 — extract_weather_data

```python
extract_task = PythonOperator(
    task_id="extract_weather_data",
    python_callable=extract_weather_data,
    dag=dag,
)
```

**What it does**:
- Calls `WeatherExtractor.extract_all()` for all 5 locations
- Each location fetches 7 days of hourly data from the Open-Meteo API
- Saves the combined raw DataFrame to `data/raw/weather_raw_{date}.csv`
- Returns the file path via XCom (`ti.xcom_push(key="raw_path", value=str(raw_path))`)

**Failure modes handled**:
- `ConnectionError`, `Timeout`, `HTTPError`, `JSONDecodeError` — all caught, logged, re-raised
- urllib3 retries (3×) run before the exception reaches Airflow
- If all urllib3 retries are exhausted, Airflow marks the task `failed` and waits 5 minutes before the task-level retry

**XCom output**: Raw CSV file path (string)

### Task 2 — transform_weather_data

```python
transform_task = PythonOperator(
    task_id="transform_weather_data",
    python_callable=transform_weather_data,
    dag=dag,
)
```

**What it does**:
- Reads the raw CSV path from XCom (`ti.xcom_pull(task_ids="extract_weather_data")`)
- Loads the CSV into a DataFrame
- Applies `WeatherDataTransformer`: column renames, type casts, null handling, range validation, deduplication, derived fields
- Saves the processed DataFrame to `data/processed/weather_transformed_{date}.csv`
- Returns the processed file path via XCom

**Failure modes handled**:
- Missing required output columns → `ValueError` → task fails
- All transformation exceptions are caught, logged with `logger.exception()`, and re-raised

**XCom output**: Processed CSV file path (string)

### Task 3 — validate_weather_data

```python
validate_task = PythonOperator(
    task_id="validate_weather_data",
    python_callable=validate_weather_data,
    dag=dag,
)
```

**What it does**:
- Reads the processed CSV path from XCom (`ti.xcom_pull(task_ids="transform_weather_data")`)
- Loads the processed CSV into a DataFrame
- Checks: all required columns present AND DataFrame is non-empty
- If either check fails, raises `ValueError` — the load task never runs

**Why this task exists as a separate step** (not merged into transform or load):
- In the Airflow graph, a dedicated validate task makes quality failures visible as their own task failure, distinct from transform or load failures
- An operator looking at the DAG can immediately see *where* the pipeline stopped and why
- The load task is only reached if validation explicitly passes — it is a green/red gate in the graph

**XCom output**: None (side-effect only — raises on failure)

### Task 4 — load_weather_data

```python
load_task = PythonOperator(
    task_id="load_weather_data",
    python_callable=load_weather_data,
    dag=dag,
)
```

**What it does**:
- Reads the processed CSV path from XCom
- Loads the CSV and restores types lost during CSV serialization (timestamps parsed from string, booleans corrected)
- Calls `WeatherDataLoader.initialize_schema()` — creates tables if they do not exist
- Calls `WeatherDataLoader.load(df)` — upserts dimension tables, inserts fact rows with `ON CONFLICT DO NOTHING`

**Why type restoration is needed**: CSV serialization flattens types. A `datetime` column becomes a string; a boolean becomes `True`/`False` strings. The load task explicitly re-parses these before inserting:

```python
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["date"] = pd.to_datetime(df["date"]).dt.date
df["is_day"] = df["is_day"].astype(bool)
df["is_weekend"] = df["is_weekend"].astype(bool)
```

**Failure modes handled**:
- Database `OperationalError` → caught, logged, re-raised → Airflow retries after 5 minutes
- Connection stale → `pool_pre_ping=True` silently reconnects

### Task Dependencies

```python
extract_task >> transform_task >> validate_task >> load_task
```

The `>>` operator sets Airflow task dependencies. A task only starts when all upstream tasks have succeeded. If `validate_task` fails, `load_task` is never triggered (status: `upstream_failed`).

---

## Retry Policy

The retry policy is configured at the `default_args` level and applies to all four tasks:

```python
"retries":     1,
"retry_delay": timedelta(minutes=5),
```

**Effective retry sequence for a failing task**:

```
T+0:00  Task attempt 1 runs → fails
T+0:xx  Task marked FAILED (short delay for Airflow state update)
T+5:00  Task attempt 2 (retry 1) runs → succeeds or fails permanently
T+5:xx  If failed permanently: task status = FAILED; downstream tasks = upstream_failed
```

**Why 1 retry with 5-minute delay**:
- One retry is sufficient for transient failures (brief network blip, temporary DB overload)
- The 5-minute delay gives the Open-Meteo API time to recover from rate limiting (429) or a brief outage (503)
- More retries would mask persistent failures and delay alerting

**urllib3 retries vs Airflow retries**: These are two independent retry layers. The urllib3 retry (3 attempts, 2–8 second delays) handles sub-second to sub-minute API transients within a single task execution. The Airflow retry (1 attempt, 5-minute delay) handles failures that persist beyond what urllib3 can resolve.

---

## Infrastructure Services and Health Checks

### Docker Compose Service Orchestration

The Airflow stack is defined in [docker-compose.yml](../docker-compose.yml):

```yaml
services:
  airflow-init:
    # One-shot: runs db init and creates admin user
    command: >
      bash -c "airflow db migrate && airflow users create ..."
    restart: "no"

  airflow-server:
    command: airflow webserver
    ports: ["8080:8080"]
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  airflow-scheduler:
    command: airflow scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    restart: unless-stopped
```

**Service startup sequence** enforced by `depends_on`:
1. `postgres_airflow` becomes healthy (pg_isready passes)
2. `airflow-init` completes (`service_completed_successfully`)
3. `airflow-server` and `airflow-scheduler` start in parallel

The `service_completed_successfully` condition means Airflow components only start after the database migration has finished — preventing the "connection refused" race condition that occurs when Airflow tries to connect before the DB schema exists.

### Database Health Checks

```yaml
postgres_analytics:
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U ${ANALYTICS_DB_USER} -d ${ANALYTICS_DB_NAME}"]
    interval: 10s
    timeout: 5s
    retries: 5
```

All three PostgreSQL services (`postgres_airflow`, `postgres_staging`, `postgres_analytics`) have identical health check configurations. The ETL and ELT containers declare `depends_on` with `condition: service_healthy` — they will not start until the database passes `pg_isready`.

---

## Scheduling Decisions

### Why @daily (Midnight UTC)?

- **Open-Meteo API**: Provides hourly forecasts and historical data updated throughout the day. A daily run at midnight captures a complete day of hourly readings plus the 7-day lookback buffer.
- **Simplicity**: `@daily` is equivalent to `0 0 * * *` in cron syntax. It is the most common schedule for data warehouse ingestion jobs.
- **UTC timezone**: Airflow runs in UTC by default. All five monitored cities (Accra, London, New York, Tokyo, Berlin) are in different time zones, but the extraction window uses UTC-based dates. The `timezone` parameter per location in [jobs/config.py](../jobs/config.py) ensures the API returns data in the local time zone, which is then normalized to UTC timestamps during transformation.

### No SLA Configuration (Current Gap)

The DAG does not currently set SLAs (Service Level Agreements). An SLA in Airflow triggers a callback (email, Slack) if a task does not complete within a specified duration. Adding an SLA:

```python
default_args = {
    ...
    "sla": timedelta(hours=2),  # alert if any task takes > 2 hours
}
```

would cause Airflow to call the `sla_miss_callback` function if the full DAG has not completed within 2 hours of its scheduled start. This is the recommended next addition for production readiness.

---

## The ELT Pipeline — Outside Airflow

The ELT pipeline is not managed by the Airflow DAG. It runs as a standalone Docker container:

```yaml
elt:
  build: .
  command: python -m jobs.elt.main
  restart: "no"
  depends_on:
    postgres_staging:
      condition: service_healthy
    postgres_analytics:
      condition: service_healthy
```

**Lifecycle**: The container runs once, completes (or fails), and exits. `restart: "no"` means it does not restart automatically. To re-run the ELT pipeline, the container must be manually restarted (`docker compose run elt`) or scheduled externally (a cron job, a second Airflow DAG, or a Kubernetes CronJob).

This is the primary operational gap of the current ELT setup compared to the Airflow-managed ETL: there is no built-in retry, no scheduling, and no UI visibility for ELT runs.

---

## Key Files

| File | Orchestration Role |
|---|---|
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | Full DAG definition: tasks, dependencies, retry policy, schedule |
| [docker-compose.yml](../docker-compose.yml) | Airflow service stack, health checks, startup ordering, ELT container |
| [jobs/config.py](../jobs/config.py) | `LOOKBACK_DAYS`, API config used by extraction tasks |
| [jobs/etl/main.py](../jobs/etl/main.py) | `WeatherEtlPipeline.run()` — called by Airflow PythonOperators |
