# Error Handling & Retries — API Timeouts, Malformed Responses, DB Connection Drops

## Overview

The pipeline faces three categories of transient failures: network problems talking to the Open-Meteo API, malformed or unexpected API responses, and database connection issues. Each category is handled with a different strategy matched to the nature of the failure. The guiding principle throughout is: **log with full context, re-raise the exception** — never silently swallow an error where doing so would leave the pipeline in an inconsistent state.

---

## Category 1 — API Failures

### Retry Strategy

**Where**: [jobs/etl/extraction.py](../jobs/etl/extraction.py) — `WeatherExtractor._build_session()`

The HTTP client is configured with automatic retries using `urllib3.util.retry.Retry` mounted on a `requests.adapters.HTTPAdapter`:

```python
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

retry_strategy = Retry(
    total=MAX_RETRIES,                   # 3 total attempts
    backoff_factor=RETRY_BACKOFF_FACTOR, # 2 → waits: 2s, 4s, 8s
    status_forcelist=RETRY_STATUS_CODES, # retry on: 429, 500, 502, 503, 504
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)
```

Configured in [jobs/config.py](../jobs/config.py):

```python
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2         # delay = backoff_factor × (2 ^ (attempt - 1))
REQUEST_TIMEOUT_SECONDS = 30     # seconds before a single request gives up
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
```

**Retry timing**:
- Attempt 1: immediate
- Attempt 2: wait 2 seconds → `2 × 2^0 = 2s`
- Attempt 3: wait 4 seconds → `2 × 2^1 = 4s`
- Attempt 4 (final): wait 8 seconds → `2 × 2^2 = 8s`
- Total worst-case wait: ~14 seconds before giving up

**Why these status codes are retried**:

| Code | Meaning | Retry rationale |
|---|---|---|
| `429` | Too Many Requests | Rate limit — wait and retry |
| `500` | Internal Server Error | Transient server-side error |
| `502` | Bad Gateway | Upstream proxy issue — usually transient |
| `503` | Service Unavailable | Server overloaded — retry after delay |
| `504` | Gateway Timeout | Upstream timeout — retry |

`400 Bad Request` and `404 Not Found` are **not** in the retry list because they indicate a problem with the request itself (wrong URL, invalid parameters), which will not resolve on retry.

### Timeout Configuration

Every API call has a hard 30-second timeout:

```python
response = self.session.get(
    self.base_url,
    params=params,
    timeout=REQUEST_TIMEOUT_SECONDS  # 30 seconds
)
```

This prevents the pipeline from hanging indefinitely if the API is unresponsive. After 30 seconds with no response, `requests` raises a `Timeout` exception.

### Exception Handling Per Error Type

```python
try:
    response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()
    ...
except requests.exceptions.ConnectionError as e:
    logger.error(f"Connection error for {location.name}: {e}")
    raise
except requests.exceptions.Timeout as e:
    logger.error(f"Request timeout for {location.name} after {REQUEST_TIMEOUT_SECONDS}s: {e}")
    raise
except requests.exceptions.HTTPError as e:
    logger.error(f"HTTP error for {location.name}: {e.response.status_code} {e}")
    raise
except ValueError as e:
    logger.error(f"JSON decode error for {location.name}: {e}")
    raise
```

Each exception type gets its own catch block with a specific, actionable log message that includes the location name and the error detail. All exceptions are re-raised so that Airflow marks the task as failed and triggers the retry policy.

### Malformed Response Handling

After a successful HTTP response, the response structure is validated before parsing:

```python
if not self.validate_response(data):
    raise ValueError(f"Invalid API response structure for {location.name}")
```

`validate_response` checks:
- `latitude`, `longitude`, `hourly`, `hourly_units` are all present at the top level
- `hourly.time` is present

If any check fails, a `ValueError` is raised with the location name. This prevents downstream code from attempting to index into a missing key and producing a confusing `KeyError`.

---

## Category 2 — Transformation Failures

### Strategy: Validate, Nullify, Then Propagate

The transformer does not raise exceptions for individual bad values — doing so would discard an entire batch because of one out-of-range pressure reading. Instead:

1. **Out-of-range values** are set to `NaN` and the count is logged as a WARNING.
2. **Rows with null critical columns** are dropped and the count is logged as a WARNING.
3. **If required output columns are missing** after transformation, a `ValueError` is raised — this is a programming error (a column rename went wrong), not a data quality issue.

```python
missing = [col for col in REQUIRED_OUTPUT_COLUMNS if col not in df.columns]
if missing:
    raise ValueError(f"Missing required columns after transformation: {missing}")
```

This `ValueError` is caught in `WeatherEtlPipeline.run()` and re-raised, causing the Airflow task to fail. A missing output column is an unrecoverable error — there is no safe partial load.

### Airflow Validation Task

Between the transform and load tasks in the Airflow DAG, the `validate_weather_data` task checks that the processed DataFrame is non-empty and has all required columns. If it raises `ValueError`, the DAG stops before touching the database:

```python
if df.empty:
    raise ValueError("Validation failed — transformed DataFrame is empty")
```

---

## Category 3 — Database Connection Failures

### Connection Pool with Pre-Ping

All database connections use SQLAlchemy engines with `pool_pre_ping=True`:

```python
engine = create_engine(CONNECTION_STRING, pool_pre_ping=True)
```

`pool_pre_ping=True` means SQLAlchemy tests the connection from the pool with a lightweight `SELECT 1` before using it. If the connection is stale (the database restarted, a network blip occurred), SQLAlchemy silently discards the broken connection and opens a fresh one. This is the primary defence against transient connection drops.

**What it handles**: Brief database restarts, network interruptions, idle connection timeouts from PostgreSQL's `idle_in_transaction_session_timeout`.

**What it does not handle**: A complete database outage. If the database is down entirely, `pool_pre_ping` will fail, SQLAlchemy will raise `OperationalError`, and the pipeline will fail with a logged exception.

### Schema Initialization Defence

Schema DDL runs before any data write:

```python
def initialize_schema(self):
    with self.engine.begin() as conn:
        conn.execute(text(CREATE_TABLES_SQL))
```

`CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` make this idempotent. If the connection drops and the schema init is re-run on retry, it has no effect on existing tables.

### Load Failures and Transaction Safety

The ETL loader uses `with self.engine.begin() as conn:` for each dimension and fact insert. `engine.begin()` returns a context manager that **commits on success and rolls back on exception**:

```python
with self.engine.begin() as conn:
    execute_values(conn, INSERT_SQL, records)
```

If the insert raises (e.g., a connection drop mid-insert), the transaction is rolled back. The table is in a consistent state. On the next retry, the full set of rows is re-attempted, and `ON CONFLICT DO NOTHING` ensures no partial success from the first attempt causes duplicate rows.

### ELT Transactional Safety

In [jobs/elt/main.py](../jobs/elt/main.py), `mark_processed` is called **only after** the analytics load succeeds:

```python
try:
    self.loader.load_to_analytics(transformed_df)
except Exception as e:
    logger.exception("Analytics load failed — staging rows NOT marked as processed")
    raise  # mark_processed never reached

self.staging_loader.mark_processed(staging_ids)
```

If the analytics load fails partway through, `mark_processed` is never called. On the next run, the same staging rows are re-selected, re-transformed, and re-attempted. The `ON CONFLICT DO NOTHING` on the fact table handles any rows that were successfully inserted before the failure.

---

## Error Flow Summary

```
API call
├── ConnectionError → log ERROR → raise → Airflow retries task (up to 1 retry, 5-min delay)
├── Timeout (30s)   → log ERROR → raise → Airflow retries task
├── HTTPError 429/5xx → urllib3 retries (×3, 2s/4s/8s) → if all fail → raise → Airflow retries
├── JSONDecodeError → log ERROR → raise → Airflow retries task
└── Bad structure   → log ERROR → raise ValueError → Airflow retries task

Transform
├── Out-of-range value → set to NaN → log WARNING → continue
├── Critical null → drop row → log WARNING → continue
└── Missing output column → raise ValueError → Airflow marks task FAILED (no retry for logic errors)

DB write
├── Connection stale → pool_pre_ping reconnects → transparent retry
├── Full DB outage → OperationalError → log EXCEPTION → raise → Airflow retries
└── Constraint violation → ON CONFLICT DO NOTHING → silently skipped (not an error)
```

---

## Configuration Reference

All retry and timeout values are centralized in [jobs/config.py](../jobs/config.py):

| Parameter | Value | Effect |
|---|---|---|
| `MAX_RETRIES` | `3` | urllib3 retries per API call |
| `RETRY_BACKOFF_FACTOR` | `2` | Multiplier: waits 2s, 4s, 8s |
| `REQUEST_TIMEOUT_SECONDS` | `30` | Hard timeout per request |
| `RETRY_STATUS_CODES` | `(429, 500, 502, 503, 504)` | HTTP codes that trigger retry |

Airflow retry configuration is in [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py):

| Parameter | Value | Effect |
|---|---|---|
| `retries` | `1` | One Airflow-level retry per task |
| `retry_delay` | `timedelta(minutes=5)` | 5-minute wait before Airflow retry |

---

## Key Files

| File | Error Handling Role |
|---|---|
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | urllib3 retry strategy, per-error-type exception handling, response validation |
| [jobs/config.py](../jobs/config.py) | Centralized retry/timeout constants |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | NaN-on-range-error, drop-on-critical-null, ValueError on missing columns |
| [jobs/etl/load.py](../jobs/etl/load.py) | `pool_pre_ping`, `engine.begin()` transaction context, `ON CONFLICT DO NOTHING` |
| [jobs/elt/load.py](../jobs/elt/load.py) | `pool_pre_ping`, `mark_processed` called after analytics success only |
| [jobs/etl/main.py](../jobs/etl/main.py) | `logger.exception()` + re-raise at every pipeline step |
| [jobs/elt/main.py](../jobs/elt/main.py) | Same exception propagation pattern |
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | Task-level `retries=1`, `retry_delay=5min` |
