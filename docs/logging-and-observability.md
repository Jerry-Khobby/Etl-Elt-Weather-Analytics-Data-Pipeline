# Logging & Observability — What Gets Logged, at What Level, and Where Logs Go

## Overview

Observability is the ability to understand the internal state of a system from its external outputs. For a data pipeline, this means knowing — from logs alone — whether a run succeeded, how many records were processed, whether any data quality issues were encountered, and exactly what went wrong when a failure occurs. This project centralizes all logging through a single configured logger with two sinks: a rotating file and the console (stdout).

---

## Logger Configuration

**Where**: [jobs/utils/logger.py](../jobs/utils/logger.py)

A factory function `get_logger(name)` is called at the top of every module to obtain a named logger:

```python
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from jobs.config import LOG_LEVEL, LOG_DIR

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    if not logger.handlers:
        formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

        # Console handler (stdout)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Rotating file handler
        log_file = Path(LOG_DIR) / "pipeline.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB per file
            backupCount=5,               # keep 5 rotated files
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
```

### Log Format

Every log line has the format:

```
2026-06-15 14:05:31 | INFO     | jobs.etl.extraction | Extracted 168 records for London, United Kingdom
2026-06-15 14:05:33 | WARNING  | jobs.etl.transform  | Column 'pressure_hpa': 3 out-of-range values set to NaN
2026-06-15 14:05:41 | ERROR    | jobs.etl.extraction | Connection error for Tokyo: HTTPSConnectionPool...
```

Fields:
- **Timestamp**: `%Y-%m-%d %H:%M:%S` — precise to the second, unambiguous format
- **Level**: Fixed-width 8 characters (`INFO    `, `WARNING `, `ERROR   `) for easy visual scanning
- **Name**: The Python module path (e.g., `jobs.etl.extraction`, `jobs.elt.transform`) — tells you exactly which file produced the log line
- **Message**: Human-readable description with embedded context (location, count, column name)

### Log Sinks

| Sink | Location | Behaviour |
|---|---|---|
| Console (stdout) | Terminal / Docker container logs | Streams in real time; visible via `docker logs <container>` |
| Rotating file | `logs/pipeline.log` | Persists after container exit; rotates at 10 MB |

**Rotation policy**: When `pipeline.log` reaches 10 MB, it is renamed to `pipeline.log.1`. If `pipeline.log.1` exists, it becomes `pipeline.log.2`, and so on up to `pipeline.log.5`. The sixth rotation deletes `pipeline.log.5`. This means at most 60 MB of log history is retained (6 files × 10 MB).

### Log Level Configuration

The log level is set via the `LOG_LEVEL` environment variable (defaulting to `INFO` in [jobs/config.py](../jobs/config.py)):

```bash
LOG_LEVEL=DEBUG   # maximum verbosity — includes DataFrame previews and column dtypes
LOG_LEVEL=INFO    # normal operation — records extracted, loaded, validated
LOG_LEVEL=WARNING # only anomalies — out-of-range values, null drops
LOG_LEVEL=ERROR   # only failures
```

In production, `INFO` is the appropriate default. During development or debugging, `DEBUG` reveals the full data profile (raw DataFrame head, column dtypes, missing value counts) that would be too verbose for routine logs.

---

## What Gets Logged at Each Level

### DEBUG

DEBUG messages are developer-facing and verbose. They are off by default (`LOG_LEVEL=INFO`).

**In [jobs/etl/main.py](../jobs/etl/main.py)**:
```
jobs.etl.main | DEBUG | Raw data preview:
   timestamp  temperature_2m  ...
0  2026-06-09T00:00         26.4
1  2026-06-09T01:00         25.8
...

jobs.etl.main | DEBUG | Column dtypes:
timestamp         object
temperature_2m    float64
...

jobs.etl.main | DEBUG | Missing values before transform:
temperature_2m    0
relative_humidity 2
...
```

These messages give a complete data profile for every pipeline run. When a data quality issue is being investigated, enabling `LOG_LEVEL=DEBUG` on a re-run produces a snapshot of what the raw data looked like at the point of extraction.

### INFO

INFO messages are the operational heartbeat. Every meaningful pipeline event produces an INFO message.

**Extraction** (`jobs.etl.extraction`):
```
INFO | Extracted 168 records for Accra, Ghana
INFO | Extracted 168 records for London, United Kingdom
INFO | Extracted 168 records for New York, United States
INFO | Extracted 168 records for Tokyo, Japan
INFO | Extracted 168 records for Berlin, Germany
INFO | Extraction complete: 840 total records across 5 locations
```

**Transformation** (`jobs.etl.transform`):
```
INFO | Removed 0 duplicate rows
INFO | Transformation complete: 840 records retained
```

**Loading** (`jobs.etl.load`):
```
INFO | Schema initialized successfully
INFO | Loaded/updated 5 records in dim_location
INFO | Loaded/updated 7 records in dim_date
INFO | dim_time already populated (24 rows)
INFO | Loaded/updated 12 records in dim_weather_condition
INFO | Attempted to insert 840 rows into fact_weather
INFO | Pipeline complete
```

**ELT pipeline** (`jobs.elt.main`):
```
INFO | Staging schema initialized
INFO | Loaded 840 raw rows to staging
INFO | Transformed 720 unprocessed staging rows
INFO | Analytics load complete
INFO | Marked 840 staging rows as processed
```

A successful run produces approximately 15–20 INFO lines. Reading them top-to-bottom gives a complete picture of what happened without requiring database queries.

### WARNING

WARNING messages indicate anomalies that did not stop the pipeline but represent data quality events that should be investigated.

**Range violations** (`jobs.etl.transform`):
```
WARNING | Column 'pressure_hpa': 3 out-of-range values set to NaN
WARNING | Column 'uv_index': 1 out-of-range values set to NaN
```

**Null drops** (`jobs.etl.transform`):
```
WARNING | Dropped 2 rows due to null values in critical columns (timestamp, temperature_celsius, location_name)
```

**Duplicate removal** (when non-zero):
```
WARNING | Removed 5 duplicate rows by (timestamp, location_name, location_country)
```

**Why WARNING, not INFO**: These events affect the completeness or accuracy of the data that reaches the analytics layer. An operator reviewing logs should notice them and decide whether to investigate the API data source or adjust validation thresholds.

### ERROR

ERROR messages indicate that a step failed. They always include the location name or the specific resource that failed.

**API failures** (`jobs.etl.extraction`):
```
ERROR | Connection error for Tokyo: HTTPSConnectionPool(host='api.open-meteo.com', port=443): Max retries exceeded
ERROR | Request timeout for Berlin after 30s: HTTPSConnectionPool...
ERROR | HTTP error for London: 503 Service Unavailable
ERROR | Missing required field 'hourly' in API response for New York
ERROR | JSON decode error for Accra: Expecting value: line 1 column 1 (char 0)
```

**Airflow validation** (`airflow.task.validate_weather_data`):
```
ERROR | Validation failed — missing columns: ['wind_speed_kmh', 'location_country']
ERROR | Validation failed — transformed DataFrame is empty
```

### EXCEPTION (logger.exception)

`logger.exception()` is used inside `except` blocks in the pipeline orchestrators. It logs at ERROR level but also appends the full Python stack trace.

**In [jobs/etl/main.py](../jobs/etl/main.py) and [jobs/elt/main.py](../jobs/elt/main.py)**:
```
ERROR    | jobs.etl.main | Extraction step failed
Traceback (most recent call last):
  File "/app/jobs/etl/main.py", line 45, in run
    raw_df = self.extractor.extract_all()
  File "/app/jobs/etl/extraction.py", line 112, in extract_all
    result = self.extract(location)
  File "/app/jobs/etl/extraction.py", line 87, in extract
    raise ValueError(f"Invalid API response structure for {location.name}")
ValueError: Invalid API response structure for Tokyo
```

The stack trace is the difference between a 5-minute debug session and a 30-minute one. It shows the exact line and the full call chain without needing to reproduce the failure.

---

## Log Locations

| Context | Log location | How to access |
|---|---|---|
| ETL/ELT containers | `logs/pipeline.log` (mounted volume) | `docker exec <container> cat /app/logs/pipeline.log` |
| Airflow tasks | `airflow/logs/dag_id=.../run_id=.../task_id=.../` | Airflow UI → DAG → Task → Logs tab |
| Container stdout | Docker daemon | `docker logs weather_etl_pipeline-etl-1` |
| Airflow DAG processor | `airflow/logs/dag_processor_manager/dag_processor_manager.log` | Direct file access |

The `logs/` directory is mounted as a Docker volume in [docker-compose.yml](../docker-compose.yml), so log files persist after container exits and are accessible from the host machine.

---

## Observability Gaps and Enhancements

The current logging provides good **run-level visibility** (did the pipeline succeed, how many records, what errors). What it does not provide:

| Gap | What is missing |
|---|---|
| **Metrics** | No counters or gauges pushed to a time-series store (Prometheus, Datadog). The `840 records` count is in a log line but not queryable as a metric. |
| **Structured logging** | Log messages are human-readable strings, not JSON. Shipping to a log aggregator (ELK, Loki) requires regex parsing rather than field extraction. |
| **Latency tracking** | No measurement of how long each pipeline step takes. A slow extraction (API latency growing over time) would not be visible without parsing timestamps from consecutive log lines. |
| **Run history** | No persistent store of per-run outcomes (record count, error count, duration). This would require writing run metadata to a `pipeline_runs` table or a monitoring service. |

A production-grade addition would be to emit structured JSON logs and push run-level metrics to a monitoring system. See [alerting-and-failure-notification.md](alerting-and-failure-notification.md) for the broader observability design.

---

## Key Files

| File | Logging Role |
|---|---|
| [jobs/utils/logger.py](../jobs/utils/logger.py) | Logger factory: format, handlers, rotation, level |
| [jobs/config.py](../jobs/config.py) | `LOG_LEVEL`, `LOG_DIR` environment configuration |
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | Per-location extraction events, API error types |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | Range warnings, null-drop warnings, record count |
| [jobs/etl/load.py](../jobs/etl/load.py) | Dimension and fact load counts |
| [jobs/etl/main.py](../jobs/etl/main.py) | DEBUG profile data; `logger.exception()` on failures |
| [jobs/elt/main.py](../jobs/elt/main.py) | ELT step completion events; `logger.exception()` on failures |
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | Airflow task logs (separate from pipeline.log) |
| [docker-compose.yml](../docker-compose.yml) | Log volume mounts; container health check logs |
