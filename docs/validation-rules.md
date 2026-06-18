# Validation Rules — What "Valid" Weather Data Means

## Overview

Validation runs at two points in the ETL pipeline: inside the **extractor** (API response structure validation) and inside the **transformer** (data value validation). The ELT pipeline delegates value validation to SQL, then applies the same structural checks. Validation is not just about rejecting bad rows — it is about making the failures visible so they can be investigated.

---

## Layer 1 — API Response Structural Validation

**Where**: [jobs/etl/extraction.py](../jobs/etl/extraction.py) — `WeatherExtractor.validate_response()`

Before any data is parsed, the raw JSON response from Open-Meteo is checked for the expected structure:

```python
REQUIRED_TOP_LEVEL_FIELDS = ["latitude", "longitude", "hourly", "hourly_units"]

def validate_response(self, response: dict) -> bool:
    for field in REQUIRED_TOP_LEVEL_FIELDS:
        if field not in response:
            logger.error(f"Missing required field '{field}' in API response")
            return False
    if "time" not in response["hourly"]:
        logger.error("Missing 'time' field in hourly data")
        return False
    return True
```

**Checked fields**: `latitude`, `longitude`, `hourly`, `hourly_units`, and `hourly.time`.

**What happens on failure**: The extractor raises an exception with a structured log message. The row is never parsed. No partial data reaches the transformer.

**Why this matters**: If Open-Meteo changes its response shape (a restructured key name, a missing section), the pipeline fails loudly at extraction instead of silently propagating malformed data into the database.

---

## Layer 2 — Data Value Validation (ETL — Python)

**Where**: [jobs/etl/transform.py](../jobs/etl/transform.py) — `WeatherDataTransformer`

### 2a. Null Handling

Two null strategies are applied depending on the semantic importance of the column:

**Critical columns — drop the row if null**:

| Column | Why dropping is correct |
|---|---|
| `timestamp` | A row with no timestamp has no identity — it cannot be placed in any time dimension |
| `temperature_celsius` | The primary measurement of the dataset; a missing temperature makes the row analytically useless |
| `location_name` | A row without a location cannot be linked to `dim_location` — it would fail the FK constraint |

```python
CRITICAL_COLUMNS = ["timestamp", "temperature_celsius", "location_name"]
df = df.dropna(subset=CRITICAL_COLUMNS)
```

**Non-critical columns — fill nulls with a domain-valid default**:

| Column | Default | Rationale |
|---|---|---|
| `precipitation_mm` | `0.0` | No reading for precipitation means no precipitation occurred |
| `rain_mm` | `0.0` | Same — absence of rain data means no rain |
| `snowfall_cm` | `0.0` | Same — absence of snowfall data means no snow |

```python
FILL_ZERO_COLUMNS = ["precipitation_mm", "rain_mm", "snowfall_cm"]
df[FILL_ZERO_COLUMNS] = df[FILL_ZERO_COLUMNS].fillna(0.0)
```

Filling these three with zero is physically meaningful: if the sensor or API produced no reading, the correct interpretation for accumulation fields is zero, not unknown.

### 2b. Type Validation

All numeric columns are coerced to float using `pd.to_numeric` with `errors='coerce'`:

```python
NUMERIC_COLUMNS = [
    "temperature_celsius", "relative_humidity_pct", "precipitation_mm",
    "rain_mm", "snowfall_cm", "wind_speed_kmh", "wind_direction_deg",
    "wind_gusts_kmh", "pressure_hpa", "visibility_m", "uv_index",
    "cloud_cover_pct", "weather_code", "is_day"
]

for col in NUMERIC_COLUMNS:
    df[col] = pd.to_numeric(df[col], errors="coerce")
```

`errors='coerce'` means any value that cannot be converted to a number (e.g., the string `"N/A"`, an empty string, a boolean label) is silently replaced with `NaN`. The range validation step downstream will then handle these NaN values.

The `timestamp` column is parsed with `pd.to_datetime` with `errors='coerce'`, which converts unparseable strings to `NaT` (Not a Time). The critical-null drop then removes any rows where the timestamp is `NaT`.

### 2c. Range Validation

After type casting, out-of-range values are replaced with `NaN` and logged:

| Column | Min | Max | Physical basis |
|---|---|---|---|
| `temperature_celsius` | -90.0 | 60.0 | All-time records: -89.2°C (Antarctica) to 56.7°C (Death Valley) |
| `relative_humidity_pct` | 0.0 | 100.0 | Humidity is a percentage — cannot be negative or exceed 100 |
| `precipitation_mm` | 0.0 | 500.0 | 500mm in one hour is beyond any recorded extreme |
| `wind_speed_kmh` | 0.0 | 400.0 | Fastest recorded tornado: ~484 km/h; 400 is a conservative upper bound |
| `pressure_hpa` | 870.0 | 1084.0 | All-time extremes: 870 hPa (Typhoon Tip) to 1083.8 hPa (Agata, Siberia) |
| `uv_index` | 0.0 | 20.0 | WHO scale: 0 (none) to 11+ (extreme); 20 is a generous ceiling |
| `cloud_cover_pct` | 0.0 | 100.0 | Cloud cover is a percentage — cannot be negative or exceed 100 |

```python
RANGE_RULES = {
    "temperature_celsius":   (-90.0, 60.0),
    "relative_humidity_pct": (0.0, 100.0),
    "precipitation_mm":      (0.0, 500.0),
    "wind_speed_kmh":        (0.0, 400.0),
    "pressure_hpa":          (870.0, 1084.0),
    "uv_index":              (0.0, 20.0),
    "cloud_cover_pct":       (0.0, 100.0),
}

for col, (min_val, max_val) in RANGE_RULES.items():
    out_of_range = ~df[col].between(min_val, max_val) & df[col].notna()
    if out_of_range.any():
        count = out_of_range.sum()
        logger.warning(f"Column '{col}': {count} out-of-range values set to NaN")
        df.loc[out_of_range, col] = np.nan
```

Out-of-range values do not cause the row to be dropped — only the specific measurement is nullified. A row with an invalid pressure reading but valid temperature, humidity, and wind data is still analytically useful for those other measurements.

### 2d. Required Column Check (Post-Transform)

After all transformations are applied, the transformer verifies that all expected output columns are present:

```python
REQUIRED_OUTPUT_COLUMNS = [
    "timestamp", "temperature_celsius", "relative_humidity_pct",
    "precipitation_mm", "wind_speed_kmh", "location_name", "location_country"
]

missing = [col for col in REQUIRED_OUTPUT_COLUMNS if col not in df.columns]
if missing:
    raise ValueError(f"Missing required columns after transformation: {missing}")
```

This is a programmer-facing guard. If a code change accidentally removes a column rename or a derived field calculation, this check catches it immediately rather than allowing a silent schema mismatch at the database insert step.

---

## Layer 3 — Data Value Validation (ELT — SQL)

**Where**: [jobs/elt/transform.py](../jobs/elt/transform.py) — `StagingTransformer`

The ELT pipeline performs the same validation logic entirely in SQL. The transformation query includes:

**Null filtering** (row-level, equivalent to ETL's `dropna`):
```sql
WHERE is_processed = FALSE
  AND "timestamp" IS NOT NULL
  AND temperature_2m IS NOT NULL
  AND location_name IS NOT NULL
```

**Null-filling for accumulation columns** (via `COALESCE`):
```sql
COALESCE(precipitation, 0.0)  AS precipitation_mm,
COALESCE(rain, 0.0)           AS rain_mm,
COALESCE(snowfall, 0.0)       AS snowfall_cm,
```

**Type coercion**:
```sql
"timestamp"::TIMESTAMP        AS timestamp,
temperature_2m::FLOAT         AS temperature_celsius,
relative_humidity_2m::FLOAT   AS relative_humidity_pct,
```

SQL will raise an error if a value cannot be cast — unlike Python's `errors='coerce'`, SQL does not silently produce NULL for cast failures. If the staging table contains an unparseable string in a NUMERIC column, the SQL cast will throw. This is acceptable: the staging schema already uses `NUMERIC` (not `VARCHAR`) for measurement columns, so only values that passed Pandas' numeric coercion in the extractor can reach staging.

**Range validation** is not performed in SQL in the current implementation. The ELT pipeline trusts that values within NUMERIC bounds are physically plausible. Extending the ELT SQL to include `CASE WHEN temperature_2m < -90 OR temperature_2m > 60 THEN NULL ELSE temperature_2m END` is a straightforward addition if range enforcement at the SQL layer is required.

---

## Layer 4 — Airflow Validation Task

**Where**: [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) — `validate_weather_data` task

The Airflow DAG has a dedicated validation task that runs **between the transform and load tasks**:

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

**What this adds**: A separate task in the DAG graph means validation failure shows as a distinct task failure in the Airflow UI — not a failure of the transform or load task. Operators can see exactly where in the pipeline the problem occurred. If validation fails, the `load_weather_data` task never runs — the pipeline stops cleanly with no partial writes to the database.

---

## Validation Summary

| Check | Where | Action on failure |
|---|---|---|
| API response structure | Extraction (Python) | Raise exception; task fails |
| Critical null columns (timestamp, temperature, location) | Transform (Python / SQL) | Drop the row |
| Type coercion | Transform (Python `errors='coerce'` / SQL cast) | Set to NaN / raise error |
| Out-of-range values | Transform (Python) | Set measurement to NaN; log count |
| Non-critical null fills | Transform (Python COALESCE / SQL COALESCE) | Fill with 0.0 |
| Required output columns present | Transform (Python) | Raise ValueError |
| DataFrame non-empty after transform | Airflow validate task | Raise ValueError; load task blocked |

---

## Key Files

| File | Validation Role |
|---|---|
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | `validate_response()` — API structure check |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | Null handling, type casting, range rules, required column check |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | SQL `WHERE` filter, `COALESCE`, SQL cast |
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | `validate_weather_data` task — post-transform gate |
| [tests/etl/test_transform.py](../tests/etl/test_transform.py) | 30 tests covering all validation branches |
