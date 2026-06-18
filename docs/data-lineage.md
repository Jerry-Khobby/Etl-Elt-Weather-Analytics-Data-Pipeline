# Data Lineage — Tracing a Value from the Fact Table to the Raw API Response

## Overview

**Data lineage** is the ability to trace any value in your analytical system back to the source that produced it. When a temperature reading in `fact_weather` looks wrong, you need to be able to answer: *Where did this number come from? Was it the API? Was it transformed? Was it derived?*

This project maintains data lineage through a chain of artefacts — the raw API response, raw CSV files, the staging table, and audit timestamps on the fact table — each preserving the data at a different stage of the pipeline.

---

## The Lineage Chain

```
Open-Meteo API Response (JSON)
        │
        │  extraction timestamp captured here
        ▼
data/raw/weather_raw_{timestamp}.csv      ← ETL audit file (raw, untransformed)
        │
        │  OR in ELT:
        ▼
weather_raw_staging (PostgreSQL)          ← ELT staging table (loaded_at timestamp)
        │  is_processed = FALSE → TRUE tracks promotion
        │
        ▼
WeatherDataTransformer / StagingTransformer
        │  column renames, type casts, derived fields
        │
        ▼
fact_weather (PostgreSQL analytics DB)    ← extracted_at preserves API pull time
        │
        ▼
Analytical views (vw_hourly_weather, vw_daily_weather_summary, ...)
        │
        ▼
Metabase Dashboards
```

---

## Stage 1 — The API Response

The Open-Meteo API returns JSON with this structure (simplified):

```json
{
  "latitude": 5.6037,
  "longitude": -0.1870,
  "hourly": {
    "time":               ["2026-06-11T00:00", "2026-06-11T01:00", ...],
    "temperature_2m":     [26.4, 25.8, ...],
    "relative_humidity_2m": [82, 85, ...],
    "precipitation":      [0.0, 0.2, ...],
    "weather_code":       [2, 61, ...],
    ...
  }
}
```

The `ExtractionResult` dataclass in [jobs/etl/extraction.py](../jobs/etl/extraction.py) captures this along with the extraction timestamp:

```python
@dataclass
class ExtractionResult:
    raw_data:   dict          # the full API JSON response
    location:   Location      # city metadata (name, country, lat, lon, timezone)
    extracted_at: datetime    # when this API call was made
    record_count: int         # number of hourly rows returned
```

The `extracted_at` timestamp travels with the data through every subsequent stage.

---

## Stage 2 — Raw CSV (ETL Pipeline)

Before any transformation occurs, the ETL main orchestrator saves the raw DataFrame to disk. In [jobs/etl/main.py](../jobs/etl/main.py):

```python
raw_df = self.extractor.extract_all()
# Save raw data BEFORE transformation — this is the lineage anchor
raw_path = DATA_RAW_DIR / f"weather_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
raw_df.to_csv(raw_path, index=False)

logger.info(f"Raw data saved to {raw_path}")
logger.debug(f"Raw columns: {raw_df.columns.tolist()}")
logger.debug(f"Raw dtypes:\n{raw_df.dtypes}")
logger.debug(f"Missing values:\n{raw_df.isnull().sum()}")
```

The raw CSV preserves the **original API column names** (`temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, etc.) and the **original values before any transformation**.

### Raw CSV Schema

The raw CSV contains:
- All 15 hourly measurement columns in original API naming
- `location_name`, `location_country`, `latitude`, `longitude` (added by extractor)
- `extracted_at` (the extraction timestamp from `ExtractionResult`)

This file is the ETL pipeline's **ground truth**: if a value in the fact table ever looks wrong, the raw CSV from the same `extracted_at` timestamp can be opened to verify the original API value.

---

## Stage 3 — Staging Table (ELT Pipeline)

In the ELT pipeline, raw data is stored in the `weather_raw_staging` table in PostgreSQL. This is queryable — unlike a CSV file, you can filter, join, and aggregate directly. In [sql/init_staging.sql](../sql/init_staging.sql):

```sql
CREATE TABLE IF NOT EXISTS weather_raw_staging (
    id             BIGSERIAL PRIMARY KEY,  -- unique row identifier
    "timestamp"    VARCHAR(50),            -- raw string, not yet cast
    temperature_2m NUMERIC,               -- original API value
    -- ... all 15 measurement columns in original API names ...
    location_name  VARCHAR(100),
    location_country VARCHAR(100),
    latitude       NUMERIC(9,6),
    longitude      NUMERIC(9,6),
    extracted_at   VARCHAR(50),           -- when the API call was made
    loaded_at      TIMESTAMPTZ DEFAULT NOW(),  -- when this row hit staging
    is_processed   BOOLEAN DEFAULT FALSE  -- tracks promotion to analytics
);
```

Two audit timestamps:
- **`extracted_at`**: When the data was pulled from the Open-Meteo API
- **`loaded_at`**: When this row was inserted into the staging table (automatically set by `DEFAULT NOW()`)

The difference between `loaded_at` and `extracted_at` tells you the latency between extraction and staging load.

---

## Stage 4 — The Fact Table

After transformation, data lands in `fact_weather`. The `extracted_at` column is preserved through the entire pipeline:

```sql
CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id             BIGSERIAL PRIMARY KEY,
    location_id         INTEGER NOT NULL REFERENCES dim_location(location_id),
    date_id             INTEGER NOT NULL REFERENCES dim_date(date_id),
    time_id             INTEGER NOT NULL REFERENCES dim_time(time_id),
    condition_id        INTEGER REFERENCES dim_weather_condition(condition_id),
    -- measurements (transformed values):
    temperature_celsius  NUMERIC(5,2),
    temperature_fahrenheit NUMERIC(5,2),  -- derived: not in API
    relative_humidity_pct NUMERIC(5,2),
    precipitation_mm     NUMERIC(6,2),
    -- ...
    extracted_at         TIMESTAMPTZ,     -- lineage anchor: original API pull time
    UNIQUE (location_id, date_id, time_id)
);
```

The `extracted_at` field is the **lineage anchor** in the fact table. It tells you exactly which API call produced each row.

---

## Tracing a Value — End to End

Suppose you find `temperature_celsius = 38.7` for Accra on 2026-06-15 at 14:00 and want to verify it.

### Step 1 — Find the fact table row

```sql
SELECT
    f.fact_id,
    f.temperature_celsius,
    f.extracted_at,
    l.location_name,
    d.full_date,
    t.hour
FROM fact_weather f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date     d ON f.date_id     = d.date_id
JOIN dim_time     t ON f.time_id     = t.time_id
WHERE l.location_name = 'Accra'
  AND d.full_date     = '2026-06-15'
  AND t.hour          = 14;
-- Returns: fact_id=12345, temperature_celsius=38.7, extracted_at='2026-06-15 14:05:31+00'
```

### Step 2 — Trace back to staging (ELT)

```sql
SELECT
    id,
    "timestamp",
    temperature_2m,   -- original API value before renaming
    extracted_at,
    loaded_at,
    is_processed
FROM weather_raw_staging
WHERE location_name = 'Accra'
  AND "timestamp"   = '2026-06-15T14:00'
  AND extracted_at  = '2026-06-15 14:05:31';
-- Returns: id=6789, temperature_2m=38.7, loaded_at='2026-06-15 14:06:02+00', is_processed=true
```

You can see `temperature_2m = 38.7` — the value was not modified by the transformation, only renamed from `temperature_2m` to `temperature_celsius`.

### Step 3 — Trace back to raw CSV (ETL)

Find the raw CSV file whose timestamp matches `extracted_at`:

```
data/raw/weather_raw_20260615_140531.csv
```

Open the file and filter for `location_name = Accra` and `timestamp = 2026-06-15T14:00`:

```
timestamp,temperature_2m,...,location_name,extracted_at
2026-06-15T14:00,38.7,...,Accra,2026-06-15 14:05:31
```

The original API value `temperature_2m = 38.7` is confirmed.

### What This Tells You

| Stage | Column Name | Value | Notes |
|---|---|---|---|
| API JSON | `temperature_2m` | `38.7` | Original API response |
| Raw CSV | `temperature_2m` | `38.7` | Saved before transformation |
| Staging table | `temperature_2m` | `38.7` | Stored as NUMERIC, no cast yet |
| Fact table | `temperature_celsius` | `38.7` | Renamed; value unchanged |
| Fact table | `temperature_fahrenheit` | `101.7` | **Derived**: `38.7 × 9/5 + 32` |

`temperature_fahrenheit` is a **derived field** — it has no lineage back to the API because it never existed in the API response. Its lineage is: `temperature_celsius × 9/5 + 32`, computed by `WeatherDataTransformer.add_derived_fields()` in the ETL or by the SQL expression `(temperature_2m * 9.0 / 5.0 + 32)::NUMERIC` in the ELT.

---

## Derived Fields — Special Lineage

Several columns in `fact_weather` have no direct API source — they are computed:

| Fact Column | Source | Derived By |
|---|---|---|
| `temperature_fahrenheit` | `temperature_2m` | `temp × 9/5 + 32` |
| Date parts in `dim_date` | `timestamp` | `EXTRACT(YEAR/MONTH/DAY/...)` |
| `period_of_day` in `dim_time` | `hour` | `CASE WHEN hour < 6 THEN 'Night'...` |
| `weather_description` | `weather_code` | WMO code lookup table |
| `weather_category` | `weather_code` | WMO code lookup table |
| `is_weekend` | `day_of_week` | `day_of_week IN (5, 6)` |

These are computed transformations, not measurements. Their lineage traces to the source column (`temperature_2m`, `weather_code`, `timestamp`) and the transformation rule, not to the API directly.

---

## Key Files

| File | Lineage Role |
|---|---|
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | Captures `extracted_at` on the `ExtractionResult` dataclass |
| [jobs/etl/main.py](../jobs/etl/main.py) | Saves raw CSV before transformation |
| [jobs/elt/load.py](../jobs/elt/load.py) | Staging table with `extracted_at` and `loaded_at` columns |
| [sql/init_staging.sql](../sql/init_staging.sql) | `loaded_at TIMESTAMPTZ DEFAULT NOW()` for staging audit |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | `extracted_at TIMESTAMPTZ` on fact_weather |
| [sql/views_analytics.sql](../sql/views_analytics.sql) | Views expose lineage columns (extracted_at flows through vw_hourly_weather) |
| [data/raw/](../data/raw/) | Raw CSV files — ETL lineage anchor |
