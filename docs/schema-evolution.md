# Schema Evolution — Handling API Changes and Schema Modifications

## Overview

**Schema evolution** is the ability of a pipeline to handle changes to its data sources or data models without breaking. APIs change: fields are added, fields are removed, data types change, and response structures are restructured. A brittle pipeline breaks on any of these. A resilient pipeline absorbs the changes gracefully.

This document describes how the project handles API-level schema changes, database schema changes, and what would need to change in each scenario.

---

## Current Defence Mechanisms

The project has several existing patterns that provide robustness against schema changes:

### 1. IF NOT EXISTS on All DDL

All `CREATE TABLE` and `CREATE INDEX` statements in [sql/init_analytics.sql](../sql/init_analytics.sql) and [sql/init_staging.sql](../sql/init_staging.sql) use `IF NOT EXISTS`:

```sql
CREATE TABLE IF NOT EXISTS fact_weather (...);
CREATE INDEX IF NOT EXISTS idx_fact_weather_location ON fact_weather(location_id);
```

This means the schema initialization scripts are **idempotent** — they can be re-run at any time without dropping or modifying existing tables. Adding new tables to the init scripts is safe; existing tables are untouched.

### 2. CREATE OR REPLACE VIEW

All five analytical views in [sql/views_analytics.sql](../sql/views_analytics.sql) use:

```sql
CREATE OR REPLACE VIEW vw_hourly_weather AS ...;
```

This means views can be updated (e.g., to add a new column) by re-running the script. The view definition is replaced atomically.

### 3. Flexible Staging Schema (ELT)

The staging table in [sql/init_staging.sql](../sql/init_staging.sql) uses `NUMERIC` for all measurement columns — no strict type constraints at the staging layer. The timestamp is stored as `VARCHAR(50)`. This intentional looseness means:

- A column that changes from integer to float in the API will still load correctly into `NUMERIC`.
- A timestamp format change (`2026-06-15T14:00` → `2026-06-15 14:00:00`) will still load as a string into `VARCHAR(50)`.

Type strictness is enforced only at the analytics layer, after the SQL transformation has validated and cast the values.

### 4. Explicit Column Selection in Transformers

The ETL transformer in [jobs/etl/transform.py](../jobs/etl/transform.py) and the ELT SQL transformation in [jobs/elt/transform.py](../jobs/elt/transform.py) both explicitly select and rename only the columns they need. They do not use `SELECT *`. This means extra columns in the API response are silently ignored.

### 5. Response Schema Validation

In [jobs/etl/extraction.py](../jobs/etl/extraction.py), the extractor validates that required top-level fields exist in the API response before processing:

```python
def validate_response(self, response: dict) -> bool:
    required_fields = ["latitude", "longitude", "hourly"]
    for field in required_fields:
        if field not in response:
            return False
    if "time" not in response["hourly"]:
        return False
    return True
```

This catches the case where the API restructures its response completely (e.g., renames `hourly` to `hourly_data`), fails fast with a clear error, and does not attempt to process a malformed response.

---

## Scenario 1 — The API Adds a New Field

**Example**: Open-Meteo adds `feels_like_temperature_2m` to the hourly variables.

### What Happens Without Changes

**ETL pipeline**: The extractor's `HOURLY_VARIABLES` list in [jobs/config.py](../jobs/config.py) does not include the new field, so it is not requested from the API. The API response does not contain it. Nothing breaks; the new field is simply not pulled.

**ELT pipeline**: Same — the new field is not in `HOURLY_VARIABLES`, not in the API response, not in the staging table, and not in the analytics schema. The pipeline runs exactly as before.

### To Add the New Field

1. **Add to `HOURLY_VARIABLES`** in [jobs/config.py](../jobs/config.py):
   ```python
   HOURLY_VARIABLES = [
       "temperature_2m",
       "feels_like_temperature_2m",  # new
       ...
   ]
   ```

2. **ETL — Add column rename** in [jobs/etl/transform.py](../jobs/etl/transform.py):
   ```python
   COLUMN_RENAME_MAP = {
       "temperature_2m":           "temperature_celsius",
       "feels_like_temperature_2m": "feels_like_celsius",  # new
       ...
   }
   ```

3. **ELT — Add column to staging table**:
   ```sql
   ALTER TABLE weather_raw_staging ADD COLUMN IF NOT EXISTS feels_like_temperature_2m NUMERIC;
   ```

4. **Add column to analytics fact table**:
   ```sql
   ALTER TABLE fact_weather ADD COLUMN IF NOT EXISTS feels_like_celsius NUMERIC(5,2);
   ```

5. **Update the SQL transformation** in [jobs/elt/transform.py](../jobs/elt/transform.py) to include the new cast.

6. **Update the loader** in [jobs/etl/load.py](../jobs/etl/load.py) to include the new column in the fact table INSERT.

7. **Update the validation list** in [jobs/etl/transform.py](../jobs/etl/transform.py) if the new column is required.

**Historical data**: After the change, historical rows in the fact table will have `feels_like_celsius = NULL` (the column was added with no default). This is acceptable — the field did not exist when those rows were written. The `ALTER TABLE ADD COLUMN IF NOT EXISTS` syntax is safe to add to the init scripts.

---

## Scenario 2 — The API Removes a Field

**Example**: Open-Meteo removes `uv_index` from the hourly variables.

### What Happens Without Changes

**ETL pipeline**: The extractor requests `uv_index` in `HOURLY_VARIABLES`. The API no longer returns it. The API response will have an empty array or missing key for `uv_index`. In [jobs/etl/transform.py](../jobs/etl/transform.py), the validation step checks for required columns:

```python
REQUIRED_COLUMNS = [
    "timestamp", "temperature_celsius", "location_name",
    "relative_humidity_pct", ...
]
```

If `uv_index` is in `REQUIRED_COLUMNS`, the pipeline raises a `ValueError` and the task fails. If `uv_index` is not in `REQUIRED_COLUMNS` (it is currently included in the transformation but may not be in the required validation set), the column may silently be missing from the DataFrame, causing a `KeyError` when the loader tries to insert it.

**ELT pipeline**: The staging loader inserts whatever columns the DataFrame has. If `uv_index` is missing from the DataFrame, the staging insert will either succeed (missing column → NULL) or fail (if the column has a NOT NULL constraint). The staging schema uses NUMERIC with no NOT NULL constraints, so it would load with NULL. The SQL transformation selecting `uv_index` from staging would return NULLs, and the analytics fact insert would set `uv_index = NULL`.

### To Handle Removal Cleanly

1. **Remove from `HOURLY_VARIABLES`** in [jobs/config.py](../jobs/config.py).
2. **Remove from the rename map** in [jobs/etl/transform.py](../jobs/etl/transform.py).
3. **Keep the column in the database schema** — dropping `uv_index` from `fact_weather` would destroy historical data. Leave it as a nullable column; historical rows retain their values, new rows get NULL.
4. **Update views** if they reference `uv_index` — the views will still work but will return NULL for new rows.
5. **Do not drop the staging column** — historical staging rows with `uv_index` values remain queryable.

The key principle: **never drop a column from the database when removing it from the pipeline**. Leave it nullable; historical values are preserved.

---

## Scenario 3 — The API Renames a Field

**Example**: `relative_humidity_2m` is renamed to `relative_humidity_percent` in the API.

### What Happens Without Changes

The extractor requests `relative_humidity_2m` but the API now returns `relative_humidity_percent`. The API will either:
- Return an empty array for the old name and a populated array for the new name (if it supports both temporarily)
- Return an error for the unrecognised variable name

The extractor's response validation checks for `hourly.time` but not for individual variable presence, so the pipeline may proceed with a missing column and fail at the transformation or loading step.

### To Handle the Rename

1. **Update `HOURLY_VARIABLES`** in [jobs/config.py](../jobs/config.py) to use the new name.
2. **Update the rename map** in [jobs/etl/transform.py](../jobs/etl/transform.py):
   ```python
   COLUMN_RENAME_MAP = {
       "relative_humidity_percent": "relative_humidity_pct",  # was: relative_humidity_2m
       ...
   }
   ```
3. **Update the staging schema** (ELT):
   ```sql
   ALTER TABLE weather_raw_staging ADD COLUMN IF NOT EXISTS relative_humidity_percent NUMERIC;
   ```
   Keep `relative_humidity_2m` for historical rows (it will have values for old rows, NULL for new rows after the API change).

---

## Scenario 4 — The API Response Structure Changes

**Example**: The API moves hourly data from `{"hourly": {"time": [...], "temperature_2m": [...]}}` to `{"data": {"hourly": {"time": [...], ...}}}`.

### What Happens Without Changes

The `validate_response` method in [jobs/etl/extraction.py](../jobs/etl/extraction.py) checks:

```python
if "hourly" not in response:
    return False
```

A top-level restructure would cause this validation to fail, and the extractor would raise an exception. The pipeline fails with a clear error. **This is the correct behaviour** — it is better to fail loudly than to proceed silently with a broken parsing path.

### To Handle a Structural Change

Update `validate_response` and the response parsing logic in `extraction.py` to match the new structure. The transformation and loading layers are unaffected — they only see the parsed DataFrame, not the raw API JSON.

---

## Scenario 5 — Adding a New Location

**Example**: Adding Sydney, Australia to the five monitored cities.

### What Happens

1. **Add the location** to the `LOCATIONS` list in [jobs/config.py](../jobs/config.py):
   ```python
   Location(name="Sydney", country="Australia", latitude=-33.8688, longitude=151.2093, timezone="Australia/Sydney")
   ```

2. **Next pipeline run**: The extractor loops over all locations. Sydney is new, so it makes an API call for Sydney's data. The loader upserts a new row into `dim_location` for Sydney (`ON CONFLICT DO NOTHING` does not fire because Sydney doesn't exist yet). All 7 days of Sydney's hourly data are inserted into `fact_weather`.

3. **No schema changes needed**: The star schema handles new locations through the `dim_location` dimension. No DDL changes, no migration scripts.

This is the star schema's composability at work — new locations are data, not schema changes.

---

## Summary Table

| Change | Breaks the pipeline? | Required actions |
|---|---|---|
| API adds a new field | No (ignored if not in HOURLY_VARIABLES) | Add to config, rename map, staging, analytics if desired |
| API removes a field | Possibly (KeyError or ValueError) | Remove from config and rename map; leave column nullable in DB |
| API renames a field | Yes (missing column) | Update config and rename map; migrate staging column |
| API restructures response | Yes (validation fails) | Update extraction.py response parsing |
| Add a new location | No | Add to LOCATIONS config; next run populates dim_location |
| Add a new analytics column | No (existing rows get NULL) | ALTER TABLE ADD COLUMN IF NOT EXISTS; update loader |
| Add a new analytical view | No | Add to views_analytics.sql; CREATE OR REPLACE VIEW |

---

## Key Files

| File | Schema Evolution Role |
|---|---|
| [jobs/config.py](../jobs/config.py) | `HOURLY_VARIABLES` and `LOCATIONS` — controls what is extracted |
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | `validate_response` — detects structural API changes |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | `COLUMN_RENAME_MAP`, `REQUIRED_COLUMNS` — maps API names to schema names |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | SQL transformation — explicit column selection, no `SELECT *` |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | `CREATE TABLE IF NOT EXISTS` — safe to re-run after adding columns |
| [sql/init_staging.sql](../sql/init_staging.sql) | NUMERIC/VARCHAR types — loose staging schema absorbs type changes |
| [sql/views_analytics.sql](../sql/views_analytics.sql) | `CREATE OR REPLACE VIEW` — views updatable without downtime |
