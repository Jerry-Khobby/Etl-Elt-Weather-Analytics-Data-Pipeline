# Slowly Changing Dimensions (SCD)

## Overview

A **Slowly Changing Dimension (SCD)** describes how a dimension table handles changes to its attribute values over time. The challenge: if a dimension attribute changes (e.g., a city is reclassified into a different region, or coordinates are corrected), do you overwrite the old value, keep both versions, or something else? The answer affects whether historical fact rows remain accurate to the world as it was when the data was recorded.

---

## SCD Types — A Reference

| Type | Strategy | Historical accuracy | Storage |
|---|---|---|---|
| **Type 0** | Never update — original value is permanent | n/a (values never change) | Lowest |
| **Type 1** | Overwrite — only current value kept | Lost (old value gone) | Low |
| **Type 2** | Add row — new row for each version with effective dates | Full (each version preserved) | High |
| **Type 3** | Add column — store current and one prior value | Partial (one version back) | Medium |

---

## What This Project Implements

This project implements **SCD Type 1** for all dimensions. The upsert pattern in [jobs/etl/load.py](../jobs/etl/load.py) uses `ON CONFLICT DO NOTHING`, which means:

- If a dimension row already exists (matched by natural key), the existing row is **left unchanged**.
- If no matching row exists, the new row is **inserted**.

Effectively, dimension attributes are written once and never updated. The project does not overwrite on conflict — it ignores the conflict entirely. This is a **conservative Type 1**: attributes are immutable after the first insert.

---

## Dimension-by-Dimension Analysis

### dim_location — Static, No SCD Concern in Practice

```sql
CREATE TABLE IF NOT EXISTS dim_location (
    location_id   SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    country       VARCHAR(100) NOT NULL,
    latitude      NUMERIC(9,6),
    longitude     NUMERIC(9,6),
    UNIQUE (location_name, country)
);
```

**Current implementation**: The five locations (Accra, London, New York, Tokyo, Berlin) are hardcoded in [jobs/config.py](../jobs/config.py). Their coordinates and country codes never change between pipeline runs.

**SCD concern**: If the Open-Meteo API corrected a coordinate (e.g., London's latitude shifted by a rounding correction), the current `ON CONFLICT DO NOTHING` would **silently ignore** the correction. The old coordinate value would remain in `dim_location` forever.

**Why Type 1 is sufficient here**: Geographic coordinates for major cities are stable over human timescales. A rounding correction to a latitude would not meaningfully affect any analytical query. If the pipeline were tracking administrative boundaries or region classifications that genuinely change (e.g., a neighbourhood being reclassified), Type 2 would be warranted. For fixed world cities, it is not.

### dim_date — Immutable by Nature (Type 0)

```sql
UNIQUE (full_date)
```

Date attributes (year, month, day, day_of_week, is_weekend) are mathematical properties of a calendar date. They **cannot change**. June 15, 2026 will always be a Monday in week 25. This dimension is effectively Type 0 — no SCD mechanism is needed because no SCD situation can arise.

### dim_time — Immutable by Nature (Type 0)

```sql
UNIQUE (hour)
```

The 24 hours of a day and their period-of-day classification (Night/Morning/Afternoon/Evening) are fixed by definition. Hour 14 will always be "Afternoon". This dimension is populated once (24 rows) and never touched again.

### dim_weather_condition — Stable Reference Table (Type 1)

```sql
UNIQUE (weather_code)
```

WMO weather codes are an international standard. Code `61` will always mean "Slight rain". New codes could theoretically be added to the WMO standard, but existing codes are never reassigned.

**SCD concern**: If the project's WMO mapping (the Python dictionary that maps codes to descriptions) is updated to correct a description (e.g., a typo fix or more precise wording), the current `ON CONFLICT DO NOTHING` would **not** apply the correction to existing rows. The old description would persist.

**To apply a Type 1 correction for weather condition descriptions**:

```sql
UPDATE dim_weather_condition
SET description = 'Moderate rain',
    category    = 'Rain'
WHERE weather_code = 61;
```

This is a manual correction — the pipeline would not propagate it automatically.

---

## If Type 2 Were Implemented

SCD Type 2 would be relevant if location attributes could meaningfully change and historical accuracy mattered. For example, if `dim_location` tracked a "climate zone" attribute that could change as regions are reclassified, Type 2 would preserve which climate zone was in effect when each historical reading was recorded.

A Type 2 `dim_location` would look like:

```sql
CREATE TABLE dim_location (
    location_id    SERIAL PRIMARY KEY,       -- surrogate key
    location_name  VARCHAR(100) NOT NULL,
    country        VARCHAR(100) NOT NULL,
    latitude       NUMERIC(9,6),
    longitude      NUMERIC(9,6),
    -- Type 2 versioning columns:
    valid_from     DATE NOT NULL,
    valid_to       DATE,                     -- NULL means currently active
    is_current     BOOLEAN NOT NULL DEFAULT TRUE,
    -- natural key only within a version window:
    UNIQUE (location_name, country, valid_from)
);
```

A change to London's attributes would insert a new row:

```sql
-- Close the current version
UPDATE dim_location
SET valid_to   = '2026-06-18',
    is_current = FALSE
WHERE location_name = 'London'
  AND country       = 'UK'
  AND is_current    = TRUE;

-- Open a new version
INSERT INTO dim_location (location_name, country, latitude, longitude, valid_from, valid_to, is_current)
VALUES ('London', 'UK', 51.5074, -0.1278, '2026-06-18', NULL, TRUE);
```

Fact rows created before the change would still reference the old `location_id` (the old surrogate key), preserving historical context. This is the power of Type 2 — but it adds significant complexity: every query that joins `dim_location` must filter for the correct version window.

**Why this complexity was not introduced**: The five fixed cities in this project have no attributes that change. Adding Type 2 machinery for a dimension that will never trigger a version change would be over-engineering (YAGNI). The documentation of the approach is more valuable than implementing it for a use case that cannot occur.

---

## Implementing Type 2 If Needed — Migration Path

If the project were extended to track changing location attributes (e.g., time zones, country groupings, monitoring station IDs), the migration path from Type 1 to Type 2 is:

1. **Add versioning columns** to `dim_location`:
   ```sql
   ALTER TABLE dim_location ADD COLUMN valid_from DATE;
   ALTER TABLE dim_location ADD COLUMN valid_to   DATE;
   ALTER TABLE dim_location ADD COLUMN is_current BOOLEAN DEFAULT TRUE;
   ```

2. **Backfill existing rows**:
   ```sql
   UPDATE dim_location SET valid_from = '2026-01-01', is_current = TRUE;
   ```

3. **Update the loader** in [jobs/etl/load.py](../jobs/etl/load.py) to:
   - Close the current version (`UPDATE SET valid_to = today, is_current = FALSE`) when an attribute changes.
   - Insert a new row as the active version.
   - Keep the old `location_id` referenced by historical fact rows.

4. **Update all queries and views** in [sql/views_analytics.sql](../sql/views_analytics.sql) to filter `WHERE is_current = TRUE` (or join on a date range) to avoid returning multiple versions of the same location.

---

## Summary

| Dimension | SCD Type Applied | Rationale |
|---|---|---|
| `dim_location` | Type 1 (ignore on conflict) | City coordinates are stable; correction via manual SQL if needed |
| `dim_date` | Type 0 (immutable) | Calendar dates are mathematical constants — they cannot change |
| `dim_time` | Type 0 (immutable) | 24 hours, fixed period labels — no change possible |
| `dim_weather_condition` | Type 1 (ignore on conflict) | WMO codes are stable; description updates require manual correction |

The conservative choice (Type 1 / ignore on conflict) is justified by the stability of all dimension attributes in this dataset. The `ON CONFLICT DO NOTHING` pattern is explicit and traceable, making any future upgrade to Type 2 a straightforward schema and loader change.

---

## Key Files

| File | SCD Role |
|---|---|
| [jobs/etl/load.py](../jobs/etl/load.py) | `ON CONFLICT (natural_key) DO NOTHING` — Type 1 implementation |
| [jobs/config.py](../jobs/config.py) | Hardcoded location list — source of dim_location rows |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | Dimension table DDL with natural key unique constraints |
