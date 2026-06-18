# Star Schema Design Rationale

## Overview

The analytics layer of this project uses a **star schema** — a dimensional model where a central fact table holds measurements and is surrounded by dimension tables that describe the context of each measurement. This design is optimized for analytical queries (aggregations, filters, drill-downs) rather than transactional queries (inserts, updates, lookups by ID).

---

## Schema Diagram

```
                    ┌─────────────────┐
                    │   dim_location  │
                    │─────────────────│
                    │ location_id  PK │
                    │ location_name   │
                    │ country         │
                    │ latitude        │
                    │ longitude       │
                    └────────┬────────┘
                             │
┌──────────────────┐         │         ┌────────────────────────┐
│    dim_date      │         │         │   dim_weather_condition │
│──────────────────│         │         │────────────────────────│
│ date_id       PK │         │         │ condition_id        PK │
│ full_date        │         │         │ weather_code           │
│ year             │         │         │ description            │
│ month            ├─────────┤         │ category               │
│ day              │         │         └──────────┬─────────────┘
│ day_of_week      │    ┌────▼─────────────────────▼──────────┐
│ day_name         │    │           fact_weather               │
│ month_name       │    │──────────────────────────────────────│
│ quarter          ├────┤ fact_id         PK  (BIGSERIAL)      │
│ week_of_year     │    │ location_id     FK → dim_location    │
│ is_weekend       │    │ date_id         FK → dim_date        │
└──────────────────┘    │ time_id         FK → dim_time        │
                        │ condition_id    FK → dim_weather_cond│
┌──────────────────┐    │ temperature_celsius                  │
│    dim_time      │    │ temperature_fahrenheit               │
│──────────────────│    │ relative_humidity_pct                │
│ time_id       PK ├────┤ precipitation_mm                     │
│ hour             │    │ rain_mm                              │
│ period_of_day    │    │ snowfall_cm                          │
└──────────────────┘    │ wind_speed_kmh                       │
                        │ wind_direction_deg                   │
                        │ wind_gusts_kmh                       │
                        │ pressure_hpa                         │
                        │ visibility_m                         │
                        │ uv_index                             │
                        │ cloud_cover_pct                      │
                        │ is_day                               │
                        │ extracted_at                         │
                        │ UNIQUE (location_id, date_id, time_id)│
                        └──────────────────────────────────────┘
```

All DDL is in [sql/init_analytics.sql](../sql/init_analytics.sql).

---

## The Grain of the Fact Table

The **grain** of a fact table defines what one row represents. Choosing the right grain is the most important decision in star schema design.

**The grain of `fact_weather` is: one hourly weather reading per location.**

This means each row answers the question: *"What were the weather conditions at a specific city during a specific hour on a specific day?"*

**Why hourly grain?**

The Open-Meteo API provides **hourly data** — each API response contains one reading per hour for each requested location. Matching the grain to the source data's natural granularity avoids lossy aggregation at load time and gives analysts maximum flexibility. If you need daily averages, you aggregate at query time (or use the `vw_daily_weather_summary` view). If you need hourly charts, the data is already at the right level.

**The grain is enforced by a unique constraint:**

```sql
UNIQUE (location_id, date_id, time_id)
```

This composite constraint means the database will reject any row that attempts to insert a second reading for the same (city, date, hour) combination. The grain is not just a design decision — it is structurally enforced.

---

## Dimension Design

### dim_location

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

**Why this dimension exists**: Every weather reading belongs to a city. Storing city attributes (country, coordinates) in the fact table would repeat the same values thousands of times. The dimension stores them once and the fact table references by ID.

**Expected cardinality**: 5 rows (Accra, London, New York, Tokyo, Berlin). This dimension is tiny and static.

**Natural key**: `(location_name, country)` — this is the business identifier. Two cities with the same name in different countries are distinct (e.g., London, UK vs London, Ontario). The unique constraint on this pair prevents accidental duplicates.

### dim_date

```sql
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE NOT NULL UNIQUE,
    year         SMALLINT,
    month        SMALLINT,
    day          SMALLINT,
    day_of_week  SMALLINT,    -- 0 = Monday, 6 = Sunday
    day_name     VARCHAR(10),
    month_name   VARCHAR(10),
    quarter      SMALLINT,
    week_of_year SMALLINT,
    is_weekend   BOOLEAN,
    UNIQUE (full_date)
);
```

**Why this dimension exists**: Analysts frequently filter and group by date attributes — "show me June data", "compare weekdays to weekends", "aggregate by quarter". Storing all these pre-computed attributes in a date dimension eliminates repeated `EXTRACT()` calls at query time and makes SQL simpler to write.

**Expected cardinality**: One row per calendar day the pipeline runs. For a year of daily runs, ~365 rows. This dimension grows at one row per day.

**Natural key**: `full_date` — a calendar date is its own unique identifier. There can be only one June 15, 2026.

**Day-of-week convention**: The project uses `0 = Monday, 6 = Sunday` (ISO 8601 / Pandas convention). This is explicitly encoded in both the ETL transformer (`(DOW + 6) % 7` conversion from PostgreSQL's Sunday=0 convention) and the ELT SQL transformation, ensuring consistency across both pipelines.

### dim_time

```sql
CREATE TABLE IF NOT EXISTS dim_time (
    time_id      SERIAL PRIMARY KEY,
    hour         SMALLINT NOT NULL UNIQUE,  -- 0-23
    period_of_day VARCHAR(20)               -- Night/Morning/Afternoon/Evening
);
```

**Why this dimension exists**: Analysts want to filter by "morning readings" or "nighttime temperatures" without writing `CASE WHEN` in every query. The pre-computed `period_of_day` label makes this trivial.

**Expected cardinality**: Exactly 24 rows — one per hour of the day. This dimension is populated once and never changes.

**Natural key**: `hour` (0–23). Hours are universally unique within a day.

**Period classification**:
- Night: 00:00–05:59
- Morning: 06:00–11:59
- Afternoon: 12:00–17:59
- Evening: 18:00–23:59

This classification is computed in the transformer and stored, not recomputed at query time.

### dim_weather_condition

```sql
CREATE TABLE IF NOT EXISTS dim_weather_condition (
    condition_id  SERIAL PRIMARY KEY,
    weather_code  SMALLINT NOT NULL UNIQUE,  -- WMO standard code
    description   VARCHAR(100),              -- e.g., "Heavy rain"
    category      VARCHAR(50),               -- e.g., "Rain"
);
```

**Why this dimension exists**: The Open-Meteo API returns `weather_code` as a WMO (World Meteorological Organization) numeric code (e.g., `61` = "Slight rain"). Storing the raw code in the fact table would require analysts to know the WMO code table. The dimension stores the human-readable `description` and a broader `category`, enabling filter queries like `WHERE category = 'Rain'` without a lookup table join.

**Expected cardinality**: ~100 rows — one per distinct WMO weather code observed. The WMO code table has ~100 defined codes; in practice, only a subset will appear in this dataset.

**Natural key**: `weather_code` — WMO codes are internationally standardized and unique.

---

## Surrogate Keys vs Natural Keys

The schema uses **both**:

| Key Type | Used For | Example |
|---|---|---|
| **Surrogate key** (SERIAL) | Primary key of every dimension | `location_id`, `date_id`, `time_id`, `condition_id` |
| **Natural key** | Unique constraint on dimensions | `(location_name, country)`, `full_date`, `hour`, `weather_code` |

### Why Surrogate Keys for PKs

1. **Compact foreign key storage**: An integer `location_id` in the fact table is 4 bytes. A `(location_name, country)` composite FK would be ~200 bytes per row. The fact table will grow to millions of rows — this matters.
2. **Join performance**: Integer equi-joins (`fact.location_id = dim.location_id`) are faster than string joins.
3. **Natural keys can change**: If a city's name is spelled differently in a new data source, the surrogate key protects the fact table from that change. The dimension row is updated; the fact table's integer FK values are unaffected.
4. **Simplicity in loading code**: The loader can do a simple `SELECT location_id FROM dim_location WHERE location_name = %s AND country = %s` to resolve the surrogate key.

### Why Natural Keys as Unique Constraints

Natural keys enforce business-level uniqueness at the database layer. Without `UNIQUE (location_name, country)`, the loader could accidentally insert "London, UK" twice and create two `location_id` values for the same city, corrupting every join.

The natural key unique constraint acts as the **idempotency guard for dimension upserts** — `ON CONFLICT (location_name, country) DO NOTHING` uses this constraint.

---

## Fact Table Measures

The 13 measurement columns in `fact_weather` are:

| Column | Unit | Source |
|---|---|---|
| `temperature_celsius` | °C | Direct from API (`temperature_2m`) |
| `temperature_fahrenheit` | °F | Derived: `temp_c × 9/5 + 32` |
| `relative_humidity_pct` | % (0–100) | Direct from API |
| `precipitation_mm` | mm | Direct from API |
| `rain_mm` | mm | Direct from API |
| `snowfall_cm` | cm | Direct from API |
| `wind_speed_kmh` | km/h | Direct from API |
| `wind_direction_deg` | degrees (0–360) | Direct from API |
| `wind_gusts_kmh` | km/h | Direct from API |
| `pressure_hpa` | hPa | Direct from API |
| `visibility_m` | metres | Direct from API |
| `uv_index` | index (0–20) | Direct from API |
| `cloud_cover_pct` | % (0–100) | Direct from API |
| `is_day` | boolean | Direct from API |

These are all **additive or semi-additive measures**:
- Temperatures are **semi-additive** (average meaningfully across locations/time, but summing makes no sense).
- Precipitation, rain, snowfall are **fully additive** (totals across time and locations are meaningful).
- Direction and UV are **non-additive** (aggregation produces meaningless values; use `AVG`, not `SUM`).

---

## Why Not a Snowflake Schema

A **snowflake schema** would normalize dimension tables further — for example, splitting `dim_date` into `dim_year` → `dim_month` → `dim_date`. This was not done because:

1. **Dimension tables are already small**: `dim_date` with 365 rows and all date attributes in one table is fast and simple. Normalizing it saves virtually no storage.
2. **Simpler queries**: Star schema queries join the fact table directly to each dimension. Snowflake schemas require chained joins (fact → dim_date → dim_month → dim_year), making queries more complex.
3. **Query performance**: Fewer joins means faster analytical queries. Metabase and similar BI tools generate simple SQL; star schemas play better with auto-generated queries.

---

## Analytical Views Built on the Schema

Five pre-built views in [sql/views_analytics.sql](../sql/views_analytics.sql) demonstrate the schema's query patterns:

| View | Aggregation Level | Key Use Case |
|---|---|---|
| `vw_hourly_weather` | One row per fact row | Time-series charts, hourly drill-downs |
| `vw_daily_weather_summary` | Rolled up by location + date | Daily trend lines, day-over-day comparison |
| `vw_location_climate_comparison` | Rolled up by location | City comparison cards |
| `vw_weather_condition_frequency` | Rolled up by location + condition | Pie/donut charts |
| `vw_weather_by_period_of_day` | Rolled up by location + period | Morning vs evening comparisons |

All views use `CREATE OR REPLACE VIEW` — they are idempotent and always reflect the current schema.

---

## Key Files

| File | Star Schema Role |
|---|---|
| [sql/init_analytics.sql](../sql/init_analytics.sql) | Full DDL for all dimension and fact tables |
| [sql/views_analytics.sql](../sql/views_analytics.sql) | Five analytical views built on the star schema |
| [jobs/etl/load.py](../jobs/etl/load.py) | Dimension upserts and fact table inserts |
| [models/schema.sql](../models/schema.sql) | Reference DDL (documentation copy) |
