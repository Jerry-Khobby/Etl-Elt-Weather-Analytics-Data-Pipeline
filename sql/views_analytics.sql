-- =============================================================================
-- ANALYTICS VIEWS — DATA FLOW DIAGRAM
-- =============================================================================
--
--   VIEWS LAYER
--   ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
--   │  vw_hourly_weather   │  │  vw_daily_weather_   │  │  vw_location_        │
--   │                      │  │  summary             │  │  climate_comparison  │
--   │  Full denormalized   │  │  1 row / day /       │  │  1 row / city        │
--   │  hourly readings     │  │  location            │  │  all-time stats      │
--   │  (all dims joined)   │  │  (avg/min/max/sum)   │  │  (avg/min/max/sum)   │
--   └──────────┬───────────┘  └──────────┬───────────┘  └──────────┬───────────┘
--              │                         │                          │
--   ┌──────────┴───────────┐  ┌──────────┴───────────┐             │
--   │  vw_weather_         │  │  vw_weather_by_      │             │
--   │  condition_frequency │  │  period_of_day       │             │
--   │                      │  │                      │             │
--   │  % of hours per      │  │  avg temp/humidity/  │             │
--   │  weather condition   │  │  wind by time slot   │             │
--   │  per city            │  │  per city            │             │
--   └──────────┬───────────┘  └──────────┬───────────┘             │
--              └───────────────┬──────────┴─────────────────────────┘
--                              │
--                 STAR SCHEMA LAYER
--                 ┌────────────▼────────────┐
--                 │      fact_weather        │
--                 │  PK: fact_id (BIGSERIAL) │
--                 │  FK: location_id         │
--                 │  FK: date_id             │
--                 │  FK: time_id             │
--                 │  FK: condition_id        │
--                 └─┬──────────┬────────┬───┴─────┐
--                   │          │        │          │
--   ┌───────────────▼─┐  ┌─────▼────┐  ┌─▼──────┐  ┌──────────────────────┐
--   │  dim_location   │  │ dim_date │  │dim_time│  │ dim_weather_condition │
--   │                 │  │          │  │        │  │                       │
--   │  location_id PK │  │ date_id  │  │time_id │  │  condition_id PK      │
--   │  location_name  │  │ full_date│  │hour    │  │  weather_code         │
--   │  country        │  │ year     │  │period_ │  │  description          │
--   │  latitude       │  │ month    │  │of_day  │  │  category             │
--   │  longitude      │  │ day      │  │        │  │                       │
--   │                 │  │ day_name │  │        │  │                       │
--   │                 │  │ is_week- │  │        │  │                       │
--   │                 │  │ end      │  │        │  │                       │
--   └─────────────────┘  └──────────┘  └────────┘  └───────────────────────┘
--
-- Safe to re-run: CREATE OR REPLACE VIEW is idempotent.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- vw_hourly_weather
-- Full denormalized view. Joins fact_weather with all four dimension tables to
-- produce one row per hourly reading with every column resolved — no foreign
-- keys exposed. Best starting point for time-series charts in Metabase.
-- Sources: fact_weather ▶ dim_location, dim_date, dim_time, dim_weather_condition
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_hourly_weather AS
SELECT
    f.fact_id,
    l.location_name,
    l.country,
    l.latitude,
    l.longitude,
    d.full_date                  AS date,
    d.year,
    d.month,
    d.month_name,
    d.day,
    d.day_name,
    d.day_of_week,
    d.week_of_year,
    d.quarter,
    d.is_weekend,
    t.hour,
    t.period_of_day,
    wc.description               AS weather_description,
    wc.category                  AS weather_category,
    f.temperature_celsius,
    f.temperature_fahrenheit,
    f.relative_humidity_pct,
    f.precipitation_mm,
    f.rain_mm,
    f.snowfall_cm,
    f.wind_speed_kmh,
    f.wind_direction_deg,
    f.wind_gusts_kmh,
    f.pressure_hpa,
    f.visibility_m,
    f.uv_index,
    f.cloud_cover_pct,
    f.is_day
FROM fact_weather          f
JOIN dim_location          l  ON f.location_id  = l.location_id
JOIN dim_date              d  ON f.date_id       = d.date_id
JOIN dim_time              t  ON f.time_id       = t.time_id
LEFT JOIN dim_weather_condition wc ON f.condition_id = wc.condition_id;


-- -----------------------------------------------------------------------------
-- vw_daily_weather_summary
-- Collapses hourly fact rows into one aggregate row per (location, day).
-- Exposes avg/min/max temperature, total precipitation, and other daily stats.
-- Use for bar charts, day-over-day trend lines, and weekend-vs-weekday splits.
-- Sources: fact_weather ▶ dim_location, dim_date   (GROUP BY location + date)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_daily_weather_summary AS
SELECT
    l.location_name,
    l.country,
    d.full_date                                            AS date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    ROUND(AVG(f.temperature_celsius)::NUMERIC, 2)         AS avg_temp_celsius,
    ROUND(MIN(f.temperature_celsius)::NUMERIC, 2)         AS min_temp_celsius,
    ROUND(MAX(f.temperature_celsius)::NUMERIC, 2)         AS max_temp_celsius,
    ROUND(AVG(f.temperature_fahrenheit)::NUMERIC, 2)      AS avg_temp_fahrenheit,
    ROUND(SUM(f.precipitation_mm)::NUMERIC, 2)            AS total_precipitation_mm,
    ROUND(AVG(f.relative_humidity_pct)::NUMERIC, 2)       AS avg_humidity_pct,
    ROUND(AVG(f.wind_speed_kmh)::NUMERIC, 2)              AS avg_wind_speed_kmh,
    ROUND(MAX(f.wind_gusts_kmh)::NUMERIC, 2)              AS max_wind_gusts_kmh,
    ROUND(AVG(f.uv_index)::NUMERIC, 2)                    AS avg_uv_index,
    ROUND(AVG(f.cloud_cover_pct)::NUMERIC, 2)             AS avg_cloud_cover_pct,
    COUNT(*)                                               AS hourly_readings
FROM fact_weather          f
JOIN dim_location          l  ON f.location_id = l.location_id
JOIN dim_date              d  ON f.date_id     = d.date_id
GROUP BY
    l.location_name, l.country,
    d.full_date, d.year, d.month, d.month_name, d.day_name, d.is_weekend;


-- -----------------------------------------------------------------------------
-- vw_location_climate_comparison
-- Produces one summary row per city across the entire recorded period.
-- Ideal for side-by-side city comparison cards, maps, and ranked tables.
-- Sources: fact_weather ▶ dim_location, dim_date   (GROUP BY location)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_location_climate_comparison AS
SELECT
    l.location_name,
    l.country,
    l.latitude,
    l.longitude,
    ROUND(AVG(f.temperature_celsius)::NUMERIC, 2)         AS avg_temp_celsius,
    ROUND(MIN(f.temperature_celsius)::NUMERIC, 2)         AS min_temp_celsius,
    ROUND(MAX(f.temperature_celsius)::NUMERIC, 2)         AS max_temp_celsius,
    ROUND(AVG(f.relative_humidity_pct)::NUMERIC, 2)       AS avg_humidity_pct,
    ROUND(SUM(f.precipitation_mm)::NUMERIC, 2)            AS total_precipitation_mm,
    ROUND(AVG(f.wind_speed_kmh)::NUMERIC, 2)              AS avg_wind_speed_kmh,
    ROUND(AVG(f.uv_index)::NUMERIC, 2)                    AS avg_uv_index,
    ROUND(AVG(f.cloud_cover_pct)::NUMERIC, 2)             AS avg_cloud_cover_pct,
    COUNT(DISTINCT d.full_date)                            AS days_recorded,
    COUNT(*)                                               AS total_readings
FROM fact_weather          f
JOIN dim_location          l  ON f.location_id = l.location_id
JOIN dim_date              d  ON f.date_id     = d.date_id
GROUP BY l.location_name, l.country, l.latitude, l.longitude;


-- -----------------------------------------------------------------------------
-- vw_weather_condition_frequency
-- Counts how many hours each weather condition occurred per city and expresses
-- it as a percentage of that city's total recorded hours (window function).
-- Use for pie/donut charts showing dominant conditions per location.
-- Sources: fact_weather ▶ dim_location, dim_weather_condition (GROUP BY location + condition)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_weather_condition_frequency AS
SELECT
    l.location_name,
    l.country,
    COALESCE(wc.description, 'Unknown') AS weather_description,
    COALESCE(wc.category,    'Unknown') AS weather_category,
    COUNT(*)                             AS occurrence_count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY l.location_name),
        2
    )                                    AS percentage_of_hours
FROM fact_weather          f
JOIN dim_location          l   ON f.location_id  = l.location_id
LEFT JOIN dim_weather_condition wc ON f.condition_id = wc.condition_id
GROUP BY l.location_name, l.country, wc.description, wc.category;


-- -----------------------------------------------------------------------------
-- vw_weather_by_period_of_day
-- Groups readings into four named time slots (Night / Morning / Afternoon /
-- Evening) and averages temperature, humidity, wind, UV, and cloud cover.
-- Use for grouped bar charts showing how weather shifts through the day.
-- Sources: fact_weather ▶ dim_location, dim_time   (GROUP BY location + period)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_weather_by_period_of_day AS
SELECT
    l.location_name,
    l.country,
    t.period_of_day,
    ROUND(AVG(f.temperature_celsius)::NUMERIC, 2)   AS avg_temp_celsius,
    ROUND(AVG(f.relative_humidity_pct)::NUMERIC, 2) AS avg_humidity_pct,
    ROUND(AVG(f.wind_speed_kmh)::NUMERIC, 2)        AS avg_wind_speed_kmh,
    ROUND(AVG(f.uv_index)::NUMERIC, 2)              AS avg_uv_index,
    ROUND(AVG(f.cloud_cover_pct)::NUMERIC, 2)       AS avg_cloud_cover_pct,
    COUNT(*)                                         AS reading_count
FROM fact_weather  f
JOIN dim_location  l  ON f.location_id = l.location_id
JOIN dim_time      t  ON f.time_id     = t.time_id
GROUP BY l.location_name, l.country, t.period_of_day;
