CREATE TABLE IF NOT EXISTS dim_location (
    location_id   SERIAL       PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    country       VARCHAR(100) NOT NULL,
    latitude      NUMERIC(9,6) NOT NULL,
    longitude     NUMERIC(9,6) NOT NULL,
    UNIQUE (location_name, country)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id      SERIAL       PRIMARY KEY,
    full_date    DATE         NOT NULL UNIQUE,
    year         SMALLINT     NOT NULL,
    month        SMALLINT     NOT NULL,
    day          SMALLINT     NOT NULL,
    day_of_week  SMALLINT     NOT NULL,
    day_name     VARCHAR(10)  NOT NULL,
    month_name   VARCHAR(10)  NOT NULL,
    quarter      SMALLINT     NOT NULL,
    week_of_year SMALLINT     NOT NULL,
    is_weekend   BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id       SERIAL       PRIMARY KEY,
    hour          SMALLINT     NOT NULL UNIQUE,
    period_of_day VARCHAR(10)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_weather_condition (
    condition_id SERIAL        PRIMARY KEY,
    weather_code SMALLINT      NOT NULL UNIQUE,
    description  VARCHAR(100)  NOT NULL,
    category     VARCHAR(50)   NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id                BIGSERIAL    PRIMARY KEY,
    location_id            INT          NOT NULL REFERENCES dim_location(location_id),
    date_id                INT          NOT NULL REFERENCES dim_date(date_id),
    time_id                INT          NOT NULL REFERENCES dim_time(time_id),
    condition_id           INT          REFERENCES dim_weather_condition(condition_id),
    temperature_celsius    NUMERIC(5,2),
    temperature_fahrenheit NUMERIC(6,2),
    relative_humidity_pct  NUMERIC(5,2),
    precipitation_mm       NUMERIC(7,2),
    rain_mm                NUMERIC(7,2),
    snowfall_cm            NUMERIC(7,2),
    wind_speed_kmh         NUMERIC(6,2),
    wind_direction_deg     NUMERIC(5,1),
    wind_gusts_kmh         NUMERIC(6,2),
    pressure_hpa           NUMERIC(7,2),
    visibility_m           NUMERIC(8,1),
    uv_index               NUMERIC(4,1),
    cloud_cover_pct        NUMERIC(5,2),
    is_day                 BOOLEAN,
    extracted_at           TIMESTAMPTZ,
    UNIQUE (location_id, date_id, time_id)
);

\i /docker-entrypoint-initdb.d/views_analytics.sql
