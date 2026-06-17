import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..config import (
    STAGING_DB_HOST,
    STAGING_DB_NAME,
    STAGING_DB_PASSWORD,
    STAGING_DB_PORT,
    STAGING_DB_USER,
)
from ..etl.transform import _resolve_weather_category, _resolve_weather_description
from ..utils.logger import get_logger

logger = get_logger(__name__)

# SQL that reads unprocessed staging rows and applies all transformations inside
# the database: type casting, column renames, derived date/time fields,
# Fahrenheit conversion, and period-of-day classification.
# day_of_week uses ((DOW + 6) % 7) to match pandas 0=Monday convention.
TRANSFORM_UNPROCESSED_SQL = """
SELECT
    id,
    "timestamp"::TIMESTAMP                                                      AS timestamp,
    temperature_2m                                                              AS temperature_celsius,
    relative_humidity_2m                                                        AS relative_humidity_pct,
    COALESCE(precipitation, 0.0)                                                AS precipitation,
    COALESCE(rain, 0.0)                                                         AS rain,
    COALESCE(snowfall, 0.0)                                                     AS snowfall,
    weather_code,
    cloud_cover,
    wind_speed_10m                                                              AS wind_speed_kmh,
    wind_direction_10m                                                          AS wind_direction_deg,
    wind_gusts_10m                                                              AS wind_gusts_kmh,
    surface_pressure                                                            AS pressure_hpa,
    visibility,
    uv_index,
    (is_day != 0)                                                               AS is_day,
    location_name,
    location_country,
    latitude,
    longitude,
    extracted_at,
    "timestamp"::DATE                                                           AS date,
    EXTRACT(HOUR    FROM "timestamp"::TIMESTAMP)::SMALLINT                     AS hour,
    EXTRACT(YEAR    FROM "timestamp"::TIMESTAMP)::SMALLINT                     AS year,
    EXTRACT(MONTH   FROM "timestamp"::TIMESTAMP)::SMALLINT                     AS month,
    EXTRACT(DAY     FROM "timestamp"::TIMESTAMP)::SMALLINT                     AS day,
    ((EXTRACT(DOW FROM "timestamp"::TIMESTAMP)::INTEGER + 6) % 7)::SMALLINT   AS day_of_week,
    EXTRACT(WEEK    FROM "timestamp"::TIMESTAMP)::SMALLINT                     AS week_of_year,
    EXTRACT(QUARTER FROM "timestamp"::TIMESTAMP)::SMALLINT                     AS quarter,
    CASE WHEN EXTRACT(DOW FROM "timestamp"::TIMESTAMP) IN (0, 6)
         THEN TRUE ELSE FALSE
    END                                                                         AS is_weekend,
    ROUND((temperature_2m * 9.0 / 5.0 + 32)::NUMERIC, 2)                     AS temperature_fahrenheit,
    CASE
        WHEN EXTRACT(HOUR FROM "timestamp"::TIMESTAMP) < 6  THEN 'Night'
        WHEN EXTRACT(HOUR FROM "timestamp"::TIMESTAMP) < 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM "timestamp"::TIMESTAMP) < 18 THEN 'Afternoon'
        ELSE 'Evening'
    END                                                                         AS period_of_day
FROM weather_raw_staging
WHERE is_processed  = FALSE
  AND "timestamp"   IS NOT NULL
  AND temperature_2m IS NOT NULL
  AND location_name  IS NOT NULL
"""


class StagingTransformer:
    """Transforms raw staged weather data using SQL into analytics-ready rows.

    Connects to the staging DB and executes a single SQL SELECT that performs
    all type casts, renames, and derived-field calculations inside PostgreSQL.
    Weather description and category are added afterwards via Python lookup
    because the full code map is already defined in the ETL transform module.
    """

    def __init__(self):
        self._engine = self._build_engine()

    def transform(self) -> tuple:
        """Returns (transformed_df, staging_row_ids).

        The caller uses staging_row_ids to mark those rows as processed
        after loading into the analytics database.
        """
        logger.info("ELT SQL transformation started")
        df = self._run_sql_transform()
        if df.empty:
            logger.info("No unprocessed rows found in staging")
            return df, []
        row_ids = df["id"].tolist()
        df = df.drop(columns=["id"])
        df = self._add_weather_labels(df)
        logger.info("ELT transformation complete | rows=%d", len(df))
        return df, row_ids

    def _run_sql_transform(self) -> pd.DataFrame:
        with self._engine.connect() as conn:
            return pd.read_sql(text(TRANSFORM_UNPROCESSED_SQL), conn)

    def _add_weather_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        df["weather_description"] = df["weather_code"].map(_resolve_weather_description)
        df["weather_category"] = df["weather_code"].map(_resolve_weather_category)
        return df

    def _build_engine(self) -> Engine:
        connection_string = (
            f"postgresql+psycopg2://{STAGING_DB_USER}:{STAGING_DB_PASSWORD}"
            f"@{STAGING_DB_HOST}:{STAGING_DB_PORT}/{STAGING_DB_NAME}"
        )
        try:
            engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info(
                "Staging transformer engine ready | host=%s port=%s db=%s",
                STAGING_DB_HOST,
                STAGING_DB_PORT,
                STAGING_DB_NAME,
            )
            return engine
        except Exception as error:
            logger.exception("Failed to create staging transformer engine: %s", error)
            raise
