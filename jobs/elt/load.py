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
from ..utils.logger import get_logger

logger = get_logger(__name__)

STAGING_TABLE = "weather_raw_staging"

CREATE_STAGING_TABLE = """
CREATE TABLE IF NOT EXISTS weather_raw_staging (
    id                   BIGSERIAL    PRIMARY KEY,
    "timestamp"          VARCHAR(50),
    temperature_2m       NUMERIC,
    relative_humidity_2m NUMERIC,
    precipitation        NUMERIC,
    rain                 NUMERIC,
    snowfall             NUMERIC,
    weather_code         SMALLINT,
    cloud_cover          NUMERIC,
    wind_speed_10m       NUMERIC,
    wind_direction_10m   NUMERIC,
    wind_gusts_10m       NUMERIC,
    surface_pressure     NUMERIC,
    visibility           NUMERIC,
    uv_index             NUMERIC,
    is_day               SMALLINT,
    location_name        VARCHAR(100),
    location_country     VARCHAR(100),
    latitude             NUMERIC(9,6),
    longitude            NUMERIC(9,6),
    extracted_at         VARCHAR(50),
    loaded_at            TIMESTAMPTZ  DEFAULT NOW(),
    is_processed         BOOLEAN      DEFAULT FALSE
)
"""


class StagingLoader:
    """Loads raw extracted weather data into the PostgreSQL staging table."""

    def __init__(self):
        self._engine = self._build_engine()

    def initialize_schema(self) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(CREATE_STAGING_TABLE))
        logger.info("Staging schema initialized")

    def load(self, raw_df: pd.DataFrame) -> int:
        df = raw_df.rename(columns={"time": "timestamp"})
        col_names = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join(f":{c}" for c in df.columns)
        insert_sql = text(
            f"INSERT INTO {STAGING_TABLE} ({col_names}) VALUES ({placeholders})"
        )
        records = df.to_dict(orient="records")
        try:
            with self._engine.begin() as conn:
                conn.execute(insert_sql, records)
            logger.info("Staging load complete | rows=%d", len(df))
            return len(df)
        except Exception as error:
            logger.exception("Staging load failed: %s", error)
            raise

    def fetch_unprocessed_ids(self) -> list:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id FROM weather_raw_staging WHERE is_processed = FALSE")
            ).fetchall()
        return [r.id for r in rows]

    def mark_processed(self, row_ids: list) -> None:
        if not row_ids:
            return
        sql = text(
            "UPDATE weather_raw_staging SET is_processed = TRUE WHERE id = ANY(:ids)"
        )
        with self._engine.begin() as conn:
            conn.execute(sql, {"ids": row_ids})
        logger.info("Marked %d staging rows as processed", len(row_ids))

    def _build_engine(self) -> Engine:
        connection_string = (
            f"postgresql+psycopg2://{STAGING_DB_USER}:{STAGING_DB_PASSWORD}"
            f"@{STAGING_DB_HOST}:{STAGING_DB_PORT}/{STAGING_DB_NAME}"
        )
        try:
            engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info(
                "Staging engine ready | host=%s port=%s db=%s",
                STAGING_DB_HOST,
                STAGING_DB_PORT,
                STAGING_DB_NAME,
            )
            return engine
        except Exception as error:
            logger.exception("Failed to create staging engine: %s", error)
            raise
