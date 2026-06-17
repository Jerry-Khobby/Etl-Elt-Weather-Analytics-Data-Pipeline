import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from ..utils.logger import get_logger

logger = get_logger(__name__)


# DDL — star schema                                                    #


CREATE_DIM_LOCATION = """
CREATE TABLE IF NOT EXISTS dim_location (
    location_id   SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    country       VARCHAR(100) NOT NULL,
    latitude      NUMERIC(9,6) NOT NULL,
    longitude     NUMERIC(9,6) NOT NULL,
    UNIQUE (location_name, country)
)
"""

CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE        NOT NULL UNIQUE,
    year         SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    day          SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,
    day_name     VARCHAR(10) NOT NULL,
    month_name   VARCHAR(10) NOT NULL,
    quarter      SMALLINT    NOT NULL,
    week_of_year SMALLINT    NOT NULL,
    is_weekend   BOOLEAN     NOT NULL
)
"""

CREATE_DIM_TIME = """
CREATE TABLE IF NOT EXISTS dim_time (
    time_id       SERIAL PRIMARY KEY,
    hour          SMALLINT   NOT NULL UNIQUE,
    period_of_day VARCHAR(10) NOT NULL
)
"""

CREATE_DIM_WEATHER_CONDITION = """
CREATE TABLE IF NOT EXISTS dim_weather_condition (
    condition_id SERIAL PRIMARY KEY,
    weather_code SMALLINT    NOT NULL UNIQUE,
    description  VARCHAR(100) NOT NULL,
    category     VARCHAR(50)  NOT NULL
)
"""

CREATE_FACT_WEATHER = """
CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id               BIGSERIAL PRIMARY KEY,
    location_id           INT          NOT NULL REFERENCES dim_location(location_id),
    date_id               INT          NOT NULL REFERENCES dim_date(date_id),
    time_id               INT          NOT NULL REFERENCES dim_time(time_id),
    condition_id          INT          REFERENCES dim_weather_condition(condition_id),
    temperature_celsius   NUMERIC(5,2),
    temperature_fahrenheit NUMERIC(6,2),
    relative_humidity_pct NUMERIC(5,2),
    precipitation_mm      NUMERIC(7,2),
    rain_mm               NUMERIC(7,2),
    snowfall_cm           NUMERIC(7,2),
    wind_speed_kmh        NUMERIC(6,2),
    wind_direction_deg    NUMERIC(5,1),
    wind_gusts_kmh        NUMERIC(6,2),
    pressure_hpa          NUMERIC(7,2),
    visibility_m          NUMERIC(8,1),
    uv_index              NUMERIC(4,1),
    cloud_cover_pct       NUMERIC(5,2),
    is_day                BOOLEAN,
    extracted_at          TIMESTAMPTZ,
    UNIQUE (location_id, date_id, time_id)
)
"""

ALL_DDL = [
    CREATE_DIM_LOCATION,
    CREATE_DIM_DATE,
    CREATE_DIM_TIME,
    CREATE_DIM_WEATHER_CONDITION,
    CREATE_FACT_WEATHER,
]


class WeatherDataLoader:
    """Loads a transformed weather DataFrame into a PostgreSQL star schema."""

    def __init__(self):
        self._engine = self._build_engine()

    
    # Public interface                                                      #
    

    def initialize_schema(self) -> None:
        with self._engine.begin() as conn:
            for ddl in ALL_DDL:
                conn.execute(text(ddl))
        logger.info("Star schema initialized (tables created where absent)")

    def load(self, df: pd.DataFrame) -> None:
        logger.info("Load started | rows=%d", len(df))
        self._load_dim_location(df)
        self._load_dim_date(df)
        self._load_dim_time(df)
        self._load_dim_weather_condition(df)
        self._load_fact_weather(df)
        logger.info("Load complete")

    
    # Dimension loaders                                                    #
    

    def _load_dim_location(self, df: pd.DataFrame) -> None:
        locations = (
            df[["location_name", "location_country", "latitude", "longitude"]]
            .drop_duplicates()
        )
        sql = text("""
            INSERT INTO dim_location (location_name, country, latitude, longitude)
            VALUES (:name, :country, :latitude, :longitude)
            ON CONFLICT (location_name, country) DO NOTHING
        """)
        with self._engine.begin() as conn:
            for _, row in locations.iterrows():
                conn.execute(sql, {
                    "name": row["location_name"],
                    "country": row["location_country"],
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                })
        logger.info("dim_location: upserted %d row(s)", len(locations))

    def _load_dim_date(self, df: pd.DataFrame) -> None:
        date_cols = ["date", "year", "month", "day", "day_of_week",
                     "week_of_year", "quarter", "is_weekend"]
        dates = df[date_cols].drop_duplicates(subset=["date"])
        sql = text("""
            INSERT INTO dim_date
                (full_date, year, month, day, day_of_week, day_name,
                 month_name, quarter, week_of_year, is_weekend)
            VALUES
                (:full_date, :year, :month, :day, :day_of_week, :day_name,
                 :month_name, :quarter, :week_of_year, :is_weekend)
            ON CONFLICT (full_date) DO NOTHING
        """)
        with self._engine.begin() as conn:
            for _, row in dates.iterrows():
                ts = pd.Timestamp(row["date"])
                conn.execute(sql, {
                    "full_date": row["date"],
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "day": int(row["day"]),
                    "day_of_week": int(row["day_of_week"]),
                    "day_name": ts.strftime("%A"),
                    "month_name": ts.strftime("%B"),
                    "quarter": int(row["quarter"]),
                    "week_of_year": int(row["week_of_year"]),
                    "is_weekend": bool(row["is_weekend"]),
                })
        logger.info("dim_date: upserted %d row(s)", len(dates))

    def _load_dim_time(self, df: pd.DataFrame) -> None:
        times = df[["hour", "period_of_day"]].drop_duplicates(subset=["hour"])
        sql = text("""
            INSERT INTO dim_time (hour, period_of_day)
            VALUES (:hour, :period_of_day)
            ON CONFLICT (hour) DO NOTHING
        """)
        with self._engine.begin() as conn:
            for _, row in times.iterrows():
                conn.execute(sql, {
                    "hour": int(row["hour"]),
                    "period_of_day": row["period_of_day"],
                })
        logger.info("dim_time: upserted %d row(s)", len(times))

    def _load_dim_weather_condition(self, df: pd.DataFrame) -> None:
        if "weather_code" not in df.columns:
            return
        conditions = (
            df[["weather_code", "weather_description", "weather_category"]]
            .drop_duplicates(subset=["weather_code"])
            .dropna(subset=["weather_code"])
        )
        sql = text("""
            INSERT INTO dim_weather_condition (weather_code, description, category)
            VALUES (:code, :description, :category)
            ON CONFLICT (weather_code) DO NOTHING
        """)
        with self._engine.begin() as conn:
            for _, row in conditions.iterrows():
                conn.execute(sql, {
                    "code": int(row["weather_code"]),
                    "description": row["weather_description"],
                    "category": row["weather_category"],
                })
        logger.info("dim_weather_condition: upserted %d row(s)", len(conditions))

    
    # Fact loader                                                          #
    

    def _load_fact_weather(self, df: pd.DataFrame) -> None:
        location_ids = self._fetch_location_ids()
        date_ids = self._fetch_date_ids()
        time_ids = self._fetch_time_ids()
        condition_ids = self._fetch_condition_ids()

        fact_df = df.copy()
        fact_df["location_id"] = fact_df.apply(
            lambda r: location_ids.get((r["location_name"], r["location_country"])),
            axis=1,
        )
        fact_df["date_id"] = fact_df["date"].map(date_ids)
        fact_df["time_id"] = fact_df["hour"].map(time_ids)
        fact_df["condition_id"] = fact_df["weather_code"].apply(
            lambda c: condition_ids.get(int(c)) if pd.notna(c) else None
        )

        sql = text("""
            INSERT INTO fact_weather (
                location_id, date_id, time_id, condition_id,
                temperature_celsius, temperature_fahrenheit,
                relative_humidity_pct, precipitation_mm, rain_mm, snowfall_cm,
                wind_speed_kmh, wind_direction_deg, wind_gusts_kmh,
                pressure_hpa, visibility_m, uv_index, cloud_cover_pct,
                is_day, extracted_at
            ) VALUES (
                :location_id, :date_id, :time_id, :condition_id,
                :temperature_celsius, :temperature_fahrenheit,
                :relative_humidity_pct, :precipitation_mm, :rain_mm, :snowfall_cm,
                :wind_speed_kmh, :wind_direction_deg, :wind_gusts_kmh,
                :pressure_hpa, :visibility_m, :uv_index, :cloud_cover_pct,
                :is_day, :extracted_at
            )
            ON CONFLICT (location_id, date_id, time_id) DO NOTHING
        """)

        with self._engine.begin() as conn:
            for record in fact_df.to_dict(orient="records"):
                conn.execute(sql, _to_fact_row(record))

        logger.info("fact_weather: inserted %d row(s)", len(fact_df))

    
    # FK lookup helpers                                                    #
    

    def _fetch_location_ids(self) -> dict:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT location_name, country, location_id FROM dim_location")
            ).fetchall()
        return {(r.location_name, r.country): r.location_id for r in rows}

    def _fetch_date_ids(self) -> dict:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT full_date, date_id FROM dim_date")
            ).fetchall()
        return {r.full_date: r.date_id for r in rows}

    def _fetch_time_ids(self) -> dict:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT hour, time_id FROM dim_time")
            ).fetchall()
        return {r.hour: r.time_id for r in rows}

    def _fetch_condition_ids(self) -> dict:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT weather_code, condition_id FROM dim_weather_condition")
            ).fetchall()
        return {r.weather_code: r.condition_id for r in rows}

    
    # Setup                                                                #
    

    def _build_engine(self) -> Engine:
        connection_string = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        try:
            engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info(
                "Database engine ready | host=%s port=%s db=%s",
                DB_HOST, DB_PORT, DB_NAME,
            )
            return engine
        except Exception as error:
            logger.error("Failed to create database engine: %s", error)
            raise


def _to_fact_row(record: dict) -> dict:
    return {
        "location_id": record.get("location_id"),
        "date_id": record.get("date_id"),
        "time_id": record.get("time_id"),
        "condition_id": record.get("condition_id"),
        "temperature_celsius": record.get("temperature_celsius"),
        "temperature_fahrenheit": record.get("temperature_fahrenheit"),
        "relative_humidity_pct": record.get("relative_humidity_pct"),
        "precipitation_mm": record.get("precipitation"),
        "rain_mm": record.get("rain"),
        "snowfall_cm": record.get("snowfall"),
        "wind_speed_kmh": record.get("wind_speed_kmh"),
        "wind_direction_deg": record.get("wind_direction_deg"),
        "wind_gusts_kmh": record.get("wind_gusts_kmh"),
        "pressure_hpa": record.get("pressure_hpa"),
        "visibility_m": record.get("visibility"),
        "uv_index": record.get("uv_index"),
        "cloud_cover_pct": record.get("cloud_cover"),
        "is_day": record.get("is_day"),
        "extracted_at": record.get("extracted_at"),
    }
