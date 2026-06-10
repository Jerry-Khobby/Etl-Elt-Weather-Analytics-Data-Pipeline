import pandas as pd

from ..utils.logger import get_logger

logger = get_logger(__name__)

COLUMN_RENAME_MAP = {
    "time": "timestamp",
    "temperature_2m": "temperature_celsius",
    "relative_humidity_2m": "relative_humidity_pct",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg",
    "wind_gusts_10m": "wind_gusts_kmh",
    "surface_pressure": "pressure_hpa",
}

VALID_RANGES = {
    "temperature_celsius": (-90.0, 60.0),
    "relative_humidity_pct": (0.0, 100.0),
    "precipitation": (0.0, 500.0),
    "wind_speed_kmh": (0.0, 400.0),
    "pressure_hpa": (870.0, 1084.0),
    "uv_index": (0.0, 20.0),
    "cloud_cover": (0.0, 100.0),
}

REQUIRED_COLUMNS = [
    "timestamp",
    "temperature_celsius",
    "relative_humidity_pct",
    "precipitation",
    "wind_speed_kmh",
    "location_name",
    "location_country",
]

WEATHER_CODE_MAP = {
    0: ("Clear sky", "Clear"),
    1: ("Mainly clear", "Clear"),
    2: ("Partly cloudy", "Cloudy"),
    3: ("Overcast", "Cloudy"),
    45: ("Foggy", "Fog"),
    48: ("Rime fog", "Fog"),
    51: ("Light drizzle", "Rain"),
    53: ("Moderate drizzle", "Rain"),
    55: ("Dense drizzle", "Rain"),
    61: ("Slight rain", "Rain"),
    63: ("Moderate rain", "Rain"),
    65: ("Heavy rain", "Rain"),
    71: ("Slight snowfall", "Snow"),
    73: ("Moderate snowfall", "Snow"),
    75: ("Heavy snowfall", "Snow"),
    77: ("Snow grains", "Snow"),
    80: ("Slight rain showers", "Rain"),
    81: ("Moderate rain showers", "Rain"),
    82: ("Violent rain showers", "Rain"),
    85: ("Slight snow showers", "Snow"),
    86: ("Heavy snow showers", "Snow"),
    95: ("Thunderstorm", "Storm"),
    96: ("Thunderstorm with hail", "Storm"),
    99: ("Thunderstorm with heavy hail", "Storm"),
}


class WeatherDataTransformer:
    """Cleans, validates, and enriches a raw weather DataFrame for analytical use."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Transformation started | rows=%d", len(df))
        df = self._rename_columns(df)
        df = self._convert_timestamps(df)
        df = self._cast_numeric_types(df)
        df = self._handle_missing_values(df)
        df = self._remove_duplicates(df)
        df = self._validate_measurements(df)
        df = self._add_derived_fields(df)
        self._validate_required_columns(df)
        logger.info("Transformation complete | rows=%d", len(df))
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=COLUMN_RENAME_MAP)

    def _convert_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    def _cast_numeric_types(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = [
            "temperature_celsius", "relative_humidity_pct", "precipitation",
            "rain", "snowfall", "cloud_cover", "wind_speed_kmh",
            "wind_direction_deg", "wind_gusts_kmh", "pressure_hpa",
            "visibility", "uv_index", "weather_code", "latitude", "longitude",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        critical_columns = ["timestamp", "temperature_celsius", "location_name"]
        before = len(df)
        df = df.dropna(subset=critical_columns)
        dropped = before - len(df)
        if dropped:
            logger.warning("Dropped %d rows with nulls in critical columns", dropped)
        df["precipitation"] = df["precipitation"].fillna(0.0)
        df["rain"] = df["rain"].fillna(0.0)
        df["snowfall"] = df["snowfall"].fillna(0.0)
        return df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["timestamp", "location_name", "location_country"])
        removed = before - len(df)
        if removed:
            logger.warning("Removed %d duplicate rows", removed)
        return df

    def _validate_measurements(self, df: pd.DataFrame) -> pd.DataFrame:
        for column, (min_val, max_val) in VALID_RANGES.items():
            if column not in df.columns:
                continue
            out_of_range = df[column].notna() & ~df[column].between(min_val, max_val)
            count = int(out_of_range.sum())
            if count:
                logger.warning(
                    "Column '%s': %d out-of-range value(s) set to NaN", column, count
                )
                df.loc[out_of_range, column] = pd.NA
        return df

    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour
        df["year"] = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month
        df["day"] = df["timestamp"].dt.day
        df["day_of_week"] = df["timestamp"].dt.dayofweek
        df["week_of_year"] = df["timestamp"].dt.isocalendar().week.astype(int)
        df["quarter"] = df["timestamp"].dt.quarter
        df["is_weekend"] = df["day_of_week"].isin([5, 6])
        df["temperature_fahrenheit"] = (df["temperature_celsius"] * 9 / 5) + 32
        df["period_of_day"] = df["hour"].map(_classify_period_of_day)
        df["weather_description"] = df["weather_code"].map(_resolve_weather_description)
        df["weather_category"] = df["weather_code"].map(_resolve_weather_category)
        if "is_day" in df.columns:
            df["is_day"] = df["is_day"].astype(bool)
        return df

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Transformed DataFrame missing required columns: {missing}")


def _classify_period_of_day(hour: int) -> str:
    if hour < 6:
        return "Night"
    if hour < 12:
        return "Morning"
    if hour < 18:
        return "Afternoon"
    return "Evening"


def _resolve_weather_description(code) -> str:
    if pd.isna(code):
        return "Unknown"
    return WEATHER_CODE_MAP.get(int(code), ("Unknown", "Unknown"))[0]


def _resolve_weather_category(code) -> str:
    if pd.isna(code):
        return "Unknown"
    return WEATHER_CODE_MAP.get(int(code), ("Unknown", "Unknown"))[1]
