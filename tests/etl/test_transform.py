import pandas as pd
import pytest
from pytest import approx

from jobs.etl.transform import (
    REQUIRED_COLUMNS,
    WeatherDataTransformer,
    _classify_period_of_day,
    _resolve_weather_category,
    _resolve_weather_description,
)


@pytest.fixture
def transformer():
    return WeatherDataTransformer()


# --- _classify_period_of_day ---


def test_classify_period_of_day_returns_night_before_6am():
    assert _classify_period_of_day(0) == "Night"
    assert _classify_period_of_day(5) == "Night"


def test_classify_period_of_day_returns_morning_from_6am():
    assert _classify_period_of_day(6) == "Morning"
    assert _classify_period_of_day(11) == "Morning"


def test_classify_period_of_day_returns_afternoon_from_noon():
    assert _classify_period_of_day(12) == "Afternoon"
    assert _classify_period_of_day(17) == "Afternoon"


def test_classify_period_of_day_returns_evening_from_6pm():
    assert _classify_period_of_day(18) == "Evening"
    assert _classify_period_of_day(23) == "Evening"


# --- _resolve_weather_description ---


def test_resolve_weather_description_returns_description_for_known_code():
    assert _resolve_weather_description(0) == "Clear sky"
    assert _resolve_weather_description(63) == "Moderate rain"
    assert _resolve_weather_description(95) == "Thunderstorm"


def test_resolve_weather_description_returns_unknown_for_unmapped_code():
    assert _resolve_weather_description(999) == "Unknown"


def test_resolve_weather_description_returns_unknown_for_nan():
    assert _resolve_weather_description(float("nan")) == "Unknown"


# --- _resolve_weather_category ---


def test_resolve_weather_category_returns_category_for_known_code():
    assert _resolve_weather_category(0) == "Clear"
    assert _resolve_weather_category(63) == "Rain"
    assert _resolve_weather_category(71) == "Snow"
    assert _resolve_weather_category(95) == "Storm"


def test_resolve_weather_category_returns_unknown_for_nan():
    assert _resolve_weather_category(float("nan")) == "Unknown"


# --- _rename_columns ---


def test_rename_columns_applies_column_rename_map(transformer):
    df = pd.DataFrame({
        "time": ["2026-06-10T00:00"],
        "temperature_2m": [28.5],
        "relative_humidity_2m": [75.0],
        "wind_speed_10m": [12.0],
        "surface_pressure": [1013.0],
    })

    result = transformer._rename_columns(df)

    assert "timestamp" in result.columns
    assert "temperature_celsius" in result.columns
    assert "relative_humidity_pct" in result.columns
    assert "wind_speed_kmh" in result.columns
    assert "pressure_hpa" in result.columns


def test_rename_columns_does_not_drop_unmapped_columns(transformer):
    df = pd.DataFrame({"time": ["2026-06-10T00:00"], "location_name": ["Accra"]})

    result = transformer._rename_columns(df)

    assert "location_name" in result.columns


# --- _convert_timestamps ---


def test_convert_timestamps_produces_datetime_column(transformer):
    df = pd.DataFrame({"timestamp": ["2026-06-10T00:00", "2026-06-10T08:00"]})

    result = transformer._convert_timestamps(df)

    assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])


# --- _cast_numeric_types ---


def test_cast_numeric_types_converts_string_to_float(transformer):
    df = pd.DataFrame({"temperature_celsius": ["28.5", "30.1"]})

    result = transformer._cast_numeric_types(df)

    assert pd.api.types.is_float_dtype(result["temperature_celsius"])


def test_cast_numeric_types_coerces_invalid_strings_to_nan(transformer):
    df = pd.DataFrame({"temperature_celsius": ["28.5", "not_a_number"]})

    result = transformer._cast_numeric_types(df)

    assert result["temperature_celsius"].isna().sum() == 1


# --- _handle_missing_values ---


def test_handle_missing_values_drops_rows_with_null_in_critical_columns(transformer):
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-10"), None],
        "temperature_celsius": [28.5, 30.0],
        "location_name": ["Accra", "Accra"],
        "precipitation": [0.0, 0.0],
        "rain": [0.0, 0.0],
        "snowfall": [0.0, 0.0],
    })

    result = transformer._handle_missing_values(df)

    assert len(result) == 1


def test_handle_missing_values_fills_precipitation_nulls_with_zero(transformer):
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-10")],
        "temperature_celsius": [28.5],
        "location_name": ["Accra"],
        "precipitation": [None],
        "rain": [None],
        "snowfall": [None],
    })

    result = transformer._handle_missing_values(df)

    assert result["precipitation"].iloc[0] == approx(0.0)
    assert result["rain"].iloc[0] == approx(0.0)
    assert result["snowfall"].iloc[0] == approx(0.0)


# --- _remove_duplicates ---


def test_remove_duplicates_drops_duplicate_timestamp_location_rows(transformer):
    df = pd.DataFrame({
        "timestamp": [
            pd.Timestamp("2026-06-10T00:00"),
            pd.Timestamp("2026-06-10T00:00"),
            pd.Timestamp("2026-06-10T01:00"),
        ],
        "location_name": ["Accra", "Accra", "Accra"],
        "location_country": ["Ghana", "Ghana", "Ghana"],
        "temperature_celsius": [28.5, 28.5, 27.0],
    })

    result = transformer._remove_duplicates(df)

    assert len(result) == 2


def test_remove_duplicates_keeps_unique_rows_unchanged(transformer):
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-10T00:00"), pd.Timestamp("2026-06-10T01:00")],
        "location_name": ["Accra", "Accra"],
        "location_country": ["Ghana", "Ghana"],
        "temperature_celsius": [28.5, 27.0],
    })

    result = transformer._remove_duplicates(df)

    assert len(result) == 2


# --- _validate_measurements ---


def test_validate_measurements_sets_out_of_range_temperature_to_nan(transformer):
    df = pd.DataFrame({"temperature_celsius": [28.5, 999.0]})

    result = transformer._validate_measurements(df)

    assert result["temperature_celsius"].iloc[0] == approx(28.5)
    assert pd.isna(result["temperature_celsius"].iloc[1])


def test_validate_measurements_keeps_valid_values_unchanged(transformer):
    df = pd.DataFrame({"temperature_celsius": [28.5, -5.0, 55.0]})

    result = transformer._validate_measurements(df)

    assert result["temperature_celsius"].notna().all()


# --- _add_derived_fields ---


def test_add_derived_fields_adds_date_and_time_components(transformer):
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-10T08:00")],
        "temperature_celsius": [28.5],
        "weather_code": [0],
        "is_day": [1],
    })

    result = transformer._add_derived_fields(df)

    assert result["year"].iloc[0] == 2026
    assert result["month"].iloc[0] == 6
    assert result["day"].iloc[0] == 10
    assert result["hour"].iloc[0] == 8
    assert result["quarter"].iloc[0] == 2


def test_add_derived_fields_computes_fahrenheit_correctly(transformer):
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-10T00:00")],
        "temperature_celsius": [0.0],
        "weather_code": [0],
        "is_day": [0],
    })

    result = transformer._add_derived_fields(df)

    assert result["temperature_fahrenheit"].iloc[0] == approx(32.0)


def test_add_derived_fields_marks_weekend_correctly(transformer):
    # June 6, 2026 is a Saturday; June 9, 2026 is a Tuesday
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-06T10:00"), pd.Timestamp("2026-06-09T10:00")],
        "temperature_celsius": [28.5, 29.0],
        "weather_code": [0, 0],
        "is_day": [1, 1],
    })

    result = transformer._add_derived_fields(df)

    assert result["is_weekend"].iloc[0] == True
    assert result["is_weekend"].iloc[1] == False


def test_add_derived_fields_assigns_correct_period_of_day(transformer):
    df = pd.DataFrame({
        "timestamp": [
            pd.Timestamp("2026-06-10T03:00"),
            pd.Timestamp("2026-06-10T09:00"),
            pd.Timestamp("2026-06-10T14:00"),
            pd.Timestamp("2026-06-10T20:00"),
        ],
        "temperature_celsius": [25.0, 28.0, 32.0, 29.0],
        "weather_code": [0, 0, 0, 0],
        "is_day": [0, 1, 1, 0],
    })

    result = transformer._add_derived_fields(df)

    assert result["period_of_day"].tolist() == ["Night", "Morning", "Afternoon", "Evening"]


def test_add_derived_fields_maps_weather_description_and_category(transformer):
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-06-10T00:00")],
        "temperature_celsius": [28.5],
        "weather_code": [63],
        "is_day": [1],
    })

    result = transformer._add_derived_fields(df)

    assert result["weather_description"].iloc[0] == "Moderate rain"
    assert result["weather_category"].iloc[0] == "Rain"


# --- _validate_required_columns ---


def test_validate_required_columns_raises_when_column_is_missing(transformer):
    df = pd.DataFrame({"timestamp": [pd.Timestamp("2026-06-10")]})

    with pytest.raises(ValueError, match="missing required columns"):
        transformer._validate_required_columns(df)


def test_validate_required_columns_passes_when_all_present(transformer, transformed_df):
    transformer._validate_required_columns(transformed_df)


# --- transform (full pipeline) ---


def test_transform_returns_all_required_columns(transformer, raw_df):
    result = transformer.transform(raw_df)

    for col in REQUIRED_COLUMNS:
        assert col in result.columns


def test_transform_does_not_increase_row_count_on_clean_data(transformer, raw_df):
    result = transformer.transform(raw_df)

    assert len(result) == len(raw_df)
