from datetime import date

import pandas as pd
import pytest

from jobs.elt.transform import StagingTransformer


@pytest.fixture
def transformer(mocker):
    mocker.patch("jobs.elt.transform.create_engine")
    return StagingTransformer()


@pytest.fixture
def sql_result_df():
    """Simulates the raw DataFrame returned by the SQL transform query.

    Matches the SELECT column list in TRANSFORM_UNPROCESSED_SQL — it still
    contains the staging 'id' column which transform() must strip out.
    """
    return pd.DataFrame({
        "id": [1, 2],
        "timestamp": [pd.Timestamp("2026-06-10T00:00"), pd.Timestamp("2026-06-10T08:00")],
        "temperature_celsius": [28.5, 30.1],
        "relative_humidity_pct": [75.0, 68.0],
        "precipitation": [0.0, 0.0],
        "rain": [0.0, 0.0],
        "snowfall": [0.0, 0.0],
        "weather_code": [0, 3],
        "cloud_cover": [10.0, 25.0],
        "wind_speed_kmh": [12.0, 15.0],
        "wind_direction_deg": [180.0, 195.0],
        "wind_gusts_kmh": [18.0, 22.0],
        "pressure_hpa": [1013.0, 1012.0],
        "visibility": [10000.0, 9000.0],
        "uv_index": [0.0, 5.0],
        "is_day": [False, True],
        "location_name": ["Accra", "Accra"],
        "location_country": ["Ghana", "Ghana"],
        "latitude": [5.556, 5.556],
        "longitude": [-0.197, -0.197],
        "extracted_at": ["2026-06-10T00:00:00+00:00", "2026-06-10T00:00:00+00:00"],
        "date": [date(2026, 6, 10), date(2026, 6, 10)],
        "hour": [0, 8],
        "year": [2026, 2026],
        "month": [6, 6],
        "day": [10, 10],
        "day_of_week": [1, 1],
        "week_of_year": [24, 24],
        "quarter": [2, 2],
        "is_weekend": [False, False],
        "temperature_fahrenheit": [83.30, 86.18],
        "period_of_day": ["Night", "Morning"],
    })


# --- _add_weather_labels ---


def test_add_weather_labels_maps_clear_sky_code(transformer, sql_result_df):
    df = sql_result_df.drop(columns=["id"])

    result = transformer._add_weather_labels(df)

    assert result["weather_description"].iloc[0] == "Clear sky"
    assert result["weather_category"].iloc[0] == "Clear"


def test_add_weather_labels_maps_overcast_code(transformer, sql_result_df):
    df = sql_result_df.drop(columns=["id"])

    result = transformer._add_weather_labels(df)

    assert result["weather_description"].iloc[1] == "Overcast"
    assert result["weather_category"].iloc[1] == "Cloudy"


def test_add_weather_labels_returns_unknown_for_unmapped_code(transformer, sql_result_df):
    df = sql_result_df.drop(columns=["id"])
    df["weather_code"] = 999

    result = transformer._add_weather_labels(df)

    assert result["weather_description"].iloc[0] == "Unknown"
    assert result["weather_category"].iloc[0] == "Unknown"


# --- transform ---


def test_transform_returns_empty_df_and_empty_ids_when_staging_has_no_rows(transformer, mocker):
    mocker.patch.object(transformer, "_run_sql_transform", return_value=pd.DataFrame())

    result_df, result_ids = transformer.transform()

    assert result_df.empty
    assert result_ids == []


def test_transform_drops_staging_id_column_from_output(transformer, sql_result_df, mocker):
    mocker.patch.object(transformer, "_run_sql_transform", return_value=sql_result_df)

    result_df, _ = transformer.transform()

    assert "id" not in result_df.columns


def test_transform_returns_staging_row_ids_for_mark_processed(transformer, sql_result_df, mocker):
    mocker.patch.object(transformer, "_run_sql_transform", return_value=sql_result_df)

    _, result_ids = transformer.transform()

    assert result_ids == [1, 2]


def test_transform_adds_weather_description_column(transformer, sql_result_df, mocker):
    mocker.patch.object(transformer, "_run_sql_transform", return_value=sql_result_df)

    result_df, _ = transformer.transform()

    assert "weather_description" in result_df.columns


def test_transform_adds_weather_category_column(transformer, sql_result_df, mocker):
    mocker.patch.object(transformer, "_run_sql_transform", return_value=sql_result_df)

    result_df, _ = transformer.transform()

    assert "weather_category" in result_df.columns


def test_transform_preserves_all_derived_date_time_columns(transformer, sql_result_df, mocker):
    mocker.patch.object(transformer, "_run_sql_transform", return_value=sql_result_df)

    result_df, _ = transformer.transform()

    for col in ["date", "hour", "year", "month", "day", "day_of_week",
                "week_of_year", "quarter", "is_weekend", "period_of_day",
                "temperature_fahrenheit"]:
        assert col in result_df.columns, f"Expected column '{col}' in transform output"


# --- _build_engine ---


def test_build_engine_raises_when_create_engine_fails(mocker):
    mocker.patch("jobs.elt.transform.create_engine", side_effect=Exception("connection refused"))

    with pytest.raises(Exception, match="connection refused"):
        StagingTransformer()
