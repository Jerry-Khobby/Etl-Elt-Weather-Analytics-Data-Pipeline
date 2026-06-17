import pytest

from jobs.etl.load import ALL_DDL, WeatherDataLoader, _to_fact_row


@pytest.fixture
def loader(mocker):
    mocker.patch("jobs.etl.load.create_engine")
    return WeatherDataLoader()


@pytest.fixture
def mock_conn(loader, mocker):
    conn = mocker.MagicMock()
    loader._engine.begin.return_value.__enter__.return_value = conn
    return conn


# --- _to_fact_row ---


def test_to_fact_row_maps_all_expected_output_keys():
    record = {
        "location_id": 1, "date_id": 2, "time_id": 3, "condition_id": 4,
        "temperature_celsius": 28.5, "temperature_fahrenheit": 83.3,
        "relative_humidity_pct": 75.0, "precipitation": 0.0, "rain": 0.0,
        "snowfall": 0.0, "wind_speed_kmh": 12.0, "wind_direction_deg": 180.0,
        "wind_gusts_kmh": 18.0, "pressure_hpa": 1013.0, "visibility": 10000.0,
        "uv_index": 0.0, "cloud_cover": 10.0, "is_day": True,
        "extracted_at": "2026-06-10T00:00:00+00:00",
    }

    row = _to_fact_row(record)

    assert row["temperature_celsius"] == 28.5
    assert row["precipitation_mm"] == 0.0
    assert row["rain_mm"] == 0.0
    assert row["snowfall_cm"] == 0.0
    assert row["visibility_m"] == 10000.0
    assert row["cloud_cover_pct"] == 10.0


def test_to_fact_row_returns_none_for_absent_keys():
    row = _to_fact_row({})

    assert row["location_id"] is None
    assert row["temperature_celsius"] is None
    assert row["extracted_at"] is None


# --- initialize_schema ---


def test_initialize_schema_executes_one_statement_per_ddl_table(loader, mock_conn):
    loader.initialize_schema()

    assert mock_conn.execute.call_count == len(ALL_DDL)


# --- _load_dim_location ---


def test_load_dim_location_executes_one_insert_per_unique_location(
    loader, mock_conn, transformed_df
):
    loader._load_dim_location(transformed_df)

    unique_locations = transformed_df[["location_name", "location_country"]].drop_duplicates()
    assert mock_conn.execute.call_count == len(unique_locations)


# --- _load_dim_date ---


def test_load_dim_date_executes_one_insert_per_unique_date(
    loader, mock_conn, transformed_df
):
    loader._load_dim_date(transformed_df)

    unique_dates = transformed_df["date"].nunique()
    assert mock_conn.execute.call_count == unique_dates


# --- _load_dim_time ---


def test_load_dim_time_executes_one_insert_per_unique_hour(
    loader, mock_conn, transformed_df
):
    loader._load_dim_time(transformed_df)

    unique_hours = transformed_df["hour"].nunique()
    assert mock_conn.execute.call_count == unique_hours


# --- _load_dim_weather_condition ---


def test_load_dim_weather_condition_skips_when_column_is_absent(loader, mock_conn):
    import pandas as pd
    df_no_code = pd.DataFrame({"temperature_celsius": [28.5]})

    loader._load_dim_weather_condition(df_no_code)

    mock_conn.execute.assert_not_called()


def test_load_dim_weather_condition_inserts_one_row_per_unique_code(
    loader, mock_conn, transformed_df
):
    loader._load_dim_weather_condition(transformed_df)

    unique_codes = transformed_df["weather_code"].dropna().nunique()
    assert mock_conn.execute.call_count == unique_codes


# --- _build_engine ---


def test_build_engine_raises_when_create_engine_fails(mocker):
    mocker.patch("jobs.etl.load.create_engine", side_effect=Exception("connection refused"))

    with pytest.raises(Exception, match="connection refused"):
        WeatherDataLoader()
