from datetime import date

import pandas as pd
import pytest

from jobs.etl.extraction import ExtractionResult, Location


@pytest.fixture
def sample_location():
    return Location(
        name="Accra",
        country="Ghana",
        latitude=5.5560,
        longitude=-0.1969,
        timezone="Africa/Accra",
    )


@pytest.fixture
def valid_api_response():
    return {
        "latitude": 5.5560,
        "longitude": -0.1969,
        "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
        "hourly": {
            "time": ["2026-06-10T00:00", "2026-06-10T08:00"],
            "temperature_2m": [28.5, 30.1],
            "relative_humidity_2m": [75.0, 68.0],
            "precipitation": [0.0, 0.0],
            "rain": [0.0, 0.0],
            "snowfall": [0.0, 0.0],
            "weather_code": [0, 3],
            "cloud_cover": [10.0, 25.0],
            "wind_speed_10m": [12.0, 15.0],
            "wind_direction_10m": [180.0, 195.0],
            "wind_gusts_10m": [18.0, 22.0],
            "surface_pressure": [1013.0, 1012.0],
            "visibility": [10000.0, 9000.0],
            "uv_index": [0.0, 5.0],
            "is_day": [0, 1],
        },
    }


@pytest.fixture
def sample_extraction_result(sample_location, valid_api_response):
    return ExtractionResult(
        location=sample_location,
        raw_data=valid_api_response,
        extracted_at="2026-06-10T00:00:00+00:00",
        records_count=2,
    )


@pytest.fixture
def raw_df():
    return pd.DataFrame({
        "time": ["2026-06-10T00:00", "2026-06-10T08:00"],
        "temperature_2m": [28.5, 30.1],
        "relative_humidity_2m": [75.0, 68.0],
        "precipitation": [0.0, 0.0],
        "rain": [0.0, 0.0],
        "snowfall": [0.0, 0.0],
        "weather_code": [0, 3],
        "cloud_cover": [10.0, 25.0],
        "wind_speed_10m": [12.0, 15.0],
        "wind_direction_10m": [180.0, 195.0],
        "wind_gusts_10m": [18.0, 22.0],
        "surface_pressure": [1013.0, 1012.0],
        "visibility": [10000.0, 9000.0],
        "uv_index": [0.0, 5.0],
        "is_day": [0, 1],
        "location_name": ["Accra", "Accra"],
        "location_country": ["Ghana", "Ghana"],
        "latitude": [5.556, 5.556],
        "longitude": [-0.197, -0.197],
        "extracted_at": ["2026-06-10T00:00:00+00:00", "2026-06-10T00:00:00+00:00"],
    })


@pytest.fixture
def transformed_df(raw_df):
    from jobs.etl.transform import WeatherDataTransformer
    return WeatherDataTransformer().transform(raw_df.copy())
