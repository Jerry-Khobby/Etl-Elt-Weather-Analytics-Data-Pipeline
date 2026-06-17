from datetime import date, timedelta

import pytest
import requests

from jobs.etl.extraction import (
    ExtractionResult,
    Location,
    WeatherExtractor,
    build_location,
    get_extraction_window,
    load_default_locations,
    parse_to_dataframe,
)
from jobs.config import EXTRACTION_LOOKBACK_DAYS


# --- build_location ---


def test_build_location_creates_location_from_config():
    config = {
        "name": "Accra",
        "country": "Ghana",
        "latitude": 5.556,
        "longitude": -0.197,
        "timezone": "Africa/Accra",
    }
    location = build_location(config)

    assert location.name == "Accra"
    assert location.country == "Ghana"
    assert location.latitude == 5.556
    assert location.longitude == -0.197
    assert location.timezone == "Africa/Accra"


def test_build_location_defaults_timezone_to_auto_when_absent():
    config = {"name": "X", "country": "Y", "latitude": 0.0, "longitude": 0.0}
    location = build_location(config)

    assert location.timezone == "auto"


# --- load_default_locations ---


def test_load_default_locations_returns_five_locations():
    locations = load_default_locations()

    assert len(locations) == 5


def test_load_default_locations_returns_location_instances():
    locations = load_default_locations()

    assert all(isinstance(loc, Location) for loc in locations)


# --- get_extraction_window ---


def test_get_extraction_window_end_date_is_today():
    _, end_date = get_extraction_window()

    assert end_date == date.today()


def test_get_extraction_window_spans_correct_number_of_days():
    start_date, end_date = get_extraction_window()

    assert (end_date - start_date).days == EXTRACTION_LOOKBACK_DAYS


# --- parse_to_dataframe ---


def test_parse_to_dataframe_row_count_matches_hourly_entries(sample_extraction_result):
    df = parse_to_dataframe(sample_extraction_result)

    assert len(df) == sample_extraction_result.records_count


def test_parse_to_dataframe_adds_location_metadata_columns(sample_extraction_result):
    df = parse_to_dataframe(sample_extraction_result)

    assert "location_name" in df.columns
    assert "location_country" in df.columns
    assert "latitude" in df.columns
    assert "longitude" in df.columns
    assert "extracted_at" in df.columns


def test_parse_to_dataframe_location_values_match_result(sample_extraction_result):
    df = parse_to_dataframe(sample_extraction_result)

    assert df["location_name"].iloc[0] == sample_extraction_result.location.name
    assert df["location_country"].iloc[0] == sample_extraction_result.location.country


# --- WeatherExtractor.extract ---


@pytest.fixture
def extractor():
    return WeatherExtractor()


def test_extract_returns_extraction_result_on_success(
    extractor, sample_location, valid_api_response, mocker
):
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = valid_api_response
    mocker.patch.object(extractor._session, "get", return_value=mock_response)

    result = extractor.extract(sample_location, date(2026, 6, 10), date(2026, 6, 17))

    assert isinstance(result, ExtractionResult)
    assert result.location == sample_location


def test_extract_records_count_matches_hourly_time_entries(
    extractor, sample_location, valid_api_response, mocker
):
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = valid_api_response
    mocker.patch.object(extractor._session, "get", return_value=mock_response)

    result = extractor.extract(sample_location, date(2026, 6, 10), date(2026, 6, 17))

    assert result.records_count == len(valid_api_response["hourly"]["time"])


def test_extract_raises_on_connection_error(extractor, sample_location, mocker):
    mocker.patch.object(
        extractor._session, "get", side_effect=requests.exceptions.ConnectionError
    )

    with pytest.raises(requests.exceptions.ConnectionError):
        extractor.extract(sample_location, date(2026, 6, 10), date(2026, 6, 17))


def test_extract_raises_on_timeout(extractor, sample_location, mocker):
    mocker.patch.object(
        extractor._session, "get", side_effect=requests.exceptions.Timeout
    )

    with pytest.raises(requests.exceptions.Timeout):
        extractor.extract(sample_location, date(2026, 6, 10), date(2026, 6, 17))


def test_extract_raises_on_http_error(extractor, sample_location, mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=mock_response
    )
    mocker.patch.object(extractor._session, "get", return_value=mock_response)

    with pytest.raises(requests.exceptions.HTTPError):
        extractor.extract(sample_location, date(2026, 6, 10), date(2026, 6, 17))


def test_extract_raises_on_invalid_json_body(extractor, sample_location, mocker):
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = requests.exceptions.JSONDecodeError("", "", 0)
    mocker.patch.object(extractor._session, "get", return_value=mock_response)

    with pytest.raises(requests.exceptions.JSONDecodeError):
        extractor.extract(sample_location, date(2026, 6, 10), date(2026, 6, 17))


# --- WeatherExtractor._validate_response_schema ---


def test_validate_response_schema_raises_on_missing_top_level_keys(extractor):
    bad_response = {"latitude": 5.5, "longitude": -0.2}

    with pytest.raises(ValueError, match="missing required top-level fields"):
        extractor._validate_response_schema(bad_response)


def test_validate_response_schema_raises_when_hourly_time_field_missing(extractor):
    bad_response = {
        "latitude": 5.5,
        "longitude": -0.2,
        "hourly_units": {},
        "hourly": {"temperature_2m": [28.0]},
    }

    with pytest.raises(ValueError, match="missing the 'time' field"):
        extractor._validate_response_schema(bad_response)


# --- WeatherExtractor.extract_all ---


def test_extract_all_returns_one_result_per_location(
    extractor, valid_api_response, mocker
):
    locations = load_default_locations()
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = valid_api_response
    mocker.patch.object(extractor._session, "get", return_value=mock_response)

    results = extractor.extract_all(locations, date(2026, 6, 10), date(2026, 6, 17))

    assert len(results) == len(locations)


# --- Context manager ---


def test_extractor_context_manager_closes_session(mocker):
    extractor = WeatherExtractor()
    close_spy = mocker.spy(extractor._session, "close")

    with extractor:
        pass

    close_spy.assert_called_once()
