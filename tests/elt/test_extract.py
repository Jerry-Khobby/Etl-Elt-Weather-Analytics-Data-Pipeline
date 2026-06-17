import pandas as pd
import pytest

from jobs.elt.extract import extract_raw


@pytest.fixture
def mock_extractor(mocker):
    mock_cls = mocker.patch("jobs.elt.extract.WeatherExtractor")
    return mock_cls.return_value.__enter__.return_value


@pytest.fixture
def single_row_frame():
    return pd.DataFrame({"col": [1]})


# --- extract_raw ---


def test_extract_raw_returns_dataframe(mock_extractor, single_row_frame, mocker):
    mocker.patch("jobs.elt.extract.load_default_locations", return_value=["loc"])
    mocker.patch("jobs.elt.extract.get_extraction_window", return_value=("2026-06-10", "2026-06-17"))
    mocker.patch("jobs.elt.extract.parse_to_dataframe", return_value=single_row_frame)
    mock_extractor.extract_all.return_value = [object()]

    result = extract_raw()

    assert isinstance(result, pd.DataFrame)


def test_extract_raw_concatenates_one_frame_per_location_result(mock_extractor, mocker):
    frame = pd.DataFrame({"col": [1]})
    mocker.patch("jobs.elt.extract.load_default_locations", return_value=["loc1", "loc2"])
    mocker.patch("jobs.elt.extract.get_extraction_window", return_value=("2026-06-10", "2026-06-17"))
    mocker.patch("jobs.elt.extract.parse_to_dataframe", return_value=frame)
    mock_extractor.extract_all.return_value = [object(), object()]

    result = extract_raw()

    assert len(result) == 2


def test_extract_raw_passes_all_locations_to_extract_all(mock_extractor, sample_location, mocker):
    locations = [sample_location, sample_location]
    mocker.patch("jobs.elt.extract.load_default_locations", return_value=locations)
    mocker.patch("jobs.elt.extract.get_extraction_window", return_value=("2026-06-10", "2026-06-17"))
    mocker.patch("jobs.elt.extract.parse_to_dataframe", return_value=pd.DataFrame({"col": [1]}))
    mock_extractor.extract_all.return_value = [object(), object()]

    extract_raw()

    mock_extractor.extract_all.assert_called_once_with(locations, "2026-06-10", "2026-06-17")


def test_extract_raw_reraises_exception_from_extractor(mock_extractor, mocker):
    mocker.patch("jobs.elt.extract.load_default_locations", return_value=["loc"])
    mocker.patch("jobs.elt.extract.get_extraction_window", return_value=("2026-06-10", "2026-06-17"))
    mock_extractor.extract_all.side_effect = ConnectionError("API down")

    with pytest.raises(ConnectionError, match="API down"):
        extract_raw()
