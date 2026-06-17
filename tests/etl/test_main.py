import pandas as pd
import pytest

from jobs.etl.main import WeatherEtlPipeline


@pytest.fixture
def pipeline(mocker):
    mocker.patch("jobs.etl.main.WeatherExtractor")
    mocker.patch("jobs.etl.main.WeatherDataTransformer")
    mocker.patch("jobs.etl.main.WeatherDataLoader")
    return WeatherEtlPipeline()


# --- run ---


def test_run_executes_all_pipeline_steps_in_order(pipeline, raw_df, transformed_df, mocker):
    extract_mock = mocker.patch.object(pipeline, "_extract", return_value=raw_df)
    save_mock = mocker.patch.object(pipeline, "_save_raw")
    pipeline._transformer.transform.return_value = transformed_df

    pipeline.run()

    extract_mock.assert_called_once()
    save_mock.assert_called_once_with(raw_df)
    pipeline._transformer.transform.assert_called_once_with(raw_df)
    pipeline._loader.initialize_schema.assert_called_once()
    pipeline._loader.load.assert_called_once_with(transformed_df)


def test_run_reraises_exception_when_extract_fails(pipeline, mocker):
    mocker.patch.object(pipeline, "_extract", side_effect=RuntimeError("API down"))

    with pytest.raises(RuntimeError, match="API down"):
        pipeline.run()


def test_run_reraises_exception_when_load_fails(pipeline, raw_df, transformed_df, mocker):
    mocker.patch.object(pipeline, "_extract", return_value=raw_df)
    mocker.patch.object(pipeline, "_save_raw")
    pipeline._transformer.transform.return_value = transformed_df
    pipeline._loader.initialize_schema.side_effect = Exception("DB unreachable")

    with pytest.raises(Exception, match="DB unreachable"):
        pipeline.run()


# --- _extract ---


def test_extract_concatenates_dataframes_from_all_locations(pipeline, sample_extraction_result, mocker):
    from jobs.etl.extraction import Location
    location = Location("Accra", "Ghana", 5.556, -0.197)
    frame = pd.DataFrame({"col": [1, 2]})

    mocker.patch("jobs.etl.main.load_default_locations", return_value=[location, location])
    mocker.patch("jobs.etl.main.get_extraction_window", return_value=("2026-06-10", "2026-06-17"))
    mocker.patch("jobs.etl.main.parse_to_dataframe", return_value=frame)

    ctx = mocker.MagicMock()
    ctx.extract_all.return_value = [sample_extraction_result, sample_extraction_result]
    pipeline._extractor.__enter__ = mocker.Mock(return_value=ctx)
    pipeline._extractor.__exit__ = mocker.Mock(return_value=False)

    result = pipeline._extract()

    assert len(result) == 4


# --- _save_raw ---


def test_save_raw_creates_csv_file_in_configured_directory(pipeline, raw_df, tmp_path, mocker):
    mocker.patch("jobs.etl.main.RAW_DATA_DIR", str(tmp_path))

    pipeline._save_raw(raw_df)

    csv_files = list(tmp_path.glob("weather_raw_*.csv"))
    assert len(csv_files) == 1


def test_save_raw_csv_content_matches_dataframe(pipeline, raw_df, tmp_path, mocker):
    mocker.patch("jobs.etl.main.RAW_DATA_DIR", str(tmp_path))

    pipeline._save_raw(raw_df)

    import pandas as pd
    csv_file = list(tmp_path.glob("weather_raw_*.csv"))[0]
    saved_df = pd.read_csv(csv_file)

    assert list(saved_df.columns) == list(raw_df.columns)
    assert len(saved_df) == len(raw_df)
