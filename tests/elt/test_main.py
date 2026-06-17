import pandas as pd
import pytest

from jobs.elt.main import WeatherEltPipeline


@pytest.fixture
def pipeline(mocker):
    mocker.patch("jobs.elt.main.StagingLoader")
    mocker.patch("jobs.elt.main.StagingTransformer")
    mocker.patch("jobs.elt.main.WeatherDataLoader")
    return WeatherEltPipeline()


# --- run ---


def test_run_executes_all_pipeline_steps_in_order(pipeline, transformed_df, mocker):
    mocker.patch("jobs.elt.main.extract_raw", return_value=pd.DataFrame({"col": [1]}))
    pipeline._transformer.transform.return_value = (transformed_df, [1, 2])

    pipeline.run()

    pipeline._staging_loader.initialize_schema.assert_called_once()
    pipeline._staging_loader.load.assert_called_once()
    pipeline._transformer.transform.assert_called_once()
    pipeline._analytics_loader.initialize_schema.assert_called_once()
    pipeline._analytics_loader.load.assert_called_once_with(transformed_df)
    pipeline._staging_loader.mark_processed.assert_called_once_with([1, 2])


def test_run_skips_analytics_load_when_staging_transform_returns_empty(pipeline, mocker):
    mocker.patch("jobs.elt.main.extract_raw", return_value=pd.DataFrame({"col": [1]}))
    pipeline._transformer.transform.return_value = (pd.DataFrame(), [])

    pipeline.run()

    pipeline._analytics_loader.load.assert_not_called()
    pipeline._staging_loader.mark_processed.assert_not_called()


def test_run_marks_processed_only_after_analytics_load_succeeds(pipeline, transformed_df, mocker):
    mocker.patch("jobs.elt.main.extract_raw", return_value=pd.DataFrame({"col": [1]}))
    pipeline._transformer.transform.return_value = (transformed_df, [10, 20, 30])

    pipeline.run()

    pipeline._staging_loader.mark_processed.assert_called_once_with([10, 20, 30])


def test_run_reraises_exception_when_extraction_fails(pipeline, mocker):
    mocker.patch("jobs.elt.main.extract_raw", side_effect=RuntimeError("API down"))

    with pytest.raises(RuntimeError, match="API down"):
        pipeline.run()


def test_run_reraises_exception_when_staging_load_fails(pipeline, mocker):
    mocker.patch("jobs.elt.main.extract_raw", return_value=pd.DataFrame({"col": [1]}))
    pipeline._staging_loader.load.side_effect = Exception("Staging DB unreachable")

    with pytest.raises(Exception, match="Staging DB unreachable"):
        pipeline.run()


def test_run_reraises_exception_when_analytics_load_fails(pipeline, transformed_df, mocker):
    mocker.patch("jobs.elt.main.extract_raw", return_value=pd.DataFrame({"col": [1]}))
    pipeline._transformer.transform.return_value = (transformed_df, [1])
    pipeline._analytics_loader.load.side_effect = Exception("Analytics DB unreachable")

    with pytest.raises(Exception, match="Analytics DB unreachable"):
        pipeline.run()
