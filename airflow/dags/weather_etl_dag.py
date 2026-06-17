"""
Weather ETL Pipeline DAG

Runs daily to extract weather data from the Open-Meteo API,
transform and validate it, then load it into the PostgreSQL star schema.

Task dependency graph:
    extract_weather_data
        >> transform_weather_data
        >> validate_weather_data
        >> load_weather_data
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

RAW_DATA_DIR = "/opt/airflow/data/raw"
PROCESSED_DATA_DIR = "/opt/airflow/data/processed"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**context) -> str:
    """Pulls weather data from the Open-Meteo API and saves it as a raw CSV.

    Returns the file path so the transform task can read it via XCom.
    """
    import pandas as pd

    from jobs.etl.extraction import (
        WeatherExtractor,
        get_extraction_window,
        load_default_locations,
        parse_to_dataframe,
    )

    locations = load_default_locations()
    start_date, end_date = get_extraction_window()

    with WeatherExtractor() as extractor:
        results = extractor.extract_all(locations, start_date, end_date)

    raw_df = pd.concat([parse_to_dataframe(r) for r in results], ignore_index=True)

    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    raw_path = f"{RAW_DATA_DIR}/weather_raw_{context['ds']}.csv"
    raw_df.to_csv(raw_path, index=False)

    context["ti"].log.info("Extracted %d rows → %s", len(raw_df), raw_path)
    return raw_path


def _transform(**context) -> str:
    """Reads the raw CSV, applies all transformations, and saves the cleaned data.

    Returns the processed file path so downstream tasks can read it via XCom.
    """
    import pandas as pd

    from jobs.etl.transform import WeatherDataTransformer

    raw_path = context["ti"].xcom_pull(task_ids="extract_weather_data")
    raw_df = pd.read_csv(raw_path)

    transformed_df = WeatherDataTransformer().transform(raw_df)

    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    processed_path = f"{PROCESSED_DATA_DIR}/weather_transformed_{context['ds']}.csv"
    transformed_df.to_csv(processed_path, index=False)

    context["ti"].log.info("Transformed %d rows → %s", len(transformed_df), processed_path)
    return processed_path


def _validate(**context) -> int:
    """Checks that all required columns are present and the dataset is non-empty.

    Raises ValueError if any check fails, which marks the task as failed
    and prevents the load task from running.
    """
    import pandas as pd

    from jobs.etl.transform import REQUIRED_COLUMNS

    processed_path = context["ti"].xcom_pull(task_ids="transform_weather_data")
    df = pd.read_csv(processed_path)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Validation failed — missing required columns: {missing}")

    if df.empty:
        raise ValueError("Validation failed — transformed DataFrame contains no rows")

    null_count = int(df[REQUIRED_COLUMNS].isna().sum().sum())
    context["ti"].log.info(
        "Validation passed | rows=%d | nulls_in_required_cols=%d",
        len(df),
        null_count,
    )
    return len(df)


def _load(**context) -> None:
    """Restores column types lost during CSV serialisation and loads the
    validated data into the PostgreSQL star schema.
    """
    import pandas as pd

    from jobs.etl.load import WeatherDataLoader

    processed_path = context["ti"].xcom_pull(task_ids="transform_weather_data")
    df = pd.read_csv(processed_path)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df["is_day"] = df["is_day"].astype(bool)
    df["is_weekend"] = df["is_weekend"].astype(bool)

    loader = WeatherDataLoader()
    loader.initialize_schema()
    loader.load(df)

    context["ti"].log.info("Load complete | rows=%d", len(df))


with DAG(
    dag_id="weather_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily ETL — Open-Meteo API to PostgreSQL star schema",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=_transform,
    )

    validate_task = PythonOperator(
        task_id="validate_weather_data",
        python_callable=_validate,
    )

    load_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=_load,
    )

    extract_task >> transform_task >> validate_task >> load_task
