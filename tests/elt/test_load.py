import pytest

from jobs.elt.load import StagingLoader


@pytest.fixture
def loader(mocker):
    mocker.patch("jobs.elt.load.create_engine")
    return StagingLoader()


@pytest.fixture
def mock_conn(loader, mocker):
    conn = mocker.MagicMock()
    loader._engine.begin.return_value.__enter__.return_value = conn
    loader._engine.connect.return_value.__enter__.return_value = conn
    return conn


# --- initialize_schema ---


def test_initialize_schema_executes_create_table_statement(loader, mock_conn):
    loader.initialize_schema()

    mock_conn.execute.assert_called_once()


# --- load ---


def test_load_renames_time_column_to_timestamp_before_inserting(loader, mock_conn):
    raw_df = __make_raw_df({"time": ["2026-06-10T00:00"], "temperature_2m": [28.5]})

    loader.load(raw_df)

    sql_text = str(mock_conn.execute.call_args.args[0])
    assert '"timestamp"' in sql_text
    assert '"time"' not in sql_text


def test_load_inserts_into_the_staging_table(loader, mock_conn):
    raw_df = __make_raw_df({"time": ["2026-06-10T00:00"]})

    loader.load(raw_df)

    sql_text = str(mock_conn.execute.call_args.args[0])
    assert "weather_raw_staging" in sql_text


def test_load_returns_row_count(loader, mock_conn):
    raw_df = __make_raw_df({"time": ["a", "b", "c"]})

    count = loader.load(raw_df)

    assert count == 3


def test_load_passes_records_as_dicts_to_execute(loader, mock_conn):
    raw_df = __make_raw_df({"time": ["2026-06-10T00:00"]})

    loader.load(raw_df)

    records = mock_conn.execute.call_args.args[1]
    assert isinstance(records, list)
    assert "timestamp" in records[0]


def test_load_reraises_exception_on_database_error(loader, mock_conn):
    mock_conn.execute.side_effect = Exception("DB write failed")
    raw_df = __make_raw_df({"time": ["a"]})

    with pytest.raises(Exception, match="DB write failed"):
        loader.load(raw_df)


# --- fetch_unprocessed_ids ---


def test_fetch_unprocessed_ids_returns_list_of_integer_ids(loader, mock_conn, mocker):
    mock_conn.execute.return_value.fetchall.return_value = [
        mocker.MagicMock(id=10),
        mocker.MagicMock(id=20),
        mocker.MagicMock(id=30),
    ]

    ids = loader.fetch_unprocessed_ids()

    assert ids == [10, 20, 30]


def test_fetch_unprocessed_ids_returns_empty_list_when_no_rows(loader, mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = []

    ids = loader.fetch_unprocessed_ids()

    assert ids == []


# --- mark_processed ---


def test_mark_processed_executes_update_statement(loader, mock_conn):
    loader.mark_processed([1, 2, 3])

    mock_conn.execute.assert_called_once()


def test_mark_processed_skips_when_id_list_is_empty(loader, mock_conn):
    loader.mark_processed([])

    mock_conn.execute.assert_not_called()


# --- _build_engine ---


def test_build_engine_raises_when_create_engine_fails(mocker):
    mocker.patch("jobs.elt.load.create_engine", side_effect=Exception("connection refused"))

    with pytest.raises(Exception, match="connection refused"):
        StagingLoader()


def __make_raw_df(data: dict):
    import pandas as pd

    return pd.DataFrame(data)
