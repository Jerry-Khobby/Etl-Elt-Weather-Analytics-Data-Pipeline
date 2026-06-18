# Testing Strategy — Unit Tests, Data Validation Tests, Coverage, and Gaps

## Overview

The project uses **pytest** as the test runner with **pytest-mock** for mocking. All tests are pure unit tests — they run in isolation without a live database or live API. This makes the test suite fast, deterministic, and runnable in any environment (including the Docker `test` container in `docker-compose.yml`). The test suite covers both the ETL and ELT pipelines independently with separate test modules mirroring the production code structure.

---

## Test Configuration

**File**: [pytest.ini](../pytest.ini)

```ini
[pytest]
testpaths = tests
pythonpath = .
python_files = test_*.py
python_classes = Test*
python_functions = test_*
```

- `testpaths = tests`: pytest only looks in the `tests/` directory
- `pythonpath = .`: adds the project root to `sys.path`, allowing `from jobs.etl.extraction import WeatherExtractor` without path manipulation
- Discovery pattern: files matching `test_*.py`, classes starting with `Test`, functions starting with `test_`

**Test dependencies** ([requirements.txt](../requirements.txt)):
```
pytest>=8.2.0
pytest-mock>=3.14.0
```

No `pytest-cov` is listed — coverage reporting is not automated, though the tests are written to achieve close to full branch coverage.

---

## Test Structure

The test directory mirrors the production code structure exactly:

```
tests/
├── conftest.py              ← shared fixtures (used by both ETL and ELT tests)
├── etl/
│   ├── test_extraction.py   ← 26 tests
│   ├── test_transform.py    ← 30 tests
│   ├── test_load.py         ← 12 tests
│   └── test_main.py         ← 8 tests
└── elt/
    ├── test_extract.py      ← 6 tests
    ├── test_transform.py    ← 12 tests
    ├── test_load.py         ← 19 tests
    └── test_main.py         ← 7 tests
```

Total: approximately **120 test functions** across both pipelines.

---

## Shared Fixtures — conftest.py

**File**: [tests/conftest.py](../tests/conftest.py)

The shared fixtures define reusable test data used across all test modules:

```python
@pytest.fixture
def sample_location():
    return Location(
        name="Accra", country="Ghana",
        latitude=5.6037, longitude=-0.1870,
        timezone="Africa/Accra"
    )

@pytest.fixture
def valid_api_response():
    return {
        "latitude": 5.6037, "longitude": -0.1870,
        "hourly_units": {...},
        "hourly": {
            "time":                ["2026-06-01T00:00", "2026-06-01T01:00"],
            "temperature_2m":      [26.4, 25.8],
            "relative_humidity_2m":[82, 85],
            "precipitation":       [0.0, 0.2],
            "weather_code":        [2, 61],
            # ... all 14 hourly variables with 2 rows
        }
    }

@pytest.fixture
def raw_df(sample_location, valid_api_response):
    # The DataFrame that WeatherExtractor.parse_to_dataframe() produces
    # from valid_api_response + sample_location metadata
    ...

@pytest.fixture
def transformed_df(raw_df):
    # The DataFrame after WeatherDataTransformer.transform(raw_df)
    return WeatherDataTransformer().transform(raw_df)
```

**Why shared fixtures matter**: Both ETL and ELT tests need the same sample location, API response shape, and raw DataFrame. Defining these once in `conftest.py` prevents duplication and ensures both pipelines are tested against identical inputs — any divergence in output is a real functional difference, not a fixture discrepancy.

---

## ETL Tests in Detail

### test_extraction.py (26 tests)

**What is tested**:

| Test group | Tests | Approach |
|---|---|---|
| `build_location` | Location dataclass construction from dict | Direct assertion on dataclass fields |
| `load_default_locations` | Returns 5 locations | Assert `len(locations) == 5` |
| `get_extraction_window` | Returns 7-day span ending today | Assert `(end - start).days == 7` |
| `parse_to_dataframe` | Row count, metadata columns added | Assert shape and column presence |
| `WeatherExtractor.extract` — success | API called with correct params, returns ExtractionResult | `mocker.patch("requests.Session.get")` |
| `WeatherExtractor.extract` — ConnectionError | Exception caught and re-raised | Assert `pytest.raises(ConnectionError)` |
| `WeatherExtractor.extract` — Timeout | Exception caught and re-raised | Assert `pytest.raises(Timeout)` |
| `WeatherExtractor.extract` — HTTPError 500 | Exception caught and re-raised | Mock `response.raise_for_status()` to raise |
| `WeatherExtractor.extract` — JSONDecodeError | Exception caught and re-raised | Mock `response.json()` to raise |
| `_validate_response_schema` — missing top-level key | Returns False | Assert return value |
| `_validate_response_schema` — missing hourly.time | Returns False | Assert return value |
| `extract_all` | Returns one ExtractionResult per location | Assert `len(results) == 5` |
| Context manager | Session is closed on `__exit__` | Assert `session.close()` called |

**Mocking strategy**: All HTTP calls are mocked via `mocker.patch`. The tests never touch the network. The mock is configured to return `valid_api_response` for success cases and to raise specific exceptions for error cases.

### test_transform.py (30 tests)

**What is tested**:

| Test group | Tests | Key assertion |
|---|---|---|
| `_get_period_of_day` | Night (0–5), Morning (6–11), Afternoon (12–17), Evening (18–23) | All 24 hours classified correctly |
| `_get_weather_labels` | Code 63 → ("Moderate rain", "Rain") | Each mapped code returns expected label |
| `_rename_columns` | `temperature_2m` → `temperature_celsius` etc. | Assert renamed columns present, old names absent |
| `_convert_timestamps` | String `"2026-06-01T00:00"` → `datetime64` | Assert `dtype == datetime64[ns]` |
| `_cast_numeric_types` | String `"26.4"` → `float` | Assert `dtype == float64` |
| `_handle_missing_values` | Null temperature → row dropped; null precipitation → filled 0.0 | Assert row count and fill value |
| `_remove_duplicates` | Two rows with same timestamp+location → one retained | Assert row count |
| `_validate_measurements` | `temperature_celsius = 100.0` → set to NaN | Assert specific cell is NaN |
| `_validate_measurements` | `-90.0` → within range, retained | Assert cell unchanged |
| `_add_derived_fields` | Year, month, day, hour, quarter, is_weekend, period_of_day, fahrenheit | Assert each derived column value |
| `_validate_required_columns` | Missing `wind_speed_kmh` → raises ValueError | Assert `pytest.raises(ValueError)` |
| Full `transform()` | Row count does not increase on clean data | Assert `len(result) <= len(input)` |

**One assert per test** (AmaliTech standard): Each test function tests exactly one behaviour. `test_missing_temperature_drops_row` tests only the null-temperature drop. `test_null_precipitation_fills_zero` tests only the precipitation fill. This makes failures immediately locatable.

### test_load.py (12 tests)

**What is tested**:

| Test | Approach |
|---|---|
| `initialize_schema` runs DDL | Mock engine, assert `execute` called once per table |
| `_upsert_dim_location` inserts one row | Mock engine, assert INSERT called with correct values |
| `_upsert_dim_date` inserts one row | Same approach |
| `_upsert_dim_time` inserts one row | Same approach |
| `_upsert_dim_weather_condition` inserts one row | Same approach |
| `load` calls all dimension upserts then fact insert | Assert call order via `call_args_list` |
| `_to_fact_row` maps all measurement columns | Direct assertion on returned dict |
| Connection error propagation | Mock engine to raise `OperationalError`, assert re-raised |

**Mocking strategy**: The `Engine` and connection are mocked entirely. No database is needed. The tests verify that the correct SQL is constructed and the correct parameters are passed — the database itself is not exercised.

### test_main.py (8 tests)

| Test | What it verifies |
|---|---|
| `run` calls extract, transform, load in order | Mock all three, assert call sequence |
| `run` saves raw CSV before transforming | Assert file written to `data/raw/` |
| `run` re-raises extraction exceptions | Mock extractor to raise, assert propagation |
| `run` re-raises transform exceptions | Mock transformer to raise, assert propagation |
| `run` re-raises load exceptions | Mock loader to raise, assert propagation |
| Raw CSV filename contains today's date | Assert filename pattern matches `weather_raw_{date}` |

---

## ELT Tests in Detail

### test_extract.py (6 tests)

Tests the `extract_raw()` function, which is a thin wrapper over `WeatherExtractor.extract_all()`. Verifies that it returns a DataFrame, concatenates results from all 5 locations, and re-raises exceptions from the extractor.

### test_transform.py (12 tests)

| Test | What it verifies |
|---|---|
| `transform` returns `(DataFrame, list[int])` tuple | Assert return type |
| `transform` only reads unprocessed rows | Assert SQL contains `WHERE is_processed = FALSE` |
| `transform` drops the staging `id` column from output | Assert `id` not in result columns |
| `transform` adds `weather_description` and `weather_category` | Assert columns present |
| `transform` preserves all derived columns | Assert `period_of_day`, `temperature_fahrenheit`, `is_weekend` present |
| `transform` returns empty DataFrame when no unprocessed rows | Assert `df.empty == True` |
| Returns correct staging IDs | Assert IDs match the staging rows selected |

### test_load.py (19 tests)

The ELT loader tests cover more ground because the staging loader has distinct responsibilities not present in the ETL loader:

| Test group | Tests |
|---|---|
| `initialize_schema` | DDL executed, table created |
| `load` | `time` renamed to `timestamp`, rows inserted, row count returned |
| `load` | Records passed as list of dicts to execute |
| `load` — DB error | Exception logged and re-raised |
| `fetch_unprocessed_ids` | Returns list of integer IDs |
| `fetch_unprocessed_ids` — empty table | Returns empty list |
| `mark_processed` | UPDATE executed with correct IDs |
| `mark_processed` — empty list | No UPDATE executed (short-circuits) |
| Engine creation error | Propagated correctly |

The `mark_processed` empty-list test is particularly important: if `staging_ids` is empty, calling `mark_processed([])` should do nothing — not execute `WHERE id IN ()` which would be a syntax error in some SQL dialects.

### test_main.py (7 tests)

| Test | What it verifies |
|---|---|
| Full run: extract→stage→transform→analytics load→mark processed | Assert all 5 calls made in order |
| Empty transform result skips analytics load | Assert analytics loader NOT called |
| Empty transform result skips mark_processed | Assert mark_processed NOT called |
| mark_processed called AFTER analytics load | Assert call order via mock |
| Extraction exception re-raised | Assert propagation |
| Staging load exception re-raised | Assert propagation |
| Analytics load exception → mark_processed NOT called | Assert mark_processed never reached |

The last test is the most important: it verifies the transactional safety property — if analytics load fails, staging rows remain unprocessed and will be retried on the next run.

---

## What Is Covered

| Component | Coverage level | Notes |
|---|---|---|
| API extraction (success path) | Full | Happy path with mocked HTTP response |
| API extraction (error paths) | Full | ConnectionError, Timeout, HTTPError, JSONDecodeError |
| API response schema validation | Full | Missing top-level keys, missing hourly.time |
| Transform — column renames | Full | All 7 rename mappings tested |
| Transform — type casting | Full | String→float, string→datetime |
| Transform — null handling | Full | Critical column drops and fill-zero cases |
| Transform — range validation | Full | In-range and out-of-range values |
| Transform — deduplication | Full | Exact duplicate removal |
| Transform — derived fields | Full | All date/time components, Fahrenheit, period, weather labels |
| Transform — required column check | Full | Missing column raises ValueError |
| ETL load — dimension upserts | Full | All 4 dimensions |
| ETL load — fact table insert | Full | Mapping and insert call |
| ETL pipeline orchestration | Full | Call order, CSV save, exception propagation |
| ELT extraction | Full | Wrapper behaviour |
| ELT SQL transformation | Full | Column presence, tuple return, empty case |
| ELT staging load | Full | Insert, rename, error, empty cases |
| ELT mark_processed | Full | Update call, empty-list short-circuit |
| ELT pipeline orchestration | Full | 5-step order, empty-transform early exit, transactional safety |

---

## What Is Not Covered

| Gap | Reason | Risk |
|---|---|---|
| **Integration tests** (real DB + real API) | No test database in the test container | A SQL syntax error or schema mismatch would only surface at runtime |
| **Airflow DAG tests** | No `pytest-airflow` dependency | Task dependencies, XCom passing, and retry behaviour are untested |
| **End-to-end pipeline test** | No docker-compose test profile with a real DB | Full pipeline success only verified manually |
| **Views correctness** | No DB in test suite | `sql/views_analytics.sql` SQL is never executed in tests |
| **Row count / data volume tests** | Fixtures use 2-row DataFrames | Behaviour with 840+ rows (e.g., batch size limits) is untested |
| **Concurrent run behaviour** | No concurrency tests | Two simultaneous pipeline runs might both attempt the same upserts |
| **Docker healthcheck failures** | Infrastructure tests not in scope | `postgres_staging` downtime impact on ELT is untested |

### The Most Important Gap: Integration Tests

The ETL and ELT loaders are tested with fully mocked database engines. The mock verifies that the correct SQL string is constructed and the correct parameters are passed — but it does not verify that the SQL is valid PostgreSQL, that the schema exists, or that the foreign key constraints are respected.

A production-grade addition would be a Docker-based integration test that:
1. Spins up a real PostgreSQL container
2. Runs `init_analytics.sql` and `init_staging.sql`
3. Runs the full pipeline end-to-end
4. Queries the result and asserts row counts and column values

This would catch the class of bug that only appears when real SQL meets a real database.

---

## Running the Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run only ETL tests
pytest tests/etl/

# Run only ELT tests
pytest tests/elt/

# Run a single test file
pytest tests/etl/test_transform.py

# Run a single test function
pytest tests/etl/test_transform.py::test_null_temperature_drops_row

# Run with print output visible (useful for debugging fixtures)
pytest -s
```

Via Docker:
```bash
docker compose run test
```

---

## Key Files

| File | Testing Role |
|---|---|
| [pytest.ini](../pytest.ini) | Test runner configuration |
| [tests/conftest.py](../tests/conftest.py) | Shared fixtures for both pipelines |
| [tests/etl/test_extraction.py](../tests/etl/test_extraction.py) | 26 extraction tests |
| [tests/etl/test_transform.py](../tests/etl/test_transform.py) | 30 transformation tests |
| [tests/etl/test_load.py](../tests/etl/test_load.py) | 12 load tests |
| [tests/etl/test_main.py](../tests/etl/test_main.py) | 8 orchestration tests |
| [tests/elt/test_extract.py](../tests/elt/test_extract.py) | 6 ELT extraction tests |
| [tests/elt/test_transform.py](../tests/elt/test_transform.py) | 12 ELT SQL transform tests |
| [tests/elt/test_load.py](../tests/elt/test_load.py) | 19 ELT staging loader tests |
| [tests/elt/test_main.py](../tests/elt/test_main.py) | 7 ELT orchestration tests |
| [requirements.txt](../requirements.txt) | `pytest>=8.2.0`, `pytest-mock>=3.14.0` |
