# Code Modularity — Separation of Extract, Transform, Load, and Validate

## Overview

The project is structured so that every pipeline concern has exactly one place in the codebase. Extraction knows nothing about transformation. Transformation knows nothing about databases. The pipeline orchestrators know nothing about HTTP or SQL — they only coordinate objects. This separation makes each component independently testable, independently replaceable, and independently readable.

---

## The Module Map

```
jobs/
├── config.py              ← all configuration values (no logic)
├── utils/
│   └── logger.py          ← logging factory (no pipeline logic)
├── etl/
│   ├── extraction.py      ← HTTP client, API calls, response parsing
│   ├── transform.py       ← data cleaning, validation, derived fields
│   ├── load.py            ← star schema DDL and database writes
│   └── main.py            ← orchestrates the three above, nothing else
└── elt/
    ├── extract.py         ← thin wrapper — reuses ETL extractor
    ├── transform.py       ← SQL-based transformation from staging
    ├── load.py            ← staging table DDL, inserts, mark_processed
    └── main.py            ← orchestrates the five ELT steps
```

Each file has **one reason to change**. If the API response format changes, only `extraction.py` changes. If a new derived field is needed, only `transform.py` changes. If the star schema gains a column, only `load.py` changes. The orchestrator (`main.py`) never changes because of a schema or API change.

---

## ETL — The Four Responsibilities

### 1. Extraction — jobs/etl/extraction.py

**Single responsibility**: Talk to the Open-Meteo API and return a raw DataFrame.

```python
@dataclass(frozen=True)
class Location:
    name: str
    country: str
    latitude: float
    longitude: float
    timezone: str

@dataclass(frozen=True)
class ExtractionResult:
    location:     Location
    raw_data:     dict
    extracted_at: str
    record_count: int

class WeatherExtractor:
    def extract_all(self) -> pd.DataFrame: ...   # batch across all locations
    def extract(self, location: Location) -> ExtractionResult: ...  # single location
    def _build_session(self) -> requests.Session: ...  # HTTP client with retry
    def _validate_response_schema(self, data: dict) -> bool: ...
    def _parse_to_dataframe(self, result: ExtractionResult) -> pd.DataFrame: ...
```

**What it knows**: HTTP, the Open-Meteo API contract, retry policy, URL construction, response parsing.

**What it does not know**: SQL, Pandas transformations, file I/O, Airflow.

**Interface contract**: Returns a `pd.DataFrame` with the original API column names (`temperature_2m`, `relative_humidity_2m`, etc.) plus four location metadata columns (`location_name`, `location_country`, `latitude`, `longitude`) and `extracted_at`. This is the only contract between extraction and transformation.

### 2. Transformation — jobs/etl/transform.py

**Single responsibility**: Clean, validate, and enrich the raw DataFrame. Return a transformed DataFrame ready for database insertion.

```python
COLUMN_RENAME_MAP = {"time": "timestamp", "temperature_2m": "temperature_celsius", ...}
VALID_RANGES       = {"temperature_celsius": (-90.0, 60.0), ...}
REQUIRED_COLUMNS   = ["timestamp", "temperature_celsius", ...]
WEATHER_CODE_MAP   = {0: ("Clear sky", "Clear"), 61: ("Slight rain", "Rain"), ...}

class WeatherDataTransformer:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._rename_columns(df)
        df = self._convert_timestamps(df)
        df = self._cast_numeric_types(df)
        df = self._handle_missing_values(df)
        df = self._remove_duplicates(df)
        df = self._validate_measurements(df)
        df = self._add_derived_fields(df)
        self._validate_required_columns(df)
        return df
```

The eight private methods are called in a fixed order by `transform()`. Each method takes a DataFrame and returns a DataFrame — a pipeline of pure functions over data. The caller (`main.py`) calls `transform(raw_df)` and gets back a clean DataFrame; it has no knowledge of which validations ran or in what order.

**What it knows**: Pandas, the raw column names, the analytics column names, WMO code semantics, validation thresholds.

**What it does not know**: HTTP, SQL, file paths, Airflow.

### 3. Loading — jobs/etl/load.py

**Single responsibility**: Write a clean, transformed DataFrame into the star schema.

```python
class WeatherDataLoader:
    def initialize_schema(self) -> None: ...   # CREATE TABLE IF NOT EXISTS
    def load(self, df: pd.DataFrame) -> None:  # upsert dims + insert fact
        self._upsert_dim_location(df)
        self._upsert_dim_date(df)
        self._upsert_dim_time(df)
        self._upsert_dim_weather_condition(df)
        self._insert_fact_weather(df)
    def _resolve_dimension_ids(self, df: pd.DataFrame) -> pd.DataFrame: ...
```

The loader reads the transformed DataFrame column names (`temperature_celsius`, `location_name`, etc.) — the same names the transformer produces. It does not know about API column names. It does not apply any business logic; it only maps DataFrame columns to database columns.

**What it knows**: PostgreSQL, the star schema structure, SQLAlchemy, `ON CONFLICT DO NOTHING`.

**What it does not know**: HTTP, Pandas transformations, WMO codes, Airflow.

### 4. Orchestration — jobs/etl/main.py

**Single responsibility**: Instantiate the three components and call them in the correct order.

```python
class WeatherEtlPipeline:
    def __init__(self):
        self._extractor   = WeatherExtractor()
        self._transformer = WeatherDataTransformer()
        self._loader      = WeatherDataLoader()

    def run(self) -> None:
        raw_df         = self._extract()
        self._save_raw(raw_df)
        transformed_df = self._transformer.transform(raw_df)
        self._loader.initialize_schema()
        self._loader.load(transformed_df)

    def _extract(self) -> pd.DataFrame:
        results = self._extractor.extract_all()
        return pd.concat([r.to_dataframe() for r in results], ignore_index=True)

    def _save_raw(self, raw_df: pd.DataFrame) -> None:
        path = DATA_RAW_DIR / f"weather_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        raw_df.to_csv(path, index=False)
```

`main.py` is intentionally thin — 82 lines total. It knows about the three components but not their internals. It handles the cross-cutting concern of saving the raw audit file (which neither extraction nor transformation should own). All exceptions propagate upward to the Airflow task.

**What it knows**: The three component classes, file I/O for raw CSV, logging.

**What it does not know**: HTTP, SQL, Pandas internals, WMO codes.

---

## ELT — Five Responsibilities

The ELT pipeline adds two responsibilities not present in ETL: staging and the mark-processed idiom.

```python
class WeatherEltPipeline:
    def __init__(self):
        self._staging_loader    = StagingLoader()       # jobs/elt/load.py
        self._transformer       = StagingTransformer()  # jobs/elt/transform.py
        self._analytics_loader  = WeatherDataLoader()   # jobs/etl/load.py (shared!)

    def run(self) -> None:
        raw_df = extract_raw()                                   # jobs/elt/extract.py

        self._staging_loader.initialize_schema()
        self._staging_loader.load(raw_df)

        transformed_df, staging_ids = self._transformer.transform()

        if transformed_df.empty:
            return

        self._analytics_loader.initialize_schema()
        self._analytics_loader.load(transformed_df)
        self._staging_loader.mark_processed(staging_ids)
```

### Reuse of the ETL Loader

`WeatherDataLoader` (from `jobs/etl/load.py`) is reused directly by the ELT pipeline. This is the **DRY principle** applied to the loading layer: both pipelines target the same star schema, so they share the same loader. The ELT pipeline imports and instantiates `WeatherDataLoader` without duplicating any of its SQL.

### ELT-Specific Components

| File | Responsibility |
|---|---|
| `jobs/elt/extract.py` | `extract_raw()` — a module-level function (not a class) that calls `WeatherExtractor.extract_all()`. Thin wrapper: the ELT pipeline needs the same extraction, so this delegates entirely to the ETL extractor rather than duplicating it. |
| `jobs/elt/transform.py` | `StagingTransformer` — issues a SQL SELECT from `weather_raw_staging WHERE is_processed = FALSE`, returns `(DataFrame, list[int])`. The transformation logic lives in SQL rather than Python. |
| `jobs/elt/load.py` | `StagingLoader` — manages the staging table: DDL initialization, raw row insertion, `fetch_unprocessed_ids`, and `mark_processed`. The staging table is an ELT-only concern. |

---

## Why This Structure

### Testability

Because each component has no hidden dependencies on the others, they can be tested in complete isolation:

- `WeatherDataTransformer.transform()` is tested by passing in a hand-crafted DataFrame. No HTTP mock, no database mock.
- `WeatherDataLoader.load()` is tested by passing in a hand-crafted DataFrame and a mocked engine. No HTTP mock, no transformation logic.
- `WeatherExtractor.extract()` is tested by mocking `requests.Session.get`. No DataFrame logic, no database.

The test suite's 120 tests contain zero integration tests — every test runs in milliseconds because nothing talks to a real external system.

### Replaceability

Each component can be replaced without touching the others:

- Replace the API source: write a new class with the same interface as `WeatherExtractor` (returns a raw DataFrame with the agreed column names). The transformer and loader are unchanged.
- Replace the transformation engine (e.g., switch from Pandas to Polars): rewrite `WeatherDataTransformer` to return a DataFrame-compatible output. The extractor and loader are unchanged.
- Replace the target database (e.g., switch from PostgreSQL to BigQuery): rewrite `WeatherDataLoader`. The extractor and transformer are unchanged.

### Readability

The pipeline intent is readable from `main.py` alone — without knowing any implementation details:

```python
raw_df = self._extract()          # get data from the API
self._save_raw(raw_df)            # save it before we touch it
transformed_df = self._transformer.transform(raw_df)  # clean and enrich
self._loader.initialize_schema()  # ensure the DB is ready
self._loader.load(transformed_df) # write to the star schema
```

A new team member reads this and understands the full pipeline in 5 lines without reading 500 lines of HTTP and SQL code.

### Single Responsibility (SOLID — SRP)

| Class / Module | One reason to change |
|---|---|
| `WeatherExtractor` | Open-Meteo API contract changes |
| `WeatherDataTransformer` | Business rules for cleaning/enriching weather data change |
| `WeatherDataLoader` | Star schema structure changes |
| `WeatherEtlPipeline` | The sequence of ETL steps changes |
| `StagingLoader` | Staging table schema changes |
| `StagingTransformer` | SQL transformation logic changes |
| `WeatherEltPipeline` | The sequence of ELT steps changes |
| `config.py` | Configuration values or environment variable names change |
| `logger.py` | Logging format or handler configuration changes |

No class has two reasons to change. This is what makes the codebase maintainable as it grows.

---

## Dependency Direction

Dependencies flow inward — from orchestrator to components, never between components:

```
main.py (WeatherEtlPipeline)
    ├── imports WeatherExtractor   (extraction.py)
    ├── imports WeatherDataTransformer (transform.py)
    └── imports WeatherDataLoader  (load.py)

extraction.py     → imports config.py, logger.py, requests
transform.py      → imports config.py, logger.py, pandas, numpy
load.py           → imports config.py, logger.py, sqlalchemy
```

`extraction.py` never imports `transform.py`. `transform.py` never imports `load.py`. There are no circular dependencies. This is the **Dependency Inversion Principle** in practice: high-level modules (`main.py`) depend on abstractions (classes with defined interfaces), not on each other's internals.

---

## Key Files

| File | Modularity Role |
|---|---|
| [jobs/etl/extraction.py](../jobs/etl/extraction.py) | Extract responsibility — HTTP only |
| [jobs/etl/transform.py](../jobs/etl/transform.py) | Transform responsibility — Pandas only |
| [jobs/etl/load.py](../jobs/etl/load.py) | Load responsibility — SQL only |
| [jobs/etl/main.py](../jobs/etl/main.py) | ETL orchestration — coordination only |
| [jobs/elt/extract.py](../jobs/elt/extract.py) | ELT extraction — delegates to ETL extractor |
| [jobs/elt/transform.py](../jobs/elt/transform.py) | SQL transform responsibility |
| [jobs/elt/load.py](../jobs/elt/load.py) | Staging responsibility |
| [jobs/elt/main.py](../jobs/elt/main.py) | ELT orchestration — 5-step coordination |
| [jobs/config.py](../jobs/config.py) | Cross-cutting config — no logic |
| [jobs/utils/logger.py](../jobs/utils/logger.py) | Cross-cutting logging — no logic |
