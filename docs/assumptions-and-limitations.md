# Assumptions & Limitations — Explicit Scope Boundaries

## Overview

Every system is built on assumptions. When those assumptions are documented, the system is easier to extend correctly and easier to hand off to a new team. This document states explicitly what this pipeline assumes to be true, what it has been built to handle, and what is deliberately outside its scope. Each item includes why the scope decision was made, so that future maintainers can judge whether a new requirement changes the calculus.

---

## Data Source Assumptions

### Single API Source — Open-Meteo Only

**Assumption**: All weather data comes from a single source: the Open-Meteo public API (`https://api.open-meteo.com/v1/forecast`).

**Impact**: The pipeline has one extraction path. There is no merging logic, no conflict resolution between sources, and no deduplication across providers. The `extracted_at` column in `fact_weather` records when the Open-Meteo API was called, not a generic "data source" field.

**Out of scope**: Integrating a second API (e.g., WeatherAPI, AccuWeather, NOAA) would require a new extractor class, a source column in the fact table to identify which API a row came from, and conflict resolution logic when two APIs report different temperatures for the same city-hour.

### No Authentication on the API

**Assumption**: The Open-Meteo API requires no API key. All 15 hourly variables are available in the free tier without registration.

**Impact**: There is no API key management, no key rotation, and no rate-limit quota tracking beyond the built-in retry on 429 responses. The `BASE_URL` in `config.py` is not a secret.

**Out of scope**: If the project switched to a paid weather API (e.g., WeatherAPI with a key), the key would need to be added to `.env`, `config.py`, and the extraction request headers. See [configuration-management-and-secrets.md](configuration-management-and-secrets.md).

### Fixed Set of Locations

**Assumption**: The five cities — Accra (Ghana), London (United Kingdom), New York (United States), Tokyo (Japan), Berlin (Germany) — are hardcoded in `jobs/config.py` and do not change between pipeline runs.

**Impact**: `dim_location` has exactly 5 rows. There is no UI or configuration mechanism for adding cities at runtime. Adding a city requires a code change and a redeployment.

**Out of scope**: Dynamic location management (user-configurable city lists, location lookup from a database, geofencing). To add a city today, edit the `LOCATIONS` list in `config.py` and redeploy.

---

## Data Granularity Assumptions

### Daily Pipeline Runs, Hourly Data Grain

**Assumption**: The Airflow DAG runs once per day (`@daily`). The data it loads has hourly granularity (one row per city per hour per day in the fact table).

**Impact**: The pipeline is not designed for real-time or sub-hourly ingestion. If the DAG runs twice on the same day (e.g., a manual re-trigger after a failure), `ON CONFLICT DO NOTHING` prevents duplicate rows, but no new data is added because the data is the same.

**Out of scope**: Real-time ingestion (streaming), sub-hourly granularity (e.g., per-minute readings), or event-driven triggers (e.g., run whenever the API publishes a new forecast).

### 7-Day Lookback Window — No Full Historical Load

**Assumption**: Each pipeline run fetches the 7 most recent days of data. Open-Meteo can provide years of historical data, but the pipeline does not load it automatically.

**Impact**: If the pipeline is deployed for the first time today, only the last 7 days of data will be present. Historical data from before that window must be loaded manually via a backfill (see [incremental-loading-and-backfills.md](incremental-loading-and-backfills.md)).

**Out of scope**: Full historical load on first deployment. To load 2 years of history, set `LOOKBACK_DAYS=730` in `config.py`, run the pipeline once, then restore `LOOKBACK_DAYS=7`.

---

## Data Quality Assumptions

### API Data Is Assumed Accurate

**Assumption**: Values returned by Open-Meteo within the physical validation ranges are accepted as correct. The pipeline validates that `temperature_celsius` is between -90°C and 60°C, but it does not verify that 38°C in Accra is plausible for June.

**Impact**: The pipeline is not a climatological validation system. Unusual-but-physically-possible values (e.g., a temperature spike during a heat wave) are accepted without cross-checking against historical norms.

**Out of scope**: Statistical anomaly detection (z-score checks, interquartile range filtering, comparison against historical baseline). Implementing this would require historical aggregate statistics in the database and a more sophisticated validation step.

### Null Fills Are Meteorologically Conservative

**Assumption**: A null value for `precipitation`, `rain`, or `snowfall` means zero precipitation occurred (not that the measurement is unknown).

**Impact**: Missing precipitation data is filled with 0.0, not with `NULL`. This preserves the row (avoiding the null-drop for critical columns) but introduces a conservative bias — the pipeline assumes dry when data is absent.

**Out of scope**: Distinguishing between "measured zero precipitation" and "sensor failure / missing data". A `precipitation_data_quality` flag column would be needed to capture this distinction.

---

## Personally Identifiable Information (PII)

**Assumption**: This pipeline processes only meteorological observations. There is no user data, location tracking of individuals, or any personally identifiable information anywhere in the pipeline.

**Impact**: No data masking, anonymisation, GDPR compliance measures, or PII audit logging are implemented. The pipeline is PII-free by design — the data subject is the weather, not a person.

**Out of scope**: Any extension that would associate weather data with user behaviour, location history of individuals, or device identifiers would require a full PII impact assessment before implementation.

---

## Infrastructure Assumptions

### Single-Node Docker Compose Deployment

**Assumption**: The pipeline runs on a single machine via Docker Compose. All services (Airflow, PostgreSQL ×3, ETL, ELT, Metabase) run on the same host.

**Impact**:
- No horizontal scaling (the ETL runs on one worker)
- No high availability (if the host machine goes down, the pipeline stops)
- Resource contention between services is possible (Airflow, ETL container, and three PostgreSQL instances all share the host's CPU and memory)
- Network latency between services is negligible (all on `docker bridge` network)

**Out of scope**: Kubernetes deployment, distributed task execution (Airflow CeleryExecutor or KubernetesExecutor), multi-region redundancy, or cloud-managed databases (RDS, Cloud SQL).

### PostgreSQL as the Only Database

**Assumption**: Both the staging layer and the analytics star schema run on PostgreSQL 14. No other database engine is supported.

**Impact**: SQL in `transform.py` uses PostgreSQL-specific syntax (`::TIMESTAMP`, `EXTRACT`, `TIMESTAMPTZ`, `ANY(:ids)`, `ON CONFLICT DO NOTHING`). The `StagingTransformer` SQL and `WeatherDataLoader` SQL are not portable to MySQL, SQLite, or BigQuery without modifications.

**Out of scope**: Database engine abstraction, multi-database support. If the target were changed to a cloud data warehouse, both the SQL transformation queries and the loader INSERT statements would need to be rewritten.

---

## Pipeline Design Assumptions

### No Real-Time or Near-Real-Time Requirements

**Assumption**: Stakeholders are satisfied with data that is at most 25 hours old (one daily pipeline run + the 7-day lookback buffer). There is no SLA requiring data to be available within minutes of it being generated by Open-Meteo.

**Impact**: The DAG schedule is `@daily`. No streaming components (Kafka, Kinesis, Flink) are used.

**Out of scope**: Any use case that requires weather data within an hour of it being published by the API.

### Both Pipelines Target the Same Schema

**Assumption**: The ETL and ELT pipelines are two independent implementations that produce the same output in the same `weather_analytics` database. This project exists to demonstrate both patterns; in a production system, only one would be deployed.

**Impact**: Running both pipelines produces redundant writes (the same hourly rows are attempted twice; `ON CONFLICT DO NOTHING` silently discards the duplicates). There is no conflict, but there is wasted work.

**Out of scope**: A combined ETL+ELT pipeline that dynamically chooses a path based on data volume or configuration. In production, pick one.

### No Schema Migration Framework

**Assumption**: The analytics schema is stable once deployed. Schema changes are applied manually with `ALTER TABLE` statements and committed to the DDL files.

**Impact**: There is no migration version table. A developer deploying to a new environment runs the init scripts; a developer upgrading an existing environment runs `ALTER TABLE` commands manually.

**Out of scope**: Automated schema migration (Alembic, Flyway). See [versioning-and-schema-migration.md](versioning-and-schema-migration.md) for the migration framework design.

---

## Monitoring Assumptions

### No External Alerting in the Current Implementation

**Assumption**: Pipeline failures are visible via the Airflow UI and log file. No Slack messages, emails, or PagerDuty alerts are sent automatically.

**Impact**: An operator must actively monitor the Airflow UI or the log file to detect failures. A failing overnight run may not be noticed until the next morning.

**Out of scope** (but designed): See [alerting-and-failure-notification.md](alerting-and-failure-notification.md) for the full design of email and Slack alerting.

### Slowly Changing Dimensions Are Type 1 Only

**Assumption**: Dimension attributes (city coordinates, WMO code descriptions) do not change meaningfully over time. The current Type 1 SCD (overwrite-or-ignore) is sufficient.

**Impact**: If a city's coordinates are corrected in the API, the correction is silently ignored by `ON CONFLICT DO NOTHING`. Historical fact rows are not re-associated.

**Out of scope**: SCD Type 2 (versioned dimension rows with effective date ranges). See [slowly-changing-dimensions.md](slowly-changing-dimensions.md) for the design.

---

## Summary Table

| Assumption / Limitation | Category | Out-of-scope extension |
|---|---|---|
| Single API source (Open-Meteo) | Data source | Multi-source merging |
| No API authentication required | Data source | API key management |
| 5 hardcoded cities | Data source | Dynamic location management |
| Daily pipeline, hourly grain | Granularity | Real-time / streaming |
| 7-day lookback only | Granularity | Auto full historical load |
| Physical range validation only | Data quality | Statistical anomaly detection |
| Null precipitation = zero | Data quality | Measurement quality flags |
| No PII in the dataset | Privacy | PII masking, GDPR compliance |
| Single-node Docker Compose | Infrastructure | Kubernetes, cloud deployment |
| PostgreSQL only | Infrastructure | Cloud DWH (BigQuery, Snowflake) |
| Daily SLA acceptable | Operations | Sub-hourly freshness |
| Both ETL and ELT run together | Operations | Production: pick one pipeline |
| Manual schema migrations | Maintenance | Alembic/Flyway |
| No external alerting | Monitoring | Slack/email on failure |
| SCD Type 1 | Data modeling | SCD Type 2 for changing dimensions |
