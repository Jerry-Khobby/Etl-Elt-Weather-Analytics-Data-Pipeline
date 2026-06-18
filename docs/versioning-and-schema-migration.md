# Versioning — Code Versioning and Schema Change Management

## Overview

This project uses **Git for code versioning** and a **manually-managed, idempotent DDL** approach for schema versioning. There is no migration framework (no Alembic, no Flyway, no Liquibase). Schema changes are applied by modifying the DDL files and running `ALTER TABLE` statements directly, with `IF NOT EXISTS` guards making DDL scripts safe to re-run. This section documents what the current approach is, why it is appropriate for this project's scale, and what a migration-framework approach would look like.

---

## Code Versioning — Git

The project's entire history is managed in Git with a conventional commit message style:

```
0608584 docs: added another form of extensive documentation for this project
5cac52f docs: added the documentation for the project
8f93699 test: tested my pipeline and corrected errors end to end
7fa530f feat: added airflow dags for orchestrations to the project
77fbe63 test: created test cases for the elt, such that everything is tested in elt
5779ccb feat: created the elt section of the project
cdc45f1 test: added test cases to the etl section of the project
943b2bf feat: implemented the etl pipeline for the project e2e, testing left
f62aaeb feat: containerize weather analytics pipeline with Docker and Airflow
070d283 feat: added the docker setup of the project to it
```

### Commit Message Convention

The project uses a prefix convention: `feat:`, `test:`, `docs:`. This is a subset of [Conventional Commits](https://www.conventionalcommits.org/) and makes it possible to scan the log and understand what type of change each commit introduced without reading the diff.

**What is committed**:
- All application code (`jobs/`, `tests/`, `airflow/dags/`)
- All SQL DDL files (`sql/`, `models/`)
- Docker configuration (`Dockerfile`, `docker-compose.yml`)
- Dependency declarations (`requirements.txt`)
- Configuration templates (`.env.example`)
- Documentation (`docs/`)

**What is never committed** (via `.gitignore`):
- `.env` (secrets)
- `venv/` (installed packages)
- `data/raw/` and `data/processed/` (pipeline output — derived artefacts, not source)
- `airflow/logs/` (operational logs)
- `__pycache__/` and `.pyc` files

---

## Schema Versioning — Current Approach

### The DDL Files Are the Schema Version

The schema at any point in time is fully described by three SQL files:

| File | Contents |
|---|---|
| [sql/init_staging.sql](../sql/init_staging.sql) | `weather_raw_staging` table |
| [sql/init_analytics.sql](../sql/init_analytics.sql) | Four dimension tables + `fact_weather` + indexes |
| [sql/views_analytics.sql](../sql/views_analytics.sql) | Five analytical views |

These files are committed to Git. The schema version is implicit: the current state of these files on the `main` branch is the authoritative schema definition.

A schema change is recorded in Git as a commit that modifies one or more of these files. `git log sql/` shows every schema change in the project's history. `git diff HEAD~1 sql/init_analytics.sql` shows exactly what changed between the previous and current schema.

### Idempotent DDL — Safe Re-runs

All `CREATE` statements use `IF NOT EXISTS`:

```sql
CREATE TABLE IF NOT EXISTS fact_weather (...);
CREATE INDEX IF NOT EXISTS idx_fact_weather_location ON fact_weather(location_id);
```

Views use `CREATE OR REPLACE`:

```sql
CREATE OR REPLACE VIEW vw_hourly_weather AS ...;
```

This means the init scripts can be re-run at any time without destroying data or raising errors. A fresh deployment (`docker compose up`) runs the init scripts against empty databases and creates the schema. An upgrade deployment can re-run the same scripts — tables that already exist are skipped, views are replaced atomically.

### The models/schema.sql Reference File

[models/schema.sql](../models/schema.sql) is a reference copy of the full schema in a single file. It serves as documentation — a developer can open one file and see the complete star schema without navigating between `init_staging.sql`, `init_analytics.sql`, and the loader's embedded DDL strings. It is manually kept in sync with the authoritative DDL in `sql/`.

---

## How Schema Changes Are Applied Today

Because there is no migration framework, schema changes require manual steps.

### Adding a New Column (Safe — Additive)

**Example**: Add `feels_like_celsius` to `fact_weather`.

**Step 1** — Update the DDL in [sql/init_analytics.sql](../sql/init_analytics.sql):
```sql
CREATE TABLE IF NOT EXISTS fact_weather (
    ...
    feels_like_celsius   NUMERIC(5,2),  -- added
    ...
);
```

**Step 2** — Add the `ALTER TABLE` for existing databases:
```sql
-- Run this once against any database that already has the table
ALTER TABLE fact_weather ADD COLUMN IF NOT EXISTS feels_like_celsius NUMERIC(5,6);
```

`ADD COLUMN IF NOT EXISTS` is idempotent — running it twice does nothing.

**Step 3** — Update the loader in [jobs/etl/load.py](../jobs/etl/load.py) to include the new column in the INSERT statement.

**Step 4** — Update the transformer in [jobs/etl/transform.py](../jobs/etl/transform.py) to produce the new column.

**Step 5** — Commit all four changes together:
```
git add sql/init_analytics.sql jobs/etl/load.py jobs/etl/transform.py
git commit -m "feat: add feels_like_celsius to fact_weather"
```

**Historical rows**: Existing rows get `feels_like_celsius = NULL` (no default). The pipeline fills values going forward. This is acceptable for an additive change.

### Removing a Column (Destructive — Handle with Care)

Removing a column from `fact_weather` destroys historical data permanently. The safe approach:

1. Remove the column from all INSERT statements and transformation logic (so new rows no longer populate it)
2. Leave the column in the table — it becomes a nullable column with NULL for all new rows and historical values for old rows
3. Only drop the column after all historical data has been exported or the business has confirmed it is no longer needed

**Column drop** (destructive — only after confirmation):
```sql
ALTER TABLE fact_weather DROP COLUMN IF EXISTS feels_like_celsius;
```

### Renaming a Column (Requires Application Update)

```sql
ALTER TABLE fact_weather RENAME COLUMN old_name TO new_name;
```

This requires updating all references in `load.py`, `transform.py`, the views in `views_analytics.sql`, and `models/schema.sql`. All changes must be deployed atomically or the pipeline will fail between the DB change and the code deployment.

---

## What a Migration Framework Would Add

If this project grew to multiple environments (dev, staging, production) with different teams writing schema changes, a migration framework like **Alembic** (the standard for SQLAlchemy projects) would add:

### Alembic Migration Files

Each schema change would be a numbered migration script:

```
alembic/
└── versions/
    ├── 001_initial_schema.py
    ├── 002_add_feels_like_celsius.py
    ├── 003_add_wind_chill_index.py
    └── ...
```

```python
# 002_add_feels_like_celsius.py
def upgrade():
    op.add_column("fact_weather",
        sa.Column("feels_like_celsius", sa.Numeric(5, 2), nullable=True)
    )

def downgrade():
    op.drop_column("fact_weather", "feels_like_celsius")
```

### Migration History Table

Alembic creates an `alembic_version` table in the database:

```sql
SELECT * FROM alembic_version;
-- version_num
-- 003_add_wind_chill_index
```

This records exactly which migrations have been applied to a specific database. `alembic upgrade head` applies all pending migrations in order. `alembic history` shows the full migration timeline.

### Rollback Support

Each migration has an `upgrade()` and `downgrade()` function. If a migration causes a problem:

```bash
alembic downgrade -1  # roll back one migration
```

This is the key advantage over the current approach: reversibility.

### Why Alembic Was Not Used Here

| Consideration | Assessment |
|---|---|
| **Single environment** | One developer, one database per Docker Compose stack. No coordination between environments needed. |
| **Schema is stable** | The star schema was designed upfront and has not required structural changes since the initial commit. |
| **Idempotent DDL is sufficient** | `IF NOT EXISTS` handles re-runs without tracking history. |
| **Added complexity** | Alembic requires an `alembic.ini`, a `migrations/env.py`, and awareness of the `alembic_version` table. For 5 tables, this is overhead without clear benefit. |
| **When to add it** | If this project were deployed to a shared staging database or required coordinating schema changes across a team, Alembic would be the correct addition. |

---

## The Python-Embedded DDL

In addition to the SQL files, the loader embeds DDL directly as Python strings in [jobs/etl/load.py](../jobs/etl/load.py):

```python
ALL_DDL = [
    CREATE_DIM_LOCATION,
    CREATE_DIM_DATE,
    CREATE_DIM_TIME,
    CREATE_DIM_WEATHER_CONDITION,
    CREATE_FACT_WEATHER,
    CREATE_INDEXES,
]
```

This DDL runs every time `WeatherDataLoader.initialize_schema()` is called — which is every pipeline run. Because all statements use `IF NOT EXISTS`, this is a no-op after the first run but guarantees that the schema exists before any data is written.

**Sync requirement**: The DDL in `load.py` and the DDL in `sql/init_analytics.sql` must be kept in sync manually. A schema change must be applied to both. This is the main maintenance cost of the current approach — there is no single source of truth for the schema definition.

A future improvement would be to have `initialize_schema()` read from `sql/init_analytics.sql` directly, eliminating the duplicate:

```python
def initialize_schema(self) -> None:
    ddl_path = Path(__file__).parents[2] / "sql" / "init_analytics.sql"
    ddl = ddl_path.read_text()
    with self.engine.begin() as conn:
        conn.execute(text(ddl))
```

---

## Key Files

| File | Versioning Role |
|---|---|
| [sql/init_analytics.sql](../sql/init_analytics.sql) | Authoritative analytics schema DDL |
| [sql/init_staging.sql](../sql/init_staging.sql) | Authoritative staging schema DDL |
| [sql/views_analytics.sql](../sql/views_analytics.sql) | View definitions (idempotent via `CREATE OR REPLACE`) |
| [models/schema.sql](../models/schema.sql) | Single-file reference copy of the full schema |
| [jobs/etl/load.py](../jobs/etl/load.py) | Embedded DDL strings run at pipeline startup |
| [.gitignore](../.gitignore) | Excludes `.env`, `venv/`, `data/` — keeps repo clean |
