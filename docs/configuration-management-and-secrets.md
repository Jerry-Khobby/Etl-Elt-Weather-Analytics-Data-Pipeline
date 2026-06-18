# Configuration Management & Secrets — Keeping Credentials Out of Code

## Overview

Secrets (passwords, API keys, Fernet encryption keys) must never appear in source code or Docker images. This project enforces a clean separation: code is committed to Git, secrets live in a `.env` file on the host machine, and the `.env` file is explicitly excluded from Git via `.gitignore`. Configuration values that vary between environments (dev/staging/production) flow in as environment variables; code only reads them at runtime.

---

## The Secrets Boundary

```
.gitignore          ← .env is listed here; Git never sees it
    │
    ▼
.env                ← Lives on the host machine ONLY; never committed
    │
    ├── docker-compose.yml reads → env_file: .env
    │                           → environment: VAR=${ENV_VAR}
    │
    └── jobs/config.py reads    → os.getenv("VAR", "default")
```

Everything outside the `.env` file is safe to commit. Everything inside it stays on the machine.

---

## .gitignore — The First Line of Defence

**File**: [.gitignore](../.gitignore)

```
.env
venv/**
venv
```

The `.env` file is the first entry. Git will never stage or commit it, even if someone runs `git add .`. This is the hard boundary that prevents accidental credential exposure.

**What is committed**:
- [`.env.example`](../.env.example) — a template showing the required variable names with placeholder values. This is the contract between the secrets file and the code.

**What is never committed**:
- `.env` — the actual secrets file with real values

---

## .env.example — The Contract

**File**: [.env.example](../.env.example)

```dotenv
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=changeme_airflow
AIRFLOW_FERNET_KEY=
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=changeme_admin
STAGING_DB_NAME=staging_db
STAGING_DB_USER=staging_user
STAGING_DB_PASSWORD=changeme_staging
ANALYTICS_DB_USER=analytics_user
ANALYTICS_DB_PASSWORD=changeme_analytics
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

This file serves three purposes:
1. **Onboarding**: A new developer copies it to `.env` and fills in real values to get a working environment.
2. **Documentation**: It declares every environment variable the project requires — the canonical list of what needs to be configured.
3. **Diff signal**: If a new secret is added to the project, `.env.example` must be updated, which shows up in the Git diff as an intentional change. A secret added to code but not to `.env.example` is a review signal.

The `changeme_` prefix on placeholder passwords is deliberate — it makes it obvious which values are placeholders and must be replaced before any real deployment.

---

## docker-compose.yml — Injecting Secrets into Containers

**File**: [docker-compose.yml](../docker-compose.yml)

Docker Compose reads the `.env` file from the project root automatically. Service definitions reference environment variables using the `${VARIABLE_NAME}` substitution syntax:

```yaml
postgres_staging:
  image: postgres:14-alpine
  environment:
    POSTGRES_DB:       ${STAGING_DB_NAME}
    POSTGRES_USER:     ${STAGING_DB_USER}
    POSTGRES_PASSWORD: ${STAGING_DB_PASSWORD}

postgres_analytics:
  image: postgres:14-alpine
  environment:
    POSTGRES_DB:       ${ANALYTICS_DB_NAME}
    POSTGRES_USER:     ${ANALYTICS_DB_USER}
    POSTGRES_PASSWORD: ${ANALYTICS_DB_PASSWORD}

airflow-server:
  env_file: .env
  environment:
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: >-
      postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@postgres_airflow/${AIRFLOW_DB_NAME}
```

**What this achieves**:
- Docker containers receive secrets as environment variables at startup
- The actual values are never written into `docker-compose.yml` — only variable references like `${VAR}`
- A `docker inspect` on the container will show the resolved values, but the YAML file in Git is clean
- Rotating a password means updating `.env` on the host and restarting the container — no code change, no commit

### env_file vs environment Block

Two mechanisms are used:

| Mechanism | Usage in this project | Effect |
|---|---|---|
| `env_file: .env` | Airflow services | Injects all variables from `.env` as environment variables in the container |
| `environment: VAR=${ENV_VAR}` | All services | Explicitly maps a specific env var, allows rename and compose-time substitution |

The `env_file` approach is used for Airflow because Airflow's configuration relies on many `AIRFLOW__*` variables that come from `.env`. The `environment` block approach is used for the PostgreSQL containers because only specific variables are needed.

---

## jobs/config.py — Reading Secrets in Python

**File**: [jobs/config.py](../jobs/config.py)

The config module loads environment variables at import time using `python-dotenv`:

```python
from dotenv import load_dotenv
import os

load_dotenv()  # reads .env from the current directory (or any parent)

# Database connection strings — credentials from environment, never hardcoded
ANALYTICS_DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('ANALYTICS_DB_USER', 'analytics_user')}:"
    f"{os.getenv('ANALYTICS_DB_PASSWORD', 'analytics_pass')}@"
    f"{os.getenv('ANALYTICS_DB_HOST', 'localhost')}:"
    f"{os.getenv('ANALYTICS_DB_PORT', '5432')}/"
    f"{os.getenv('ANALYTICS_DB_NAME', 'weather_analytics')}"
)

STAGING_DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('STAGING_DB_USER', 'staging_user')}:"
    f"{os.getenv('STAGING_DB_PASSWORD', 'staging_pass')}@"
    f"{os.getenv('STAGING_DB_HOST', 'localhost')}:"
    f"{os.getenv('STAGING_DB_PORT', '5434')}/"
    f"{os.getenv('STAGING_DB_NAME', 'staging_db')}"
)

# Log level — configurable, safe to expose
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR   = os.getenv("LOG_DIR",   "logs")
```

**Key patterns**:

1. **`load_dotenv()` at the top** — reads the `.env` file and populates `os.environ`. In Docker containers, `load_dotenv()` is a no-op because the variables are already in the environment (injected by Docker Compose). In local development without Docker, it reads from the `.env` file directly.

2. **`os.getenv("KEY", "default")`** — every secret has a fallback default. The defaults match the `docker-compose.yml` service names and the `.env.example` placeholder values, so the pipeline works out-of-the-box with the example credentials in a local Docker environment.

3. **Credentials are only in the connection string** — no password variable is ever logged, printed, or stored as a plain attribute. The full URL is constructed once and passed directly to `create_engine()`.

4. **No API key** — the Open-Meteo API is entirely free and requires no authentication. The `BASE_URL` in `config.py` is not a secret.

---

## Dockerfile — No Secrets in the Image

**File**: [Dockerfile](../Dockerfile)

```dockerfile
FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y libpq-dev gcc

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt"
```

The Dockerfile:
- Installs system and Python dependencies only
- Copies `requirements.txt` (no secrets in it)
- Contains zero credentials, connection strings, or passwords

Secrets are injected at container **runtime** by Docker Compose via `env_file` and `environment` blocks. The built image is clean and can be pushed to any container registry without leaking credentials. Rebuilding the image never requires access to the `.env` file.

---

## The Airflow Fernet Key

The `AIRFLOW_FERNET_KEY` is a special case — it is Airflow's symmetric encryption key for secrets stored in the Airflow metadata database (e.g., connection passwords stored in the Airflow UI).

```dotenv
AIRFLOW_FERNET_KEY=<generate — see command below>
```

This key is:
- Generated once (`from cryptography.fernet import Fernet; Fernet.generate_key()`)
- Stored in `.env` only
- Injected into Airflow via `AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}`
- Never in code, never in `docker-compose.yml` literally

If this key were lost or rotated without re-encrypting stored connections, Airflow would be unable to decrypt its stored secrets. It must be backed up securely outside the repository.

---

## What Is Not a Secret

Not every configuration value is a secret. These are safe to hardcode in `config.py` or `docker-compose.yml`:

| Value | Why it is safe | Where it lives |
|---|---|---|
| Open-Meteo base URL | Public API, no authentication | `config.py` `BASE_URL` |
| City names and coordinates | Public geographic data | `config.py` `LOCATIONS` |
| `LOOKBACK_DAYS = 7` | Operational config, no security impact | `config.py` |
| `MAX_RETRIES = 3` | Operational config | `config.py` |
| Port numbers (5432, 5433, 5434) | Network topology, not credentials | `docker-compose.yml` |
| Log level, log directory | Operational config | `config.py` with env override |
| WMO weather code map | Public meteorological standard | `transform.py` |

These values are in committed code because they have no security implications. Keeping every configuration value in `.env` would make the project harder to understand without improving security.

---

## Security Gaps and Production Hardening

The current approach is appropriate for a development/learning environment. A production deployment would add:

| Gap | Production approach |
|---|---|
| `.env` file on host | Use Docker Secrets, Kubernetes Secrets, or a secrets manager (HashiCorp Vault, AWS Secrets Manager) |
| Plaintext passwords in `docker-compose.yml` env blocks | Use `secrets:` block in Docker Compose v3 with secret files |
| Single `.env` for all environments | Separate `.env.dev`, `.env.staging`, `.env.prod` managed outside the repo |
| No secret rotation | Automate rotation via the secrets manager with a connection pool restart |
| Airflow Fernet key in `.env` | Store in a secrets manager; inject at deploy time |

---

## Key Files

| File | Secrets Management Role |
|---|---|
| [.gitignore](../.gitignore) | `/.env` excluded — the hard boundary |
| [.env.example](../.env.example) | Template showing required variables with placeholder values |
| [jobs/config.py](../jobs/config.py) | `load_dotenv()` + `os.getenv()` — runtime credential injection |
| [docker-compose.yml](../docker-compose.yml) | `env_file: .env` + `${VAR}` substitution — container-level injection |
| [Dockerfile](../Dockerfile) | No secrets; dependencies only |
