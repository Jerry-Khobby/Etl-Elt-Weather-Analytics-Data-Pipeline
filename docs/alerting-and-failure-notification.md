# Alerting & Failure Notification

## Overview

Alerting is the mechanism that brings failures to human attention **without requiring someone to actively check**. In this project, failure notification is currently implicit — failures surface through Airflow task states (visible in the UI) and log entries (visible if someone opens the log file). No external notification is sent automatically when a pipeline run fails.

This document describes what alerting the project currently has, what it is missing, and a concrete design for adding proactive alerting.

---

## What Exists Today

### Airflow UI — Visual Task State

The Airflow web interface at `http://localhost:8080` shows the state of every task in every DAG run:

| State | Visual | Meaning |
|---|---|---|
| `success` | Green | Task completed without exception |
| `failed` | Red | Task raised an unhandled exception |
| `upstream_failed` | Orange | An upstream task failed; this task was skipped |
| `running` | Light blue | Currently executing |
| `retrying` | Yellow | Failed; waiting for retry delay |

A failing pipeline run turns the affected task red and all downstream tasks orange. This is the current "alert" — an operator must open the Airflow UI, navigate to the DAG, and check the graph view to see this.

**Limitation**: This is a pull model — the operator must look. No notification is pushed to any external channel.

### Log File — ERROR and EXCEPTION Lines

Every failure writes an `ERROR` or `EXCEPTION` (with stack trace) line to `logs/pipeline.log`. An operator who monitors the log file (via `tail -f`, a log aggregator, or log shipping) will see failure lines immediately.

**Limitation**: `pipeline.log` must be actively monitored. In a containerized deployment, logs are also available via `docker logs`, but again require active polling.

### Container Exit Code

When the ETL or ELT container's Python process raises an uncaught exception, Python exits with code `1`. Docker records this as `Exited (1)`. `docker ps -a` shows the container in a failed state. Monitoring tools that watch Docker container health (e.g., Portainer, a cron job running `docker ps`) can detect this.

**Limitation**: No automatic notification when the container exits with a non-zero code.

---

## What Is Missing

| Notification type | Current state | Gap |
|---|---|---|
| Airflow task failure → email | Not configured | `smtp` settings not set in Airflow config |
| Airflow task failure → Slack/webhook | Not configured | No `on_failure_callback` defined |
| SLA breach notification | Not configured | No `sla` set on tasks or DAG |
| Data quality alert (null rate, row count anomaly) | Not configured | No threshold checks exist |
| Freshness alert (data is stale) | Not configured | No `MAX(extracted_at)` age check |

---

## Design: Adding Proactive Alerting

The following additions would make the pipeline production-grade from an alerting perspective. None of these require replacing the existing code — they are additive.

### 1. Airflow Email Alerting on Task Failure

Airflow has built-in email support via `email_on_failure`. Add to `default_args` in [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py):

```python
default_args = {
    "owner":           "weather_pipeline",
    "start_date":      datetime(2026, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
    "email":           ["data-team@example.com"],
    "email_on_failure": True,
    "email_on_retry":   False,  # don't spam on expected retries
}
```

Configure the SMTP server in Airflow's `airflow.cfg` or via environment variables in [docker-compose.yml](../docker-compose.yml):

```yaml
airflow-server:
  environment:
    AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
    AIRFLOW__SMTP__SMTP_PORT: 587
    AIRFLOW__SMTP__SMTP_USER: pipeline@example.com
    AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_PASSWORD}
    AIRFLOW__SMTP__SMTP_MAIL_FROM: pipeline@example.com
```

Airflow will then send an email when any task fails after all retries are exhausted.

### 2. Slack Notification via on_failure_callback

For richer, real-time notifications, define a callback function that posts to a Slack webhook:

```python
import requests as http_requests

def notify_slack_on_failure(context):
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url
    exception = context.get("exception", "unknown error")

    message = (
        f":red_circle: *Pipeline failure*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run*: `{run_id}`\n"
        f"*Error*: `{str(exception)[:200]}`\n"
        f"*Logs*: {log_url}"
    )
    http_requests.post(
        url=SLACK_WEBHOOK_URL,
        json={"text": message},
        timeout=10,
    )

default_args = {
    ...
    "on_failure_callback": notify_slack_on_failure,
}
```

The `context` dictionary that Airflow passes to callbacks contains the full task instance, the exception that caused the failure, the log URL, and the run ID — everything needed to write a useful alert message.

### 3. SLA Miss Notification

Adding an SLA causes Airflow to alert when the DAG takes longer than expected:

```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    message = (
        f":warning: *SLA breach*\n"
        f"DAG `{dag.dag_id}` has not completed within 2 hours.\n"
        f"Blocking tasks: {[ti.task_id for ti in blocking_tis]}"
    )
    http_requests.post(url=SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)

dag = DAG(
    dag_id="weather_etl_pipeline",
    ...
    sla_miss_callback=sla_miss_callback,
    default_args={
        ...
        "sla": timedelta(hours=2),
    },
)
```

An SLA miss does not stop the DAG — it sends a notification while the DAG continues running. This is appropriate for: "tell me if the daily load isn't done before the dashboard opens at 08:00."

### 4. Data Quality Alerts — Anomaly Detection

Add a post-load validation task to the DAG that checks for data quality anomalies:

```python
def check_data_quality(**context):
    engine = create_engine(ANALYTICS_CONNECTION_STRING)

    with engine.connect() as conn:
        # Check 1: Row count for today's data
        result = conn.execute(text("""
            SELECT COUNT(*) as row_count
            FROM fact_weather f
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE d.full_date = CURRENT_DATE
        """)).fetchone()

        expected_rows = 5 * 24  # 5 locations × 24 hours
        actual_rows = result["row_count"]
        if actual_rows < expected_rows * 0.9:   # alert if < 90% expected
            raise ValueError(
                f"Row count anomaly: expected ~{expected_rows}, got {actual_rows}"
            )

        # Check 2: Null rate for temperature
        result = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE temperature_celsius IS NULL) * 100.0 / COUNT(*) AS null_rate
            FROM fact_weather
        """)).fetchone()

        if result["null_rate"] > 5.0:
            raise ValueError(
                f"Temperature null rate too high: {result['null_rate']:.1f}%"
            )

        # Check 3: Data freshness
        result = conn.execute(text("""
            SELECT MAX(extracted_at) AS latest
            FROM fact_weather
        """)).fetchone()

        age_hours = (datetime.utcnow() - result["latest"].replace(tzinfo=None)).total_seconds() / 3600
        if age_hours > 26:
            raise ValueError(
                f"Data is stale: latest record is {age_hours:.1f} hours old"
            )

    logger.info("Data quality checks passed")
```

Add this as the fifth task in the DAG, after `load_weather_data`:

```python
quality_check_task = PythonOperator(
    task_id="check_data_quality",
    python_callable=check_data_quality,
    dag=dag,
)

extract_task >> transform_task >> validate_task >> load_task >> quality_check_task
```

With `on_failure_callback=notify_slack_on_failure` in `default_args`, a failing quality check automatically sends a Slack alert with the specific check that failed.

### 5. ELT Container Alerting

Since the ELT pipeline runs outside Airflow, its failures are not visible in the Airflow UI. Options:

**Option A — Wrap in a second Airflow DAG**: Create `weather_elt_pipeline` DAG with a single `BashOperator` that runs `python -m jobs.elt.main`. This brings ELT under Airflow's retry, SLA, and callback machinery.

**Option B — Docker healthcheck + external monitor**: The ELT container exits with code 1 on failure. An external monitoring tool (Uptime Kuma, a simple cron job) can watch for `docker ps` showing the container in `Exited (1)` state and send a notification.

**Option C — Exit hook in elt/main.py**: Add a `try/except/finally` at the top level that sends a Slack message on failure:

```python
if __name__ == "__main__":
    pipeline = WeatherEltPipeline()
    try:
        pipeline.run()
    except Exception as e:
        http_requests.post(
            url=SLACK_WEBHOOK_URL,
            json={"text": f":red_circle: ELT pipeline failed: {str(e)[:300]}"},
            timeout=10,
        )
        raise
```

---

## Alert Priority Matrix

| Event | Severity | Notification channel | Timing |
|---|---|---|---|
| Airflow task fails (after retries) | High | Email + Slack | Immediate |
| SLA breach (> 2 hours) | Medium | Slack | At SLA deadline |
| Data quality check fails (null rate, row count) | Medium | Slack | After load task |
| Freshness alert (data > 26 hours old) | Medium | Slack | After load task |
| ELT container exits with code 1 | High | Slack (via exit hook) | Immediate |
| Out-of-range values > 5% of rows | Low | Log WARNING | During transform |

---

## Current Alert Coverage vs. Production Readiness

| Scenario | Currently alerted? | How |
|---|---|---|
| API completely unreachable | No (log only) | Airflow UI shows red task |
| All 840 rows dropped by validation | No | Airflow validate task fails; UI shows red |
| Database completely down | No (log only) | Airflow load task fails; UI shows red |
| Pipeline succeeds but data is wrong | No | No post-load quality check |
| Pipeline takes 6 hours instead of 30 minutes | No | No SLA configured |
| ELT container fails | No | Container exits with code 1; not surfaced |

The gap in every row is the same: no push notification. The Airflow UI shows all of these failures correctly — the missing piece is sending that information to a person without requiring them to open the UI.

---

## Key Files

| File | Alerting Role |
|---|---|
| [airflow/dags/weather_etl_dag.py](../airflow/dags/weather_etl_dag.py) | Add `email_on_failure`, `on_failure_callback`, `sla`, `sla_miss_callback` here |
| [docker-compose.yml](../docker-compose.yml) | Add `AIRFLOW__SMTP__*` env vars for email; add ELT health monitoring |
| [jobs/elt/main.py](../jobs/elt/main.py) | Add exit-hook Slack notification for ELT failures |
| [jobs/config.py](../jobs/config.py) | Add `SLACK_WEBHOOK_URL` environment variable |
