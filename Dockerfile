FROM apache/airflow:2.9.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

# Pin apache-airflow to prevent pip from upgrading it when resolving our
# extra packages, which would overwrite the image's airflow entry point.
RUN pip install --no-cache-dir "apache-airflow==2.9.2" -r /requirements.txt
