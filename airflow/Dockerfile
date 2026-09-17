FROM apache/airflow:2.9.3
 
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
       libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
 
USER airflow

# Airflow container needs Python libraries that are generally not in the base Airflow image like:
# boto3
# snowflake-connector-python

RUN pip install --no-cache-dir \
    boto3 \
    snowflake-connector-python