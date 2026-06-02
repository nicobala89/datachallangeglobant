#!/bin/bash
set -e

# Railway names these POSTGRES_HOST_PRIVATE / POSTGRES_PORT_PRIVATE.
# Normalize to the standard names so DAG tasks and dbt pick them up correctly.
export POSTGRES_HOST="${POSTGRES_HOST:-${POSTGRES_HOST_PRIVATE:-localhost}}"
export POSTGRES_PORT="${POSTGRES_PORT:-${POSTGRES_PORT_PRIVATE:-5432}}"

airflow db migrate

# Delete then recreate so every redeploy resets to the env-var password.
# If the user doesn't exist yet, delete returns a non-zero exit that we swallow.
airflow users delete --username "${AIRFLOW_ADMIN_USERNAME:-admin}" 2>/dev/null || true
airflow users create \
    --username  "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --password  "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin \
    --lastname  User \
    --role      Admin \
    --email     "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"

airflow scheduler &

exec airflow webserver --port "${PORT:-8080}"
