#!/bin/bash
set -e

# Migrate / initialise the metadata DB
airflow db migrate

# Create admin user — reads the same AIRFLOW_ADMIN_* vars used everywhere else.
# || true makes it idempotent: no error if the user already exists.
airflow users create \
    --username  "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --password  "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin \
    --lastname  User \
    --role      Admin \
    --email     "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" 2>/dev/null || true

# Run scheduler in background; webserver in foreground (keeps container alive).
airflow scheduler &

exec airflow webserver --port "${PORT:-8080}"
