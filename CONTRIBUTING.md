# Development Guide

This document covers how to work on the Globant Data Engineering Challenge codebase locally.

---

## Setup

```bash
# Install Python dependencies (use a virtualenv)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Start infrastructure
docker compose up -d --build
```

---

## Project layout

| Path | What lives here |
|---|---|
| `catalog/api/catalog_api.py` | FastAPI app entry point, dataset catalog endpoints |
| `catalog/api/routes/` | Feature routers: ingest, backup, restore, metrics, pipeline, dashboard, admin |
| `catalog/api/static/index.html` | Single-file web portal (vanilla HTML/CSS/JS) |
| `catalog/api/db.py` | `get_db()` dependency — psycopg2 connection |
| `ingestion/schemas/globant_schemas.py` | Pydantic models for CSV validation |
| `processing/spark_jobs/globant_analytics_job.py` | PySpark job computing the analytics marts |
| `processing/transformers/globant_transformer.py` | Medallion Bronze → Silver transformer |
| `storage/sql/globant_schema.sql` | DDL for `raw.*` and `analytics.*` schemas |
| `storage/sql/init_db.py` | Bootstrap script — runs the DDL against a live Postgres |
| `airflow/dags/` | All Airflow DAGs |
| `scripts/metabase_setup.py` | Idempotent Metabase auto-configuration |
| `dbt/` | dbt models for Silver → Gold transformation |

---

## Making API changes

FastAPI routes are baked into the Docker image at build time (`COPY . .`). After editing any Python file under `catalog/`:

```bash
docker compose up -d --build fastapi
```

The static `index.html` is served from disk — changes there take effect on hard-refresh (no rebuild needed).

---

## Environment variables

All defaults are in `.env.example`. Copy to `.env` before first run. Key variables:

| Variable | Purpose |
|---|---|
| `API_KEY` | Required header for all write endpoints |
| `POSTGRES_*` | Database connection |
| `AIRFLOW_URL / USERNAME / PASSWORD` | Used by the pipeline proxy route |
| `METABASE_URL / ADMIN_EMAIL / ADMIN_PASSWORD` | Used by the dashboard embed route |
| `BRONZE_DIR` | Path inside the fastapi container for Parquet staging |
| `BACKUP_DIR` | Path inside the fastapi container for AVRO backups |

---

## Running tests

```bash
PYTHONPATH=. pytest tests/test_globant_challenge.py -v
```

Tests cover: DB constraints, authentication, ingest routes, validation schemas, backup/restore.

---

## Adding a new API route

1. Create `catalog/api/routes/my_feature.py` with an `APIRouter`
2. Import and register it in `catalog/api/catalog_api.py`:
   ```python
   from catalog.api.routes.my_feature import router as my_router
   app.include_router(my_router)
   ```
3. Rebuild: `docker compose up -d --build fastapi`

---

## Code conventions

- No unnecessary comments — let names do the talking
- Validate at system boundaries (user input, external APIs); trust internal code
- No backwards-compatibility shims for code that no longer exists
- One short summary line in git commits; describe *why*, not *what*

---

## Common tasks

**Reset everything (clean slate)**
```bash
# Via the portal — Backup/Restore tab → Danger Zone → Reset Everything
# Or directly:
curl -X POST http://localhost:8000/api/v1/admin/reset -H "X-API-Key: globant-secret"
```

**Inspect the database**
```bash
docker compose exec postgres psql -U globant -d globant_analytics
\dt raw.*
\dt analytics.*
SELECT * FROM raw.rejected_log LIMIT 10;
```

**Re-run the Metabase setup script**
```bash
docker compose run --rm metabase-setup
```

**Run the PySpark analytics job manually**
```bash
docker compose exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.7.2 \
  /app/processing/spark_jobs/globant_analytics_job.py
```
