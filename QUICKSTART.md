# Quick Start

## Prerequisites

- Docker and Docker Compose
- At least 8 GB of RAM available for the containers
- Ports 3000, 5432, 7077, 8000, 8080, 8081 free

---

## 1. Configure environment

```bash
cp .env.example .env
```

The defaults work out of the box. Only change them if you need non-standard credentials:

| Variable | Default |
|---|---|
| `API_KEY` | `globant-secret` |
| `POSTGRES_USER` | `globant` |
| `POSTGRES_PASSWORD` | `globant_password` |
| `POSTGRES_DB` | `globant_analytics` |
| `METABASE_ADMIN_EMAIL` | `admin@globant.com` |
| `METABASE_ADMIN_PASSWORD` | `globant-admin` |

---

## 2. Start all services

```bash
docker compose up -d --build
```

First boot takes 3–5 minutes. Metabase and the Metabase setup script need extra time to initialize.

Check status:

```bash
docker compose ps
docker compose logs -f fastapi       # API server
docker compose logs -f metabase-setup  # auto-dashboard setup
```

---

## 3. Access the services

| Service | URL | Credentials |
|---|---|---|
| Web portal | <http://localhost:8000> | API key: `globant-secret` |
| Swagger docs | <http://localhost:8000/docs> | — |
| Airflow | <http://localhost:8080> | admin / admin |
| Metabase | <http://localhost:3000> | admin@globant.com / globant-admin |
| Spark UI | <http://localhost:8081> | — |

---

## 4. Upload data

1. Open <http://localhost:8000>
2. Enter your API key (`globant-secret`) and click **Save Key**
3. Go to the **Ingest** tab
4. Drag and drop your CSV files for `departments`, `jobs`, and `hired_employees`
5. The portal validates, chunks, and uploads — rejected rows are reported inline

CSV format expected:

**departments.csv** — `id,department`
**jobs.csv** — `id,job`
**hired_employees.csv** — `id,name,datetime,department_id,job_id`

---

## 5. Run the pipeline

Go to the **Pipeline** tab in the portal and trigger `globant_csv_ingestion_dag` or `globant_medallion_pipeline`.

Or trigger directly from Airflow at <http://localhost:8080>.

---

## 6. View analytics

The **Dashboard** tab embeds the Metabase public dashboard automatically once the pipeline has run and the analytics marts are populated.

---

## Common commands

```bash
# Stop everything (keeps volumes)
docker compose down

# Full reset — destroy volumes and start clean
docker compose down -v && docker compose up -d --build

# Rebuild only the API after code changes
docker compose up -d --build fastapi

# Open a psql shell
docker compose exec postgres psql -U globant -d globant_analytics

# Tail Airflow logs
docker compose logs -f airflow-scheduler

# Run tests (outside Docker)
PYTHONPATH=. pytest tests/test_globant_challenge.py -v
```

---

## Troubleshooting

**Portal returns 401** — API key header missing. Enter `globant-secret` in the portal key field and click Save.

**Airflow DAG not found** — DAGs mount via a volume. Check `docker compose ps airflow-webserver` and `docker compose logs airflow-scheduler`.

**Metabase dashboard blank** — Metabase may still be starting. Wait ~3 minutes and refresh. Check `docker compose logs metabase-setup` for setup progress.

**Port already in use** — Stop conflicting services or change the port mappings in `docker-compose.yml`.

**Postgres auth error after credential change** — The postgres volume stores the old credentials. Run `docker compose down -v` to recreate it.
