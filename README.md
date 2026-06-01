# Globant Data Engineering Challenge

A complete end-to-end data platform built for the Globant Data Engineering Challenge. It covers CSV ingestion, bronze/silver/gold medallion architecture, PySpark analytics, AVRO backup/restore, Airflow orchestration, and a Metabase analytics dashboard — all wired together through a FastAPI control plane with a minimalist web UI.

**Author**: [Nicolás Balaguera](https://nicobalaguera.com) · [LinkedIn](https://www.linkedin.com/in/nicolas-alejandro-balaguera-gonzalez/) · [GitHub](https://github.com/nicobala89/GlobantDataChallenge)

---

## Architecture

```text
CSV Upload (Portal / API)
        │
        ▼
  Bronze Layer ─── Parquet files on disk (/app/data/bronze/)
        │               orchestrated by Airflow
        ▼
  Silver Layer ─── PostgreSQL raw.* tables
  (raw.departments, raw.jobs, raw.hired_employees, raw.rejected_log)
        │               dbt + PySpark transforms
        ▼
  Gold Layer ──── PostgreSQL analytics.* tables
  (analytics.mart_hires_by_quarter, analytics.mart_departments_above_mean)
        │
        ▼
  Metabase Dashboard  ←─────  FastAPI /api/v1/metrics
```

---

## Services

| Service | Port | Credentials |
|---|---|---|
| **FastAPI + Web Portal** | 8000 | API key: `globant-secret` |
| **Apache Airflow** | 8080 | admin / admin |
| **Metabase** | 3000 | admin@globant.com / globant-admin |
| **PostgreSQL** | 5432 | globant / globant_password |
| **Spark Master UI** | 8081 | — |

---

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env          # edit API keys / passwords as needed

# 2. Spin everything up
docker compose up -d --build

# 3. Open the portal
open http://localhost:8000
```

Metabase auto-configures on first boot via `scripts/metabase_setup.py` (runs as a one-shot init container).

---

## API Endpoints

### Ingestion
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/ingest/{table}` | Batch ingest 1–1000 records (departments / jobs / hired_employees) |
| `GET` | `/api/v1/status` | Record counts for all raw tables + rejection log |

### Backup / Restore
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/backup/{table}` | Serialize table to AVRO |
| `POST` | `/api/v1/restore/{table}` | Truncate table and reload from latest AVRO |

### Analytics
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/metrics/hires-by-quarter` | Hires by dept/job/quarter (2021) |
| `GET` | `/api/v1/metrics/departments-above-mean` | Departments above mean hires (2021) |

### Pipeline & Dashboard
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/pipeline/dags` | List DAG status |
| `POST` | `/api/v1/pipeline/dags/{dag_id}/trigger` | Trigger a DAG run |
| `GET` | `/api/v1/pipeline/dags/{dag_id}/runs` | Recent run history |
| `GET` | `/api/v1/dashboard-embed` | Metabase public dashboard UUID |

### Admin
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/reset` | Truncate all tables + clear bronze + delete Airflow run history |

---

## Airflow DAGs

| DAG ID | Purpose |
|---|---|
| `globant_csv_ingestion_dag` | Watches `/app/data/bronze/` and ingests CSVs into raw.* tables |
| `globant_medallion_pipeline` | Full Bronze → Silver → Gold pipeline |
| `globant_backup_dag` | AVRO backup of all raw tables |
| `globant_restore_dag` | AVRO restore of all raw tables |

---

## Data Validation

Records are validated on ingest via Pydantic schemas (`ingestion/schemas/globant_schemas.py`):

- **departments**: `id` (int), `department` (str, non-empty)
- **jobs**: `id` (int), `job` (str, non-empty)
- **hired_employees**: `id` (int), `name` (str), `datetime` (ISO-8601), `department_id` (int), `job_id` (int)

Invalid rows are rejected and written to `raw.rejected_log` with the original row and reason — they never touch the main tables.

---

## Analytics Queries

**Hires by quarter (2021)**
```sql
SELECT department, job, hire_year, q1, q2, q3, q4
FROM analytics.mart_hires_by_quarter
ORDER BY department, job;
```

**Departments above mean hires (2021)**
```sql
SELECT department, hired
FROM analytics.mart_departments_above_mean
ORDER BY hired DESC;
```

---

## Project Structure

```text
GlobantDataChallange/
├── airflow/
│   ├── dags/                       # All Airflow DAGs
│   └── operators/ utils/           # Custom operators & pipeline factory
├── catalog/
│   └── api/
│       ├── catalog_api.py          # FastAPI app + dataset catalog endpoints
│       ├── db.py                   # DB connection helper
│       ├── routes/                 # ingest, backup, restore, metrics,
│       │                           #   pipeline, dashboard, admin
│       └── static/index.html       # Web portal UI
├── ingestion/
│   └── schemas/globant_schemas.py  # Pydantic validation models
├── processing/
│   ├── spark_jobs/                 # PySpark analytics job
│   └── transformers/               # Medallion transformers
├── storage/
│   └── sql/
│       ├── globant_schema.sql      # DDL for raw.* and analytics.*
│       └── init_db.py              # Schema bootstrap script
├── scripts/
│   └── metabase_setup.py           # Metabase auto-configuration
├── dbt/                            # dbt models (silver → gold)
├── docker-compose.yml
├── .env.example
└── requirements.txt
```

---

## Running Tests

```bash
PYTHONPATH=. pytest tests/test_globant_challenge.py -v
```

---

## License

MIT — see [LICENSE](LICENSE).
