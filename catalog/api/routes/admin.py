import os
import shutil
import requests
from pathlib import Path
from fastapi import APIRouter, HTTPException, Header
from requests.auth import HTTPBasicAuth
import psycopg2

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])

def _af_creds():
    url  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080").rstrip("/")
    user = os.getenv("AIRFLOW_USERNAME", os.getenv("AIRFLOW_ADMIN_USERNAME", "admin"))
    pwd  = os.getenv("AIRFLOW_PASSWORD", os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
    return url, HTTPBasicAuth(user, pwd)

_TRUNCATE_TABLES = [
    "raw.hired_employees",
    "raw.departments",
    "raw.jobs",
    "raw.rejected_log",
    "analytics.mart_hires_by_quarter",
    "analytics.mart_departments_above_mean",
]

_ALL_DAGS = [
    "globant_medallion_pipeline",
    "globant_csv_ingestion_dag",
    "globant_backup_dag",
    "globant_restore_dag",
]

_BRONZE_DIR = Path(os.getenv("BRONZE_DIR", "/app/data/bronze"))


def _require_key(x_api_key: str):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


def _truncate_postgres() -> dict:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "globant_analytics"),
        user=os.getenv("POSTGRES_USER", "globant"),
        password=os.getenv("POSTGRES_PASSWORD", "globant_password"),
    )
    conn.autocommit = True
    cur = conn.cursor()
    truncated = []
    for table in _TRUNCATE_TABLES:
        try:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            truncated.append(table)
        except Exception as e:
            truncated.append(f"{table} (error: {e})")
    cur.close()
    conn.close()
    return {"truncated": truncated}


def _clear_bronze() -> dict:
    deleted = []
    if _BRONZE_DIR.exists():
        for entity in _BRONZE_DIR.iterdir():
            try:
                if entity.is_dir():
                    shutil.rmtree(entity)
                else:
                    entity.unlink()
                deleted.append(str(entity.name))
            except Exception as e:
                deleted.append(f"{entity.name} (error: {e})")
    return {"bronze_cleared": deleted}


def _clear_airflow_runs() -> dict:
    af_url, auth = _af_creds()
    headers = {"Content-Type": "application/json"}
    deleted_total = 0
    errors = []

    for dag_id in _ALL_DAGS:
        try:
            r = requests.get(
                f"{af_url}/api/v1/dags/{dag_id}/dagRuns?limit=200",
                auth=auth, headers=headers, timeout=10,
            )
            if r.status_code == 404:
                continue
            r.raise_for_status()
            runs = r.json().get("dag_runs", [])
            for run in runs:
                run_id = run["dag_run_id"]
                d = requests.delete(
                    f"{af_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
                    auth=auth, headers=headers, timeout=10,
                )
                if d.ok:
                    deleted_total += 1
        except Exception as e:
            errors.append(f"{dag_id}: {e}")

    return {"airflow_runs_deleted": deleted_total, "errors": errors}


@router.post("/reset")
def full_reset(x_api_key: str = Header(...)):
    _require_key(x_api_key)

    pg  = _truncate_postgres()
    bz  = _clear_bronze()
    af  = _clear_airflow_runs()

    return {
        "status": "reset_complete",
        "postgres": pg,
        "bronze": bz,
        "airflow": af,
    }
