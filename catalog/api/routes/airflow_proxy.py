import os
import requests
from fastapi import APIRouter, HTTPException, Depends
from requests.auth import HTTPBasicAuth
from catalog.api.routes.ingest import get_api_key

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"])


def _af(method: str, path: str, **kwargs):
    # Read env vars per-request so Railway's late injection is always picked up.
    # rstrip('/') prevents double-slash when AIRFLOW_URL has a trailing slash.
    url  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080").rstrip("/")
    user = os.getenv("AIRFLOW_USERNAME", os.getenv("AIRFLOW_ADMIN_USERNAME", "admin"))
    pwd  = os.getenv("AIRFLOW_PASSWORD", os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
    r = requests.request(
        method,
        f"{url}/api/v1{path}",
        auth=HTTPBasicAuth(user, pwd),
        headers={"Content-Type": "application/json"},
        timeout=10,
        **kwargs,
    )
    r.raise_for_status()
    return r.json()


@router.get("/dags")
def list_dags():
    """Return the subset of DAGs we expose in the UI."""
    try:
        data = _af("GET", "/dags?limit=50")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Cannot reach Airflow: {e}")
    exposed = {
        "globant_medallion_pipeline",
        "globant_csv_ingestion_dag",
    }
    dags = [d for d in data.get("dags", []) if d["dag_id"] in exposed]
    return {"dags": dags}


@router.post("/dags/{dag_id}/trigger")
def trigger_dag(dag_id: str, _key: str = Depends(get_api_key)):
    """Unpause (if needed) and trigger a DAG run."""
    try:
        # Unpause first so the run actually executes
        _af("PATCH", f"/dags/{dag_id}", json={"is_paused": False})
        run = _af("POST", f"/dags/{dag_id}/dagRuns", json={"conf": {}})
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code,
                            detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"dag_run_id": run["dag_run_id"], "state": run["state"],
            "logical_date": run.get("logical_date")}


@router.get("/dags/{dag_id}/runs")
def get_runs(dag_id: str, limit: int = 5):
    """Return the most recent N runs for a DAG."""
    try:
        data = _af("GET", f"/dags/{dag_id}/dagRuns?limit={limit}&order_by=-start_date")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return {"dag_runs": []}
        raise HTTPException(status_code=e.response.status_code,
                            detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"dag_runs": data.get("dag_runs", [])}
