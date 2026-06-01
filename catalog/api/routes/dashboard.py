import os
import requests
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1", tags=["dashboard"])

MB_URL        = os.getenv("METABASE_URL",            "http://metabase:3000")
MB_PUBLIC_URL = os.getenv("METABASE_PUBLIC_URL",     MB_URL)   # browser-visible URL
MB_EMAIL      = os.getenv("METABASE_ADMIN_EMAIL",    "admin@globant.com")
MB_PASS       = os.getenv("METABASE_ADMIN_PASSWORD", "Globant2026!")
DASH_NAME     = "Globant 2021 Hiring Analytics"


def _login() -> str:
    r = requests.post(
        f"{MB_URL}/api/session",
        json={"username": MB_EMAIL, "password": MB_PASS},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["id"]


@router.get("/dashboard-embed")
def get_dashboard_embed():
    """Return the public UUID for the Metabase dashboard so the browser can embed it."""
    try:
        token = _login()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Cannot connect to Metabase: {e}")

    h = {"X-Metabase-Session": token}

    # Enable public sharing (idempotent)
    try:
        requests.put(
            f"{MB_URL}/api/setting/enable-public-sharing",
            json={"value": True}, headers=h, timeout=10,
        )
    except Exception:
        pass

    # Locate the dashboard
    r = requests.get(f"{MB_URL}/api/dashboard", headers=h, timeout=10)
    r.raise_for_status()
    dash_id = next(
        (d["id"] for d in r.json() if d.get("name") == DASH_NAME), None
    )
    if not dash_id:
        raise HTTPException(
            status_code=404,
            detail="Dashboard not found in Metabase. Run the ingestion pipeline first.",
        )

    # Get existing public UUID or create one
    r = requests.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=h, timeout=10)
    r.raise_for_status()
    uuid = r.json().get("public_uuid")

    if not uuid:
        r = requests.post(
            f"{MB_URL}/api/dashboard/{dash_id}/public_link",
            headers=h, timeout=10,
        )
        r.raise_for_status()
        uuid = r.json()["uuid"]

    return {
        "uuid":           uuid,
        "dashboard_name": DASH_NAME,
        "dashboard_id":   dash_id,
        "embed_url":      f"{MB_PUBLIC_URL}/public/dashboard/{uuid}",
        "direct_url":     f"{MB_PUBLIC_URL}/dashboard/{dash_id}",
    }
