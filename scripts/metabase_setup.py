"""
Metabase Auto-Setup  (idempotent)
----------------------------------
Runs once after Metabase starts. Safe to re-run — every step checks whether
the object already exists before creating it, so no duplicates on restart.

Configures:
  1. Admin account (first-time only)
  2. PostgreSQL analytics database connection
  3. Five native SQL questions (business metrics + supporting charts)
  4. A dashboard that pins all five questions with proper layout

Environment variables (same as .env):
  METABASE_URL, METABASE_ADMIN_EMAIL, METABASE_ADMIN_PASSWORD
  POSTGRES_HOST / PORT / DB / USER / PASSWORD
"""

import os, sys, time, requests

MB_URL   = os.getenv("METABASE_URL",           "http://metabase:3000")
MB_EMAIL = os.getenv("METABASE_ADMIN_EMAIL",   "admin@globant.com")
MB_PASS  = os.getenv("METABASE_ADMIN_PASSWORD","globant-admin")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB   = os.getenv("POSTGRES_DB",   "globant_analytics")
PG_USER = os.getenv("POSTGRES_USER", "globant")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "globant_password")

DB_NAME   = "Globant Analytics DB"
DASH_NAME = "Globant 2021 Hiring Analytics"


# ── Helpers ────────────────────────────────────────────────────────────────────

def wait_for_metabase(timeout: int = 300):
    print("Waiting for Metabase…")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{MB_URL}/api/health", timeout=5)
            if r.ok and r.json().get("status") == "ok":
                print("Metabase is ready.")
                time.sleep(3)   # brief extra wait for internal init
                return
        except Exception:
            pass
        time.sleep(8)
    raise TimeoutError("Metabase did not become ready in time.")


def get_setup_token() -> str:
    r = requests.get(f"{MB_URL}/api/session/properties")
    r.raise_for_status()
    token = r.json().get("setup-token")
    if not token:
        raise RuntimeError("No setup-token — already configured.")
    return token


def first_time_setup() -> str:
    """Complete first-time wizard. Returns session token."""
    setup_token = get_setup_token()
    # v0.50+ requires site_name inside user AND site_locale in prefs
    payload = {
        "token": setup_token,
        "user": {
            "email": MB_EMAIL, "password": MB_PASS,
            "first_name": "Admin", "last_name": "Globant",
            "site_name": "Globant Analytics",
        },
        "prefs": {
            "site_name": "Globant Analytics",
            "site_locale": "en",
            "allow_tracking": False,
        },
        "database": None,
    }
    r = requests.post(f"{MB_URL}/api/setup", json=payload)
    r.raise_for_status()
    return r.json()["id"]


def login() -> str:
    r = requests.post(f"{MB_URL}/api/session",
                      json={"username": MB_EMAIL, "password": MB_PASS})
    r.raise_for_status()
    return r.json()["id"]


# ── Idempotent resource helpers ────────────────────────────────────────────────

def get_or_create_database(token: str) -> int:
    h = {"X-Metabase-Session": token}
    existing = requests.get(f"{MB_URL}/api/database", headers=h)
    existing.raise_for_status()
    for db in existing.json().get("data", existing.json() if isinstance(existing.json(), list) else []):
        if db.get("name") == DB_NAME:
            print(f"  DB already connected — id={db['id']}")
            return db["id"]

    payload = {
        "engine": "postgres",
        "name": DB_NAME,
        "details": {
            "host": PG_HOST, "port": PG_PORT, "dbname": PG_DB,
            "user": PG_USER, "password": PG_PASS,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": "analytics",
        },
        "auto_run_queries": True,
        "is_full_sync": True,
    }
    r = requests.post(f"{MB_URL}/api/database", json=payload, headers=h)
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  DB added — id={db_id}")
    # Wait for Metabase to sync the schema
    time.sleep(5)
    return db_id


def get_or_create_question(token: str, db_id: int, name: str, sql: str,
                            display: str, viz_settings: dict = None) -> int:
    h = {"X-Metabase-Session": token}
    cards = requests.get(f"{MB_URL}/api/card", headers=h)
    cards.raise_for_status()
    for card in cards.json():
        if card.get("name") == name:
            print(f"  Question already exists: '{name}' — id={card['id']}")
            return card["id"]

    payload = {
        "name": name,
        "display": display,
        "dataset_query": {
            "type": "native",
            "database": db_id,
            "native": {"query": sql},
        },
        "visualization_settings": viz_settings or {},
    }
    r = requests.post(f"{MB_URL}/api/card", json=payload, headers=h)
    r.raise_for_status()
    card_id = r.json()["id"]
    print(f"  Question created: '{name}' — id={card_id}")
    return card_id


def get_or_create_dashboard(token: str) -> tuple:
    """Returns (dash_id, is_new)."""
    h = {"X-Metabase-Session": token}
    dashes = requests.get(f"{MB_URL}/api/dashboard", headers=h)
    dashes.raise_for_status()
    for d in dashes.json():
        if d.get("name") == DASH_NAME:
            print(f"  Dashboard already exists — id={d['id']}")
            return d["id"], False

    r = requests.post(f"{MB_URL}/api/dashboard", json={"name": DASH_NAME}, headers=h)
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"  Dashboard created — id={dash_id}")
    return dash_id, True


def add_cards_to_dashboard(token: str, dash_id: int, cards: list):
    """
    cards: list of dicts with keys: card_id, row, col, size_x, size_y
    Uses PUT /api/dashboard/:id to set all cards at once (works across MB versions).
    """
    h = {"X-Metabase-Session": token}

    # Fetch current state
    current = requests.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=h)
    current.raise_for_status()
    existing_cards = current.json().get("dashcards", [])
    if existing_cards:
        print(f"  Dashboard already has {len(existing_cards)} cards — skipping card layout")
        return

    dashcards = [
        {
            "id": -(i + 1),           # negative ids = new cards
            "card_id": c["card_id"],
            "row": c["row"], "col": c["col"],
            "size_x": c["size_x"], "size_y": c["size_y"],
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for i, c in enumerate(cards)
    ]

    r = requests.put(
        f"{MB_URL}/api/dashboard/{dash_id}",
        json={"dashcards": dashcards},
        headers=h,
    )
    r.raise_for_status()
    print(f"  {len(dashcards)} cards added to dashboard")


# ── SQL questions ──────────────────────────────────────────────────────────────

Q_HIRES_BY_QUARTER = """
SELECT
    department                AS "Department",
    job                       AS "Job",
    hire_year                 AS "Year",
    q1                        AS "Q1",
    q2                        AS "Q2",
    q3                        AS "Q3",
    q4                        AS "Q4",
    q1 + q2 + q3 + q4        AS "Total"
FROM analytics.mart_hires_by_quarter
ORDER BY department, job, hire_year
"""

Q_ABOVE_MEAN = """
SELECT
    department   AS "Department",
    hired        AS "Employees Hired"
FROM analytics.mart_departments_above_mean
ORDER BY hired DESC
"""

Q_HIRES_BY_YEAR = """
SELECT
    hire_year                     AS "Year",
    SUM(q1 + q2 + q3 + q4)::int  AS "Total Hires"
FROM analytics.mart_hires_by_quarter
GROUP BY hire_year
ORDER BY hire_year
"""

Q_QUARTERLY_TREND = """
SELECT
    hire_year || '-Q' || q_num   AS "Period",
    SUM(hires)::int              AS "Hires"
FROM (
    SELECT hire_year, 1 AS q_num, SUM(q1) AS hires FROM analytics.mart_hires_by_quarter GROUP BY hire_year
    UNION ALL
    SELECT hire_year, 2,          SUM(q2)           FROM analytics.mart_hires_by_quarter GROUP BY hire_year
    UNION ALL
    SELECT hire_year, 3,          SUM(q3)           FROM analytics.mart_hires_by_quarter GROUP BY hire_year
    UNION ALL
    SELECT hire_year, 4,          SUM(q4)           FROM analytics.mart_hires_by_quarter GROUP BY hire_year
) t
GROUP BY hire_year, q_num
ORDER BY hire_year, q_num
"""

Q_TOP_DEPARTMENTS = """
SELECT
    department            AS "Department",
    SUM(q1+q2+q3+q4)::int AS "Total Hires"
FROM analytics.mart_hires_by_quarter
GROUP BY department
ORDER BY "Total Hires" DESC
LIMIT 15
"""

VIZ_BAR_DEPT = {
    "graph.dimensions": ["Department"],
    "graph.metrics":    ["Employees Hired"],
    "graph.x_axis.title_text": "Department",
    "graph.y_axis.title_text": "Employees Hired in 2021",
}

VIZ_BAR_YEAR = {
    "graph.dimensions": ["Year"],
    "graph.metrics":    ["Total Hires"],
    "graph.x_axis.title_text": "Year",
    "graph.y_axis.title_text": "Total Hires",
}

VIZ_LINE_TREND = {
    "graph.dimensions": ["Period"],
    "graph.metrics":    ["Hires"],
    "graph.x_axis.title_text": "Quarter",
    "graph.y_axis.title_text": "Hires",
}

VIZ_BAR_TOP_DEPT = {
    "graph.dimensions": ["Department"],
    "graph.metrics":    ["Total Hires"],
    "graph.x_axis.title_text": "Department",
    "graph.y_axis.title_text": "Total Hires (all years)",
}


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    wait_for_metabase()

    # Step 1 — authenticate
    # Try login first (handles already-configured restarts).
    # Fall back to first-time setup if login fails (fresh install).
    try:
        session = login()
        print("Logged in to existing Metabase instance.")
    except Exception:
        try:
            print("Login failed — running first-time setup…")
            session = first_time_setup()
            print("First-time setup complete.")
        except Exception as e:
            raise RuntimeError(f"Cannot authenticate with Metabase: {e}")

    # Step 2 — database
    print("\n[1/3] Database connection")
    db_id = get_or_create_database(session)

    # Step 3 — questions
    print("\n[2/3] Questions")
    q1 = get_or_create_question(session, db_id,
        "Hires by Department, Job, Year & Quarter",
        Q_HIRES_BY_QUARTER, "table")

    q2 = get_or_create_question(session, db_id,
        "Departments Above Mean Hires (2021)",
        Q_ABOVE_MEAN, "bar", VIZ_BAR_DEPT)

    q3 = get_or_create_question(session, db_id,
        "Total Hires by Year",
        Q_HIRES_BY_YEAR, "bar", VIZ_BAR_YEAR)

    q4 = get_or_create_question(session, db_id,
        "Quarterly Hiring Trend",
        Q_QUARTERLY_TREND, "line", VIZ_LINE_TREND)

    q5 = get_or_create_question(session, db_id,
        "Top Departments by Total Hires",
        Q_TOP_DEPARTMENTS, "bar", VIZ_BAR_TOP_DEPT)

    # Step 4 — dashboard
    print("\n[3/3] Dashboard")
    dash_id, is_new = get_or_create_dashboard(session)

    if is_new:
        # Grid is 24 columns wide in Metabase
        # Row 0: two KPI bars side by side (year totals + above-mean)
        # Row 8: quarterly trend (full width)
        # Row 16: detail table (full width)
        # Row 26: top departments
        add_cards_to_dashboard(session, dash_id, [
            {"card_id": q3, "row":  0, "col":  0, "size_x": 12, "size_y": 8},   # hires by year
            {"card_id": q2, "row":  0, "col": 12, "size_x": 12, "size_y": 8},   # above mean bar
            {"card_id": q4, "row":  8, "col":  0, "size_x": 24, "size_y": 8},   # quarterly trend
            {"card_id": q1, "row": 16, "col":  0, "size_x": 24, "size_y": 10},  # detail table
            {"card_id": q5, "row": 26, "col":  0, "size_x": 24, "size_y": 8},   # top departments
        ])

    # Enable public sharing so the dashboard can be embedded via iframe
    h = {"X-Metabase-Session": session}
    try:
        requests.put(f"{MB_URL}/api/setting/enable-public-sharing",
                     json={"value": True}, headers=h, timeout=10)
        print("  Public sharing enabled")
    except Exception as e:
        print(f"  Warning: could not enable public sharing: {e}")

    # Create (or fetch) a public link for the dashboard
    r = requests.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=h, timeout=10)
    r.raise_for_status()
    pub_uuid = r.json().get("public_uuid")
    if not pub_uuid:
        r = requests.post(f"{MB_URL}/api/dashboard/{dash_id}/public_link",
                          headers=h, timeout=10)
        r.raise_for_status()
        pub_uuid = r.json()["uuid"]
        print(f"  Public link created — uuid={pub_uuid}")
    else:
        print(f"  Public link already exists — uuid={pub_uuid}")

    print(f"\n✓ Metabase ready")
    print(f"  URL:       {MB_URL}")
    print(f"  Login:     {MB_EMAIL}")
    print(f"  Dashboard: {MB_URL}/dashboard/{dash_id}")
    print(f"  Embed URL: {MB_URL}/public/dashboard/{pub_uuid}")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"Setup failed: {e}", file=sys.stderr)
        sys.exit(1)
