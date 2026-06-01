from fastapi import APIRouter, Depends, HTTPException
from catalog.api.db import get_db
from catalog.api.routes.ingest import get_api_key

router = APIRouter(prefix="/api/v1/metrics", tags=["Metrics"])

_MART_HIRES_BY_QUARTER = """
    SELECT
        d.department,
        j.job,
        EXTRACT(YEAR FROM he.datetime::timestamp)::int     AS hire_year,
        COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime::timestamp) = 1)::int AS q1,
        COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime::timestamp) = 2)::int AS q2,
        COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime::timestamp) = 3)::int AS q3,
        COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM he.datetime::timestamp) = 4)::int AS q4
    FROM raw.hired_employees he
    JOIN raw.departments d ON he.department_id = d.id
    JOIN raw.jobs        j ON he.job_id        = j.id
    GROUP BY d.department, j.job, EXTRACT(YEAR FROM he.datetime::timestamp)
    ORDER BY d.department, j.job, hire_year
"""

_MART_ABOVE_MEAN = """
    WITH dept_counts AS (
        SELECT
            d.id,
            d.department,
            COUNT(he.id)::int AS hired
        FROM raw.departments d
        LEFT JOIN raw.hired_employees he
            ON d.id = he.department_id
            AND EXTRACT(YEAR FROM he.datetime::timestamp) = 2021
        GROUP BY d.id, d.department
    ),
    mean_val AS (SELECT AVG(hired) AS m FROM dept_counts)
    SELECT dc.id, dc.department, dc.hired
    FROM dept_counts dc, mean_val mv
    WHERE dc.hired > mv.m
    ORDER BY dc.hired DESC
"""


@router.post("/refresh")
def refresh_analytics(x_api_key: str = Depends(get_api_key), db=Depends(get_db)):
    """Compute both analytics marts from raw tables and materialise them."""
    cursor = db.cursor()
    try:
        # ── Mart 1: hires by quarter ──────────────────────────────────────
        cursor.execute("SELECT COUNT(*) AS c FROM raw.hired_employees")
        source_rows = cursor.fetchone()["c"]
        if source_rows == 0:
            raise HTTPException(status_code=400, detail="raw.hired_employees is empty — ingest data first")

        cursor.execute("TRUNCATE TABLE analytics.mart_hires_by_quarter")
        cursor.execute(f"""
            INSERT INTO analytics.mart_hires_by_quarter (department, job, hire_year, q1, q2, q3, q4)
            {_MART_HIRES_BY_QUARTER}
        """)
        cursor.execute("SELECT COUNT(*) AS c FROM analytics.mart_hires_by_quarter")
        mart1_rows = cursor.fetchone()["c"]



        # ── Mart 2: departments above mean ────────────────────────────────
        cursor.execute("TRUNCATE TABLE analytics.mart_departments_above_mean")
        cursor.execute(f"""
            INSERT INTO analytics.mart_departments_above_mean (id, department, hired)
            {_MART_ABOVE_MEAN}
        """)
        cursor.execute("SELECT COUNT(*) AS c FROM analytics.mart_departments_above_mean")
        mart2_rows = cursor.fetchone()["c"]

        # Mean value for reference
        cursor.execute("""
            WITH dept_counts AS (
                SELECT COUNT(he.id)::int AS hired
                FROM raw.departments d
                LEFT JOIN raw.hired_employees he
                    ON d.id = he.department_id
                    AND EXTRACT(YEAR FROM he.datetime::timestamp) = 2021
                GROUP BY d.id
            )
            SELECT ROUND(AVG(hired), 2) AS mean FROM dept_counts
        """)
        mean_hires = float(cursor.fetchone()["mean"] or 0)

        db.commit()
        return {
            "status": "ok",
            "source_rows_total": int(source_rows),
            "mart_hires_by_quarter_rows": mart1_rows,
            "mart_departments_above_mean_rows": mart2_rows,
            "mean_hires_per_department_2021": mean_hires,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Analytics refresh failed: {e}")


@router.get("/hires-by-quarter")
def get_hires_by_quarter(db=Depends(get_db)):
    """Hires per job+department split by year and quarter, alphabetically sorted."""
    cursor = db.cursor()
    try:
        cursor.execute(
            "SELECT department, job, hire_year, q1, q2, q3, q4 "
            "FROM analytics.mart_hires_by_quarter "
            "ORDER BY department, job, hire_year"
        )
        return cursor.fetchall()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/departments-above-mean")
def get_departments_above_mean(db=Depends(get_db)):
    """Departments that hired above the 2021 mean, sorted descending by hires."""
    cursor = db.cursor()
    try:
        cursor.execute("SELECT id, department, hired FROM analytics.mart_departments_above_mean ORDER BY hired DESC")
        return cursor.fetchall()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
