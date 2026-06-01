import os
import json
from datetime import datetime as dt_type
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Header, HTTPException, Depends
from pydantic import BaseModel, ValidationError
from psycopg2.extras import execute_values

from ingestion.schemas.globant_schemas import TABLE_SCHEMAS
from ingestion.bronze.bronze_writer import write_bronze
from catalog.api.db import get_db

router = APIRouter(prefix="/api/v1", tags=["Ingestion"])


class BatchPayload(BaseModel):
    rows: List[Dict[str, Any]]


def get_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> str:
    if not x_api_key:
        raise HTTPException(status_code=403, detail="X-API-Key header is missing")
    expected_key = os.getenv("API_KEY", "globant-secret")
    if x_api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_api_key


def _normalize(val) -> str:
    """Canonical string representation for field comparison."""
    if val is None:
        return ""
    if isinstance(val, dt_type):
        return val.replace(tzinfo=None).isoformat(timespec="seconds")
    if isinstance(val, str):
        try:
            parsed = dt_type.fromisoformat(val.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=None).isoformat(timespec="seconds")
        except ValueError:
            pass
    return str(val)


def _diff(db_row: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """Return {field: {current, incoming}} for every field that changed."""
    return {
        k: {"current": db_row.get(k), "incoming": incoming[k]}
        for k in incoming
        if _normalize(db_row.get(k)) != _normalize(incoming[k])
    }


def _log_rejected(cursor, table_name: str, raw_row: Dict[str, Any], reason: str):
    cursor.execute(
        "INSERT INTO raw.rejected_log (table_name, raw_row, reason) VALUES (%s, %s, %s)",
        (table_name, json.dumps(raw_row, default=str), reason),
    )


@router.post("/ingest/{table_name}")
def ingest_batch(
    table_name: str,
    payload: BatchPayload,
    x_api_key: str = Depends(get_api_key),
    db=Depends(get_db),
):
    if len(payload.rows) > 1000:
        raise HTTPException(status_code=400, detail="Batch size exceeds 1000 rows limit")

    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    cursor = db.cursor()

    # ── Pre-fetch FK sets for hired_employees ─────────────────────────────────
    valid_dept_ids: set = set()
    valid_job_ids: set = set()
    if table_name == "hired_employees":
        try:
            cursor.execute("SELECT id FROM raw.departments")
            valid_dept_ids = {row["id"] for row in cursor.fetchall()}
            cursor.execute("SELECT id FROM raw.jobs")
            valid_job_ids = {row["id"] for row in cursor.fetchall()}
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Error querying reference tables: {e}")

    # ── Step 1: Pydantic + FK validation ──────────────────────────────────────
    valid_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []

    for row in payload.rows:
        try:
            validated = schema(**row)
            validated_dict = validated.model_dump()
        except ValidationError as e:
            reason = "; ".join(f"{err['loc'][0]}: {err['msg']}" for err in e.errors())
            rejected_rows.append({"row": row, "reason": reason})
            _log_rejected(cursor, table_name, row, reason)
            continue
        except Exception as e:
            rejected_rows.append({"row": row, "reason": str(e)})
            _log_rejected(cursor, table_name, row, str(e))
            continue

        if table_name == "hired_employees":
            fk_errors = []
            if validated_dict.get("department_id") not in valid_dept_ids:
                fk_errors.append(f"department_id {validated_dict['department_id']} not in raw.departments")
            if validated_dict.get("job_id") not in valid_job_ids:
                fk_errors.append(f"job_id {validated_dict['job_id']} not in raw.jobs")
            if fk_errors:
                reason = "FK Violation: " + "; ".join(fk_errors)
                rejected_rows.append({"row": row, "reason": reason})
                _log_rejected(cursor, table_name, row, reason)
                continue

        valid_rows.append(validated_dict)

    # ── Step 2: Idempotency check — compare against existing Silver rows ───────
    new_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    conflict_rows: List[Dict[str, Any]] = []

    if valid_rows:
        incoming_ids = [r["id"] for r in valid_rows]
        try:
            cursor.execute(
                f"SELECT * FROM raw.{table_name} WHERE id = ANY(%s)",
                (incoming_ids,),
            )
            existing = {row["id"]: dict(row) for row in cursor.fetchall()}
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Idempotency check failed: {e}")

        for row in valid_rows:
            row_id = row["id"]
            if row_id not in existing:
                new_rows.append(row)
            else:
                db_row = {k: v for k, v in existing[row_id].items() if not k.startswith("_")}
                changes = _diff(db_row, row)
                if not changes:
                    skipped_rows.append(row)
                else:
                    conflict_rows.append({
                        "id": row_id,
                        "changed_fields": list(changes.keys()),
                        "diff": changes,
                    })
                    _log_rejected(
                        cursor,
                        table_name,
                        row,
                        f"CONFLICT: id={row_id} exists with different data — "
                        + ", ".join(f"{k}: {v['current']!r} → {v['incoming']!r}" for k, v in changes.items()),
                    )

    # ── Step 3: Bronze — land all validated rows as immutable audit trail ─────
    if new_rows:
        try:
            write_bronze(table_name, new_rows)
        except Exception as e:
            pass  # Bronze failure is non-fatal; Silver is the system of record

    # ── Step 4: Silver — insert only genuinely new rows ───────────────────────
    if new_rows:
        try:
            columns = list(new_rows[0].keys())
            execute_values(
                cursor,
                f"INSERT INTO raw.{table_name} ({', '.join(columns)}) VALUES %s",
                [tuple(r[c] for c in columns) for r in new_rows],
            )
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Database write error: {e}")

    db.commit()

    return {
        "table": table_name,
        "inserted": len(new_rows),
        "skipped": len(skipped_rows),
        "conflicts": len(conflict_rows),
        "rejected": len(rejected_rows),
        "conflict_details": conflict_rows,
        "errors": rejected_rows,
    }


@router.get("/status")
def get_tables_status(db=Depends(get_db)):
    cursor = db.cursor()
    status = {}
    for table in ["departments", "jobs", "hired_employees"]:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM raw.{table}")
            status[table] = cursor.fetchone()["count"]
        except Exception:
            db.rollback()
            status[table] = 0

    try:
        cursor.execute("SELECT COUNT(*) as count FROM raw.rejected_log")
        status["rejected_log"] = cursor.fetchone()["count"]
    except Exception:
        db.rollback()
        status["rejected_log"] = 0

    return status
