import os
from datetime import date, datetime
from fastapi import APIRouter, Depends, HTTPException
import fastavro
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1", tags=["Backup"])

# Get DB dependency and API Key auth
from catalog.api.db import get_db
from catalog.api.routes.ingest import get_api_key

AVRO_SCHEMAS = {
    "departments": {
        "type": "record",
        "name": "Department",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "department", "type": "string"},
            {"name": "_loaded_at", "type": ["null", "string"], "default": None}
        ]
    },
    "jobs": {
        "type": "record",
        "name": "Job",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "job", "type": "string"},
            {"name": "_loaded_at", "type": ["null", "string"], "default": None}
        ]
    },
    "hired_employees": {
        "type": "record",
        "name": "HiredEmployee",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "datetime", "type": "string"},
            {"name": "department_id", "type": "int"},
            {"name": "job_id", "type": "int"},
            {"name": "_loaded_at", "type": ["null", "string"], "default": None}
        ]
    }
}

@router.post("/backup/{table_name}")
def backup_table(
    table_name: str,
    x_api_key: str = Depends(get_api_key),
    db=Depends(get_db)
):
    schema = AVRO_SCHEMAS.get(table_name)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found for backup")
        
    cursor = db.cursor()
    try:
        cursor.execute(f"SELECT * FROM raw.{table_name}")
        rows = cursor.fetchall()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")

    # Prepare rows for AVRO serialization
    formatted_rows = []
    for row in rows:
        formatted_row = {}
        for field in schema["fields"]:
            name = field["name"]
            val = row.get(name)
            
            # Convert datetime objects to string representation
            if isinstance(val, (datetime, date)):
                val = val.isoformat()
            
            formatted_row[name] = val
        formatted_rows.append(formatted_row)

    # Ensure backups directory exists
    backup_dir = os.getenv("BACKUP_DIR", "backups")
    os.makedirs(backup_dir, exist_ok=True)
    
    # Generate filename
    filename = f"{table_name}_{date.today().isoformat()}.avro"
    filepath = os.path.join(backup_dir, filename)

    try:
        # Write to AVRO file
        parsed_schema = fastavro.parse_schema(schema)
        with open(filepath, "wb") as out:
            fastavro.writer(out, parsed_schema, formatted_rows)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write AVRO file: {e}")

    return {
        "table": table_name,
        "backup_file": filepath,
        "rows_backed_up": len(formatted_rows)
    }
