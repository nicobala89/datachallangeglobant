import os
import glob
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException
import fastavro
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1", tags=["Restore"])

# Get DB dependency and API Key auth
from catalog.api.db import get_db
from catalog.api.routes.ingest import get_api_key

@router.post("/restore/{table_name}")
def restore_table(
    table_name: str,
    x_api_key: str = Depends(get_api_key),
    db=Depends(get_db)
):
    if table_name not in ["departments", "jobs", "hired_employees"]:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found for restore")
        
    backup_dir = os.getenv("BACKUP_DIR", "backups")
    # Find files matching table_name_*.avro
    search_pattern = os.path.join(backup_dir, f"{table_name}_*.avro")
    backup_files = glob.glob(search_pattern)
    
    if not backup_files:
        raise HTTPException(status_code=404, detail=f"No backup files found for table '{table_name}'")
        
    # Sort by modification time to find the newest backup file
    latest_backup = max(backup_files, key=os.path.getmtime)
    
    # Read AVRO file
    records = []
    try:
        with open(latest_backup, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                records.append(record)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read AVRO file {latest_backup}: {e}")
        
    if not records:
        raise HTTPException(status_code=400, detail=f"Backup file {latest_backup} is empty")
        
    cursor = db.cursor()
    try:
        # Destructive restore (truncate table with CASCADE)
        cursor.execute(f"TRUNCATE TABLE raw.{table_name} CASCADE")
        
        # Ingest records back
        columns = list(records[0].keys())
        col_names = ", ".join(columns)
        col_placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO raw.{table_name} ({col_names}) VALUES ({col_placeholders})"
        
        # Prepare records
        tuples = [tuple(r[col] for col in columns) for r in records]
        cursor.executemany(sql, tuples)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to restore table: {e}")
        
    return {
        "table": table_name,
        "restored_from": latest_backup,
        "rows_restored": len(records)
    }
