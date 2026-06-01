import os
import sys
import glob
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is in path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastavro
import psycopg2

default_args = {
    'owner': 'nicobalaguera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def restore_task(table_name: str):
    backup_dir = os.getenv("BACKUP_DIR", "backups")
    search_pattern = os.path.join(backup_dir, f"{table_name}_*.avro")
    backup_files = glob.glob(search_pattern)
    
    if not backup_files:
        raise FileNotFoundError(f"No backup files found for table '{table_name}' in {backup_dir}")
        
    latest_backup = max(backup_files, key=os.path.getmtime)
    print(f"Restoring table '{table_name}' from latest backup: {latest_backup}")
    
    # Read AVRO
    records = []
    with open(latest_backup, "rb") as f:
        reader = fastavro.reader(f)
        for record in reader:
            records.append(record)
            
    if not records:
        print(f"Warning: Backup file {latest_backup} contains 0 records. Skipping restore.")
        return

    user = os.getenv("POSTGRES_USER", "globant")
    password = os.getenv("POSTGRES_PASSWORD", "globant_password")
    db_name = os.getenv("POSTGRES_DB", "globant_analytics")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")

    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = conn.cursor()
    
    try:
        # Destructive restore (truncate table with CASCADE)
        print(f"Truncating raw.{table_name} with CASCADE...")
        cursor.execute(f"TRUNCATE TABLE raw.{table_name} CASCADE")
        
        # Prepare bulk insert
        columns = list(records[0].keys())
        col_names = ", ".join(columns)
        col_placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO raw.{table_name} ({col_names}) VALUES ({col_placeholders})"
        
        tuples = [tuple(r[col] for col in columns) for r in records]
        print(f"Inserting {len(tuples)} rows into raw.{table_name}...")
        cursor.executemany(sql, tuples)
        conn.commit()
        print("Restore completed successfully!")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

with DAG(
    'globant_restore_dag',
    default_args=default_args,
    description='AVRO Restore for Globant raw tables',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['globant', 'restore', 'avro'],
) as dag:

    restore_depts = PythonOperator(
        task_id='restore_departments',
        python_callable=restore_task,
        op_kwargs={'table_name': 'departments'},
    )

    restore_jobs = PythonOperator(
        task_id='restore_jobs',
        python_callable=restore_task,
        op_kwargs={'table_name': 'jobs'},
    )

    restore_employees = PythonOperator(
        task_id='restore_hired_employees',
        python_callable=restore_task,
        op_kwargs={'table_name': 'hired_employees'},
    )

    # Dependencies: departments and jobs must be restored before hired_employees
    # to maintain foreign key referential integrity constraints
    [restore_depts, restore_jobs] >> restore_employees
