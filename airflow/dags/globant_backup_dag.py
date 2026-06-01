import os
import sys
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is in path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Import AVRO schema and logic
from catalog.api.routes.backup import AVRO_SCHEMAS
import fastavro
import psycopg2
from psycopg2.extras import RealDictCursor

default_args = {
    'owner': 'nicobalaguera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def backup_task(table_name: str):
    schema = AVRO_SCHEMAS.get(table_name)
    if not schema:
        raise ValueError(f"No AVRO schema found for table: {table_name}")

    user = os.getenv("POSTGRES_USER", "globant")
    password = os.getenv("POSTGRES_PASSWORD", "globant_password")
    db_name = os.getenv("POSTGRES_DB", "globant_analytics")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")

    print(f"Connecting to database to backup table: {table_name}")
    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port,
        cursor_factory=RealDictCursor
    )
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM raw.{table_name}")
        rows = cursor.fetchall()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise e

    formatted_rows = []
    for row in rows:
        formatted_row = {}
        for field in schema["fields"]:
            name = field["name"]
            val = row.get(name)
            if isinstance(val, (datetime, date)):
                val = val.isoformat()
            formatted_row[name] = val
        formatted_rows.append(formatted_row)

    cursor.close()
    conn.close()

    # Ensure backups directory exists
    backup_dir = os.getenv("BACKUP_DIR", "backups")
    os.makedirs(backup_dir, exist_ok=True)
    
    # Generate filename
    filename = f"{table_name}_{date.today().isoformat()}.avro"
    filepath = os.path.join(backup_dir, filename)

    print(f"Writing {len(formatted_rows)} rows to AVRO file: {filepath}")
    parsed_schema = fastavro.parse_schema(schema)
    with open(filepath, "wb") as out:
        fastavro.writer(out, parsed_schema, formatted_rows)
    print("Backup completed successfully!")

with DAG(
    'globant_backup_dag',
    default_args=default_args,
    description='AVRO Backup for Globant raw tables',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['globant', 'backup', 'avro'],
) as dag:

    backup_depts = PythonOperator(
        task_id='backup_departments',
        python_callable=backup_task,
        op_kwargs={'table_name': 'departments'},
    )

    backup_jobs = PythonOperator(
        task_id='backup_jobs',
        python_callable=backup_task,
        op_kwargs={'table_name': 'jobs'},
    )

    backup_employees = PythonOperator(
        task_id='backup_hired_employees',
        python_callable=backup_task,
        op_kwargs={'table_name': 'hired_employees'},
    )

    [backup_depts, backup_jobs, backup_employees]
