import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'nicobalaguera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def validate_sources():
    """Verify if target CSV files exist in data/csv/"""
    csv_dir = "data/csv"
    required_files = ["departments.csv", "jobs.csv", "hired_employees.csv"]
    
    missing = []
    for file in required_files:
        path = os.path.join(csv_dir, file)
        if not os.path.exists(path):
            missing.append(file)
            
    if missing:
        raise FileNotFoundError(f"Missing required source CSV files in data/csv/: {', '.join(missing)}")
    print("All source CSV files are present!")

def run_spark_migration(table_name: str, silver_table: str):
    """Run the PySpark GlobantTransformer migration for a specific table"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from pyspark.sql import SparkSession
    from processing.transformers.globant_transformer import GlobantTransformer

    csv_path = os.path.join("data/csv", f"{table_name}.csv")

    spark = SparkSession.builder \
        .appName(f"Globant Ingestion - {table_name}") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    try:
        transformer = GlobantTransformer(
            spark=spark,
            table_name=table_name,
            csv_path=csv_path,
            silver_table=silver_table,
            config={
                'postgres_host': os.getenv('POSTGRES_HOST', 'localhost'),
                'postgres_port': os.getenv('POSTGRES_PORT', 5432),
                'postgres_database': os.getenv('POSTGRES_DB', 'globant_analytics'),
                'postgres_user': os.getenv('POSTGRES_USER', 'globant'),
                'postgres_password': os.getenv('POSTGRES_PASSWORD', 'globant_password')
            }
        )
        result = transformer.run_pipeline()
        print(f"Migration completed successfully: {result}")
    finally:
        spark.stop()

def run_spark_analytics():
    """Run PySpark jobs to compute analytics marts in gold layer"""
    from pyspark.sql import SparkSession
    from processing.spark_jobs.globant_analytics_job import run_analytics_marts

    spark = SparkSession.builder \
        .appName("Globant Analytics Job") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    try:
        run_analytics_marts(spark)
        print("Analytics marts computed and materialized successfully!")
    finally:
        spark.stop()

with DAG(
    'globant_csv_ingestion_dag',
    default_args=default_args,
    description='Globant CSV historic migration and analytics pipeline',
    schedule_interval=None,  # Run manually / triggered once
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['globant', 'ingestion', 'spark'],
) as dag:

    task_validate = PythonOperator(
        task_id='validate_source_files',
        python_callable=validate_sources,
    )

    task_load_depts = PythonOperator(
        task_id='load_departments',
        python_callable=run_spark_migration,
        op_kwargs={'table_name': 'departments', 'silver_table': 'raw.departments'},
    )

    task_load_jobs = PythonOperator(
        task_id='load_jobs',
        python_callable=run_spark_migration,
        op_kwargs={'table_name': 'jobs', 'silver_table': 'raw.jobs'},
    )

    task_load_employees = PythonOperator(
        task_id='load_hired_employees',
        python_callable=run_spark_migration,
        op_kwargs={'table_name': 'hired_employees', 'silver_table': 'raw.hired_employees'},
    )

    task_analytics = PythonOperator(
        task_id='run_analytics_job',
        python_callable=run_spark_analytics,
    )

    task_validate >> task_load_depts >> task_load_jobs >> task_load_employees >> task_analytics
