"""
Example Airflow DAG for the Gorigami Data Framework
This DAG demonstrates a complete data pipeline with ingestion, processing, and quality checks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'gorigami',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_data_pipeline',
    default_args=default_args,
    description='Example end-to-end data pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'gorigami'],
)


def ingest_data(**context):
    """
    Example ingestion function
    Replace with actual ingestion logic from ingestion/extractors/
    """
    print("Ingesting data from source...")
    # Add your ingestion logic here
    return "Ingestion completed"


def process_data(**context):
    """
    Example processing function
    Replace with actual Spark job from processing/spark_jobs/
    """
    print("Processing data with transformations...")
    # Add your processing logic here
    return "Processing completed"


def validate_data(**context):
    """
    Example data quality validation
    Replace with actual quality checks from quality/checks/
    """
    print("Validating data quality...")
    # Add your validation logic here
    return "Validation completed"


# Define tasks
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Example of a Spark job submission (commented out)
# spark_submit_task = BashOperator(
#     task_id='submit_spark_job',
#     bash_command='spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_jobs/example_job.py',
#     dag=dag,
# )

# Define task dependencies
ingest_task >> process_task >> validate_task
