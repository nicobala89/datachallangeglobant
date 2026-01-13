"""
Complete Sales Pipeline DAG
Demonstrates end-to-end pipeline with ingestion, transformation, and quality validation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import custom operators
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from airflow.operators.ingestion_operator import IngestionOperator
from airflow.operators.transformation_operator import TransformationOperator
from airflow.operators.quality_validation_operator import QualityValidationOperator
from examples.sales_transformer_example import SalesTransformer


# Default arguments
default_args = {
    'owner': 'gorigami',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'sales_pipeline',
    default_args=default_args,
    description='Complete sales data pipeline: ingest → validate → transform → validate',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['sales', 'pipeline', 'complete'],
) as dag:

    # Task 1: Ingest sales data to bronze
    ingest_sales = IngestionOperator(
        task_id='ingest_sales_data',
        source_path='/data/source/sales_{{ ds }}.csv',
        bronze_path='/data/bronze/sales/',
        connector_type='file',
        connector_config={
            'expected_format': 'csv'
        },
        schema_path='ingestion/schemas/sales_schema.yaml',
        extractor_config={
            'csv': {
                'delimiter': ',',
                'encoding': 'utf-8'
            }
        }
    )

    # Task 2: Validate bronze data
    validate_bronze = QualityValidationOperator(
        task_id='validate_bronze_quality',
        data_path='/data/bronze/sales/*.parquet',
        layer='bronze',
        rules_path='config/quality/bronze_rules.yaml',
        fail_on_error=True
    )

    # Task 3: Transform to silver
    transform_sales = TransformationOperator(
        task_id='transform_to_silver',
        transformer_class=SalesTransformer,
        bronze_path='/data/bronze/sales/*.parquet',
        silver_table='silver.sales_transactions',
        transformer_config={
            'postgres_host': 'postgres',
            'postgres_port': 5432,
            'postgres_database': 'gorigami_analytics',
            'postgres_user': 'gorigami',
            'postgres_password': 'gorigami_password'
        },
        spark_config={
            'spark.jars': '/opt/spark/jars/postgresql-jdbc.jar'
        }
    )

    # Task 4: Validate silver data
    validate_silver = QualityValidationOperator(
        task_id='validate_silver_quality',
        data_path='silver.sales_transactions',
        layer='silver',
        rules_path='config/quality/silver_rules.yaml',
        fail_on_error=True
    )

    # Task dependencies
    ingest_sales >> validate_bronze >> transform_sales >> validate_silver


# Documentation
dag.doc_md = """
# Sales Pipeline DAG

Complete end-to-end sales data pipeline with quality gates.

## Pipeline Flow

1. **Ingest Sales Data** - Extract CSV to bronze Parquet
2. **Validate Bronze** - Quality checks on raw data
3. **Transform to Silver** - Clean, deduplicate, enrich
4. **Validate Silver** - Quality checks on curated data

## Quality Gates

- Bronze validation blocks transformation if data quality is poor
- Silver validation ensures only high-quality data reaches consumers

## Schedule

Runs daily at midnight, processing previous day's sales data.

## Monitoring

Check XCom for detailed results from each task:
- `ingestion_result` - Extraction metadata
- `validation_result` - Quality validation results
- `transformation_result` - Transformation metadata
"""
