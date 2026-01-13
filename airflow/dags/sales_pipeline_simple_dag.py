"""
Simple Sales Pipeline DAG (using factory)
Demonstrates simplified DAG creation using PipelineTaskFactory.
"""

from datetime import datetime, timedelta
from airflow import DAG

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from airflow.utils.pipeline_factory import PipelineTaskFactory
from examples.sales_transformer_example import SalesTransformer


# Default arguments
default_args = {
    'owner': 'gorigami',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'sales_pipeline_simple',
    default_args=default_args,
    description='Simplified sales pipeline using task factory',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['sales', 'pipeline', 'factory'],
)

# Create complete pipeline using factory
tasks = PipelineTaskFactory.create_complete_pipeline(
    dag=dag,
    pipeline_name='sales',
    source_path='/data/source/sales_{{ ds }}.csv',
    bronze_path='/data/bronze/sales/',
    transformer_class=SalesTransformer,
    silver_table='silver.sales_transactions',
    schema_path='ingestion/schemas/sales_schema.yaml',
    transformer_config={
        'postgres_host': 'postgres',
        'postgres_port': 5432,
        'postgres_database': 'gorigami_analytics',
        'postgres_user': 'gorigami',
        'postgres_password': 'gorigami_password'
    }
)

# Documentation
dag.doc_md = """
# Simple Sales Pipeline (Factory Pattern)

This DAG demonstrates the simplified pipeline creation using `PipelineTaskFactory`.

## Benefits

- **Less boilerplate** - Single factory call creates all tasks
- **Consistent patterns** - Standard pipeline structure
- **Easy maintenance** - Centralized task creation logic

## Pipeline

1. Ingest → 2. Validate Bronze → 3. Transform → 4. Validate Silver

All tasks created with a single `create_complete_pipeline()` call.
"""
