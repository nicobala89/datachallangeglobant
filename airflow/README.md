# Gorigami Data Framework - Airflow Orchestration

## Overview

The Airflow layer provides reusable operators and task factories for orchestrating ingestion, transformation, and quality validation pipelines.

---

## Custom Operators

### 1. IngestionOperator

Runs connector + extractor for data ingestion.

```python
from airflow.operators.ingestion_operator import IngestionOperator

ingest = IngestionOperator(
    task_id='ingest_sales',
    source_path='/data/source/sales.csv',
    bronze_path='/data/bronze/sales/',
    schema_path='ingestion/schemas/sales_schema.yaml'
)
```

**Features**:

- Validates connection before extraction
- Applies schema if provided
- Writes to bronze Parquet
- Pushes metadata to XCom

### 2. TransformationOperator

Runs PySpark transformers.

```python
from airflow.operators.transformation_operator import TransformationOperator
from examples.sales_transformer_example import SalesTransformer

transform = TransformationOperator(
    task_id='transform_sales',
    transformer_class=SalesTransformer,
    bronze_path='/data/bronze/sales/*.parquet',
    silver_table='silver.sales_transactions'
)
```

**Features**:

- Creates Spark session automatically
- Runs transformer.run()
- Writes to PostgreSQL silver layer
- Cleans up Spark session

### 3. QualityValidationOperator

Validates data quality with configurable rules.

```python
from airflow.operators.quality_validation_operator import QualityValidationOperator

validate = QualityValidationOperator(
    task_id='validate_bronze',
    data_path='/data/bronze/sales/*.parquet',
    layer='bronze',
    rules_path='config/quality/bronze_rules.yaml',
    fail_on_error=True  # Fail task if validation fails
)
```

**Features**:

- Loads rules from YAML
- Validates data against rules
- Fails task on errors (optional)
- Pushes validation results to XCom

---

## Pipeline Task Factory

Simplifies DAG creation with reusable patterns.

### Complete Pipeline

```python
from airflow.utils.pipeline_factory import PipelineTaskFactory
from examples.sales_transformer_example import SalesTransformer

tasks = PipelineTaskFactory.create_complete_pipeline(
    dag=dag,
    pipeline_name='sales',
    source_path='/data/source/sales.csv',
    bronze_path='/data/bronze/sales/',
    transformer_class=SalesTransformer,
    silver_table='silver.sales_transactions',
    schema_path='ingestion/schemas/sales_schema.yaml'
)
```

**Creates**:

1. Ingestion task
2. Bronze validation task
3. Transformation task
4. Silver validation task

All with proper dependencies configured.

---

## Example DAGs

### Complete Pipeline DAG

[sales_pipeline_dag.py](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/airflow/dags/sales_pipeline_dag.py)

Full example showing all operators used individually.

### Simple Pipeline DAG

[sales_pipeline_simple_dag.py](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/airflow/dags/sales_pipeline_simple_dag.py)

Simplified example using `PipelineTaskFactory`.

---

## DAG Structure

### Standard Pipeline Pattern

```text
Ingest → Validate Bronze → Transform → Validate Silver
```

**Quality Gates**:

- Bronze validation blocks transformation if data is poor
- Silver validation ensures only quality data reaches consumers

### XCom Data

Each operator pushes metadata to XCom:

- `ingestion_result` - Extraction metadata
- `transformation_result` - Transformation metadata
- `validation_result` - Quality validation results

Access in downstream tasks:

```python
def process_results(**context):
    ingestion = context['task_instance'].xcom_pull(
        task_ids='ingest_sales',
        key='ingestion_result'
    )
    print(f"Extracted {ingestion['record_count']} records")
```

---

## Configuration

### Operator Parameters

All operators support templated fields:

```python
IngestionOperator(
    source_path='/data/source/sales_{{ ds }}.csv',  # Templated
    bronze_path='/data/bronze/sales/',
    # ...
)
```

### Spark Configuration

Pass Spark config to transformation and validation operators:

```python
TransformationOperator(
    # ...
    spark_config={
        'spark.jars': '/path/to/postgresql-jdbc.jar',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g'
    }
)
```

---

## Best Practices

1. **Use task factory** - Reduces boilerplate for standard pipelines
2. **Enable quality gates** - Set `fail_on_error=True` for validations
3. **Monitor XCom** - Check operator results in downstream tasks
4. **Template paths** - Use `{{ ds }}` for date-partitioned data
5. **Configure retries** - Set appropriate retry policies
6. **Document DAGs** - Use `dag.doc_md` for documentation

---

## Next Steps

1. Review example DAGs
2. Create your own transformer class
3. Define quality rules
4. Build your pipeline DAG
5. Test locally with Airflow
6. Deploy to production
