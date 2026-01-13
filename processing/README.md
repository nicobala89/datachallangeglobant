# Gorigami Data Framework - Processing Layer

## Overview

The processing layer transforms bronze Parquet data using PySpark and loads it to the silver PostgreSQL layer with decoupled transformation logic.

### Architecture

```text
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│Bronze Layer │────────▶│Transformers │────────▶│Silver Layer │
│  (Parquet)  │         │  (PySpark)  │         │ (PostgreSQL)│
└─────────────┘         └─────────────┘         └─────────────┘
```

**Three-Component Design:**

1. **Bronze Reader** - Reads Parquet from bronze layer
2. **Transformers** - Apply business logic transformations
3. **Silver Writer** - Writes to PostgreSQL silver layer

---

## Quick Start

### 1. Create a Transformer

```python
from pyspark.sql import DataFrame
from processing.transformers import BaseTransformer
from processing.transformations import common_transformations as ct

class MyTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        # Apply transformations
        df = ct.clean_nulls(df, columns=['id', 'name'])
        df = ct.deduplicate(df, key_columns=['id'])
        return df
```

### 2. Run the Transformation

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyTransformer").getOrCreate()

transformer = MyTransformer(
    spark=spark,
    bronze_path='/data/bronze/sales/*.parquet',
    silver_table='silver.sales_transactions'
)

result = transformer.run()
print(f"Processed {result.record_count} records")
```

---

## Directory Structure

```text
processing/
├── __init__.py
├── README.md                    # This file
├── transformers/                # Transformer classes
│   ├── __init__.py
│   ├── base_transformer.py     # Base transformer class
│   └── TRANSFORMER_GUIDE.md    # Detailed guide
├── transformations/             # Transformation functions
│   ├── __init__.py
│   ├── transformation_registry.py
│   └── common_transformations.py
└── spark_jobs/                  # Legacy Spark jobs
    └── example_transformation.py
```

---

## Key Features

### 1. Decoupled Transformation Logic

Transformations are separate, reusable functions:

```python
from processing.transformations import common_transformations as ct

# Use in any transformer
df = ct.clean_nulls(df, columns=['id'])
df = ct.deduplicate(df, key_columns=['id'])
df = ct.standardize_dates(df, column='created_at')
```

### 2. Bronze → Silver Flow

- **Read**: Load Parquet from bronze layer
- **Transform**: Apply business logic
- **Write**: Save to PostgreSQL silver layer
- **Track**: Record metadata and lineage

### 3. Silver Layer Metadata

Every silver table includes:

| Column | Description |
| --- | --- |
| `_silver_load_timestamp` | When loaded to silver |
| `_bronze_source_path` | Source bronze file |
| `_transformer_name` | Transformer used |
| `_transformation_version` | Version applied |

### 4. Incremental Processing

Process only new data:

```python
result = transformer.read_bronze_incremental(
    watermark_column='updated_at',
    last_watermark=last_run_timestamp
)
```

---

## Available Transformations

See [common_transformations.py](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/processing/transformations/common_transformations.py) for all functions:

- `clean_nulls()` - Handle null values
- `standardize_dates()` - Normalize date formats
- `deduplicate()` - Remove duplicates
- `enrich_with_lookup()` - Join with reference data
- `aggregate_metrics()` - Calculate aggregations
- `add_calculated_column()` - Add computed columns
- `filter_by_condition()` - Filter rows
- `rename_columns()` - Rename columns
- `cast_columns()` - Cast data types

---

## Silver Layer Conventions

### Schema Organization

```sql
CREATE SCHEMA silver;           -- Curated data
CREATE SCHEMA silver_metadata;  -- Transformation tracking
```

### Table Naming

Format: `silver.{domain}_{entity}`

Examples:

- `silver.sales_transactions`
- `silver.customer_profiles`
- `silver.product_catalog`

### Indexes

- Primary key on business key
- Index on `_silver_load_timestamp`
- Indexes on frequently queried columns

---

## Examples

- [sales_transformer_example.py](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/examples/sales_transformer_example.py) - Complete sales transformation
- [sales_transformation.yaml](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/config/transformations/sales_transformation.yaml) - Configuration example

---

## Integration with Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from processing.transformers import MyTransformer

def run_transformation(**context):
    spark = SparkSession.builder.appName("Transform").getOrCreate()
    transformer = MyTransformer(spark, '/data/bronze/sales/', 'silver.sales')
    result = transformer.run()
    spark.stop()
    return result.record_count

with DAG('sales_transformation', ...) as dag:
    transform_task = PythonOperator(
        task_id='transform_sales',
        python_callable=run_transformation
    )
```

---

## Documentation

- **[Transformer Guide](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/processing/transformers/TRANSFORMER_GUIDE.md)** - Detailed transformer documentation
- **[Common Transformations](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/processing/transformations/common_transformations.py)** - Reusable transformation functions
- **[Silver Schema](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/storage/sql/silver_schema.sql)** - PostgreSQL schema definitions

---

## Next Steps

1. **Review examples** - Check [sales_transformer_example.py](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/examples/sales_transformer_example.py)
2. **Create transformer** - Extend `BaseTransformer` for your use case
3. **Define transformations** - Use common functions or create custom ones
4. **Test locally** - Run with sample bronze data
5. **Integrate with Airflow** - Add to your DAG
6. **Monitor silver layer** - Verify data quality in PostgreSQL
