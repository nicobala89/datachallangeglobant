# Gorigami Data Framework - Ingestion Layer

## Overview

The ingestion layer provides a **modular, two-tier architecture** for extracting data from various sources and loading it into the bronze/raw layer with full traceability.

### Architecture

```text
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  Connector  │────────▶│  Extractor  │────────▶│Bronze Layer │
│ (Validate)  │         │  (Extract)  │         │  (Parquet)  │
└─────────────┘         └─────────────┘         └─────────────┘
```

**Two-Tier Design:**

1. **Connectors** (`ingestion/connectors/`)
   - Lightweight connection validation
   - Verify entity/document existence
   - Return metadata without data transfer
   - Fast and reusable

2. **Extractors** (`ingestion/extractors/`)
   - Pull data from sources
   - Apply schema definitions
   - Add traceability metadata
   - Write to bronze layer in Parquet format

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Basic File Extraction

```python
from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor

# Validate file exists
connector = FileConnector(config={'path': '/data/sales.csv'})
connector.validate()

# Extract to bronze layer
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/'
)

result = extractor.extract()
print(f"Extracted {result.record_count} records to {result.output_path}")
```

### 3. With Schema

```python
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/',
    schema_path='ingestion/schemas/sales_schema.yaml'
)

result = extractor.extract()
# Schema validated and applied
```

---

## Directory Structure

```text
ingestion/
├── __init__.py
├── connectors/              # Connection validation
│   ├── __init__.py
│   ├── README.md           # Connector-specific docs
│   ├── base_connector.py   # Base class
│   └── file_connector.py   # File connector
├── extractors/             # Data extraction
│   ├── __init__.py
│   ├── README.md          # Extractor-specific docs
│   ├── EXTRACTOR_GUIDE.md # Detailed usage guide
│   ├── base_extractor.py  # Base class
│   ├── file_extractor.py  # File extractor
│   ├── api_extractor.py   # API extractor (legacy)
│   └── database_extractor.py  # DB extractor (legacy)
├── schemas/               # Schema definitions
│   └── sales_schema.yaml
└── config/               # Connector configurations
    └── sources/
```

---

## Available Connectors & Extractors

### File Connector & Extractor

**Supported Formats:** CSV, Parquet, XLSX, JSON/JSONL

**Use Case:** Extract data from file-based sources

**Example:**

```python
from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor

connector = FileConnector(config={
    'path': '/data/sales.csv',
    'expected_format': 'csv'
})

extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/',
    config={
        'csv': {'delimiter': ',', 'encoding': 'utf-8'}
    }
)

result = extractor.extract()
```

**Documentation:** See [connectors/README.md](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/ingestion/connectors/README.md) and [extractors/EXTRACTOR_GUIDE.md](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/ingestion/extractors/EXTRACTOR_GUIDE.md)

---

## Key Features

### 1. Traceability Metadata

Every extraction adds metadata columns:

- `_extraction_timestamp` - When extracted
- `_source_system` - Source type (file, database, api)
- `_source_entity` - Entity name
- `_extractor_name` - Extractor used
- `_schema_version` - Schema version

### 2. Schema Application

Define schemas in YAML:

```yaml
# ingestion/schemas/sales_schema.yaml
version: "1.0"
columns:
  - name: transaction_id
    type: string
    required: true
  - name: amount
    type: float
    required: true
```

### 3. Bronze Layer Standardization

All data written to bronze layer in Parquet format:

```text
/data/bronze/
├── sales/
│   └── sales_20260113_152329.parquet
├── customers/
│   └── customers_20260113_152330.parquet
```

### 4. Incremental Extraction

Extract only new/updated records:

```python
from datetime import datetime, timedelta

last_run = datetime.now() - timedelta(days=1)

result = extractor.extract_incremental(
    watermark_column='updated_at',
    last_watermark=last_run
)
```

---

## Configuration

### Connector Configuration (YAML)

```yaml
# config/sources/sales_file.yaml
path: "/data/source/sales.csv"
storage_type: local
expected_format: csv
```

### Schema Definition (YAML)

```yaml
# ingestion/schemas/sales_schema.yaml
version: "1.0"
description: "Sales transaction schema"
columns:
  - name: transaction_id
    type: string
    required: true
  - name: amount
    type: float
    required: true
```

### Environment Variables

Use `${VAR_NAME}` syntax in configurations:

```yaml
path: "${DATA_SOURCE_PATH}/sales.csv"
```

---

## Examples

See [examples/file_ingestion_example.py](file:///Users/nicolasbalaguera/dev/gorigamiDataFrame/examples/file_ingestion_example.py) for comprehensive usage examples.

---

## Integration with Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor

def extract_sales_data(**context):
    connector = FileConnector.from_yaml('config/sources/sales.yaml')
    extractor = FileExtractor(connector, '/data/bronze/sales/')
    result = extractor.extract()
    return result.record_count

with DAG('sales_ingestion', ...) as dag:
    extract_task = PythonOperator(
        task_id='extract_sales',
        python_callable=extract_sales_data
    )
```

---

## Testing

Run the verification test:

```bash
python tests/test_file_ingestion.py
```

This creates sample CSV/Parquet/Excel files and extracts them to the bronze layer.

---

## Documentation

- **[Connectors](gorigamiDataFrame/ingestion/connectors/README.md)** - Connector implementation details
- **[Extractors](gorigamiDataFrame/ingestion/extractors/EXTRACTOR_GUIDE.md)** - Extractor usage guide
- **[Examples](gorigamiDataFrame/examples/file_ingestion_example.py)** - Code examples

---

## Next Steps

1. **Define schemas** for your data sources in `ingestion/schemas/`
2. **Create connector configs** in `config/sources/`
3. **Build extraction pipelines** in Airflow DAGs
4. **Monitor bronze layer** for data quality

---

## Future Enhancements

- Database connector & extractor
- API connector & extractor
- Cloud storage support (S3, GCS, Azure)
- Streaming connector & extractor
- Connection pooling
- Retry logic with exponential backoff
