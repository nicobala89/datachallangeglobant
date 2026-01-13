# Gorigami Data Framework - Extractor Guide

## Overview

Extractors are responsible for **pulling data from sources, applying schemas, adding traceability metadata, and writing to the bronze/raw layer** in a standardized format.

---

## Extractor Responsibilities

1. **Data Extraction** - Pull data from source using a connector
2. **Schema Application** - Apply or infer data schemas
3. **Data Quality** - Basic validation checks
4. **Traceability** - Add comprehensive metadata for lineage
5. **Bronze Layer Writing** - Write to standardized Parquet format
6. **Metrics Tracking** - Log extraction metrics and performance

---

## Architecture

### Connector + Extractor Pattern

```python
# Step 1: Create connector (validates connection)
connector = FileConnector(config={'path': '/data/source/sales.csv'})
connector.validate()  # Lightweight check

# Step 2: Create extractor (uses connector)
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/',
    schema_path='schemas/sales.yaml'
)

# Step 3: Extract data
result = extractor.extract()  # Heavy lifting happens here
```

### Why Separate?

- **Faster validation** - Check connections without transferring data
- **Reusable connections** - One connector, multiple extractors
- **Clear responsibilities** - Connection vs extraction logic
- **Better testing** - Mock connectors independently

---

## Bronze Layer Conventions

### Directory Structure

```text
/data/bronze/
├── sales/
│   ├── sales_20260113_151614.parquet
│   ├── sales_20260113_162030.parquet
│   └── ...
├── customers/
│   └── customers_20260113_151615.parquet
└── products/
    └── products_20260113_151616.parquet
```

### File Naming

Format: `{entity}_{timestamp}.parquet`

- **entity**: Source entity name (table, file basename, etc.)
- **timestamp**: Extraction timestamp (`YYYYMMDD_HHMMSS`)
- **format**: Always Parquet (standardized)

### Partitioning (Optional)

For large datasets, partition by date:

```text
/data/bronze/sales/
├── year=2026/
│   └── month=01/
│       ├── sales_20260113_151614.parquet
│       └── sales_20260114_091530.parquet
```

---

## Traceability Metadata

Every extraction adds these metadata columns:

| Column | Description | Example |
| --- | --- | --- |
| `_extraction_timestamp` | When data was extracted | `2026-01-13 15:16:14` |
| `_source_system` | Source system type | `file`, `database`, `api` |
| `_source_entity` | Source entity name | `sales_data.csv`, `users` table |
| `_extractor_name` | Extractor class used | `FileExtractor` |
| `_schema_version` | Schema version applied | `1.0` |

### Custom Metadata

Add custom metadata by prefixing with `custom_`:

```python
source_info = {
    'source_system': 'file',
    'source_entity': 'sales.csv',
    'custom_department': 'finance',
    'custom_region': 'us-west'
}
df = extractor._add_traceability_metadata(df, source_info)
# Adds: _custom_department, _custom_region
```

---

## Schema Application

### Schema Definition (YAML)

```yaml
version: "1.0"
description: "Sales transaction schema"

columns:
  - name: transaction_id
    type: string
    required: true

  - name: amount
    type: float
    required: true
    validation:
      min: 0

  - name: transaction_date
    type: datetime
    required: true
```

### Supported Types

- `string` - Text data
- `integer` - Whole numbers
- `float` - Decimal numbers
- `boolean` - True/False
- `datetime` - Timestamps

### Schema Inference

If no schema is provided, extractors will infer schema from data:

```python
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/'
    # No schema_path = inferred schema
)
```

---

## Incremental Extraction

### Watermark-Based

Extract only new/updated records:

```python
from datetime import datetime, timedelta

# Get last successful run timestamp
last_run = datetime.now() - timedelta(days=1)

# Extract incrementally
result = extractor.extract_incremental(
    watermark_column='updated_at',
    last_watermark=last_run
)
```

### File-Based Tracking

For file sources, track processed files:

```python
# Track which files have been processed
processed_files = load_processed_files_list()

if file_path not in processed_files:
    result = extractor.extract()
    mark_file_as_processed(file_path)
```

---

## Available Extractors

### FileExtractor

Extracts from CSV, Parquet, XLSX files.

**Supported Formats**:

- CSV/TSV
- Parquet
- XLSX (Excel)
- JSON/JSONL

**Configuration**:

```python
extractor = FileExtractor(
    connector=file_connector,
    output_path='/data/bronze/sales/',
    schema_path='schemas/sales.yaml',
    config={
        'csv': {
            'delimiter': ',',
            'encoding': 'utf-8',
            'header': 0
        },
        'xlsx': {
            'sheet_name': 'Sales Data'
        }
    }
)
```

### DatabaseExtractor (Coming Soon)

Extracts from relational databases.

### APIExtractor (Coming Soon)

Extracts from REST APIs.

---

## Usage Examples

### Basic Extraction

```python
from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor

# Create connector
connector = FileConnector(config={'path': '/data/sales.csv'})
connector.validate()

# Create extractor
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/'
)

# Extract
result = extractor.extract()
print(f"Extracted {result.record_count} records")
```

### With Schema

```python
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/',
    schema_path='schemas/sales.yaml'
)

result = extractor.extract()
# Schema is applied and validated
```

### Incremental

```python
from datetime import datetime

last_run = get_last_watermark('sales_pipeline')

result = extractor.extract_incremental(
    watermark_column='created_at',
    last_watermark=last_run
)

update_watermark('sales_pipeline', datetime.now())
```

### Context Manager

```python
with FileConnector(config={'path': '/data/sales.csv'}) as connector:
    extractor = FileExtractor(connector, '/data/bronze/sales/')
    result = extractor.extract()
# Connector automatically closed
```

---

## Best Practices

1. **Always validate first** - Use connector.validate() before extraction
2. **Define schemas** - Don't rely on inference for production
3. **Use incremental loading** - Reduce data transfer and processing
4. **Track watermarks** - Store last successful run timestamps
5. **Monitor extraction metrics** - Log record counts, duration, errors
6. **Handle failures gracefully** - Implement retry logic
7. **Version schemas** - Track schema changes over time
8. **Test with sample data** - Validate before production runs

---

## Extraction Result

Every extraction returns an `ExtractionResult`:

```python
result = extractor.extract()

print(result.success)        # True/False
print(result.output_path)    # /data/bronze/sales/sales_20260113_151614.parquet
print(result.record_count)   # 15234
print(result.timestamp)      # 2026-01-13 15:16:14
print(result.metadata)       # {'source_file': '...', 'columns': [...]}
```

---

## Error Handling

### Common Errors

- `ExtractorError` - General extraction failure
- `SchemaValidationError` - Schema validation failed
- `ConnectionValidationError` - Connector validation failed

### Example

```python
from ingestion.extractors import ExtractorError

try:
    result = extractor.extract()
except ExtractorError as e:
    logger.error(f"Extraction failed: {e}")
    # Handle error (retry, alert, etc.)
```

---

## Creating Custom Extractors

### Step 1: Extend BaseExtractor

```python
from ingestion.extractors import BaseExtractor, ExtractionResult

class MyCustomExtractor(BaseExtractor):
    def extract(self) -> ExtractionResult:
        # Implement extraction logic
        pass
    
    def extract_incremental(self, watermark_column, last_watermark):
        # Implement incremental logic
        pass
```

### Step 2: Use Your Extractor

```python
extractor = MyCustomExtractor(
    connector=my_connector,
    output_path='/data/bronze/custom/'
)

result = extractor.extract()
```

---

## Next Steps

1. Review the [file ingestion example](gorigamiDataFrame/examples/file_ingestion_example.py)
2. Define schemas for your data sources
3. Create connector configurations
4. Build extraction pipelines in Airflow
5. Monitor bronze layer for data quality

For more details, see the [connector documentation](gorigamiDataFrame/ingestion/connectors/README.md).
