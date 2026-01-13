# File Extractor

## Purpose

The File Extractor reads data from CSV, Parquet, XLSX, and JSON files, applies schemas, adds traceability metadata, and writes to the bronze layer in standardized Parquet format.

---

## Supported Formats

- **CSV/TSV** - Delimited text files
- **Parquet** - Columnar storage format
- **XLSX** - Microsoft Excel spreadsheets
- **JSON/JSONL** - JSON documents

---

## Responsibilities

- ✅ Read data from files using FileConnector
- ✅ Apply or infer schema definitions
- ✅ Add traceability metadata columns
- ✅ Write to bronze layer in Parquet format
- ✅ Support incremental extraction via watermark
- ✅ Track extraction metrics

---

## Usage

### Basic Extraction

```python
from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor

# Create and validate connector
connector = FileConnector(config={'path': '/data/sales.csv'})
connector.validate()

# Create extractor
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/'
)

# Extract data
result = extractor.extract()

print(f"Records: {result.record_count}")
print(f"Output: {result.output_path}")
```

### With Schema

```python
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/',
    schema_path='ingestion/schemas/sales_schema.yaml'
)

result = extractor.extract()
# Schema validated and applied
```

### With Format-Specific Configuration

```python
# CSV configuration
extractor = FileExtractor(
    connector=connector,
    output_path='/data/bronze/sales/',
    config={
        'csv': {
            'delimiter': ',',
            'encoding': 'utf-8',
            'header': 0,
            'skiprows': None
        }
    }
)

# Excel configuration
extractor = FileExtractor(
    connector=excel_connector,
    output_path='/data/bronze/reports/',
    config={
        'xlsx': {
            'sheet_name': 'Sales Data',
            'header': 0
        }
    }
)
```

### Incremental Extraction

```python
from datetime import datetime, timedelta

# Get last successful run
last_run = datetime.now() - timedelta(days=1)

# Extract only new records
result = extractor.extract_incremental(
    watermark_column='updated_at',
    last_watermark=last_run
)

print(f"New records: {result.record_count}")
```

---

## Configuration Options

### CSV Configuration

```python
config = {
    'csv': {
        'delimiter': ',',      # Field delimiter
        'encoding': 'utf-8',   # File encoding
        'header': 0,           # Header row index
        'skiprows': None,      # Rows to skip
        'na_values': None      # Values to treat as NA
    }
}
```

### Excel Configuration

```python
config = {
    'xlsx': {
        'sheet_name': 0,       # Sheet name or index
        'header': 0,           # Header row index
        'skiprows': None       # Rows to skip
    }
}
```

### JSON Configuration

```python
config = {
    'json': {
        'orient': 'records'    # JSON orientation
    }
}
```

---

## Extraction Result

```python
result = extractor.extract()

# Access result properties
result.success          # True/False
result.output_path      # /data/bronze/sales/sales_20260113_152329.parquet
result.record_count     # 15234
result.timestamp        # 2026-01-13 15:23:29
result.metadata         # {'source_file': '...', 'columns': [...]}
```

---

## Traceability Metadata

Every extraction adds these columns:

| Column | Description | Example |
| --- | --- | --- |
| `_extraction_timestamp` | When extracted | `2026-01-13 15:23:29` |
| `_source_system` | Source type | `file` |
| `_source_entity` | Source name | `sales_data.csv` |
| `_extractor_name` | Extractor used | `FileExtractor` |
| `_schema_version` | Schema version | `1.0` |

---

## Bronze Layer Output

### File Naming

Format: `{entity}_{timestamp}.parquet`

Example: `sales_20260113_152329.parquet`

### Directory Structure

```text
/data/bronze/
├── sales/
│   ├── sales_20260113_152329.parquet
│   └── sales_20260114_091530.parquet
├── customers/
│   └── customers_20260113_152330.parquet
```

### File Format

All bronze layer files are written in **Parquet format** with:

- Snappy compression
- PyArrow engine
- Traceability metadata columns included

---

## Error Handling

```python
from ingestion.extractors import ExtractorError

try:
    result = extractor.extract()
except ExtractorError as e:
    print(f"Extraction failed: {e}")
    # Handle error (retry, alert, etc.)
```

### Common Errors

- **ExtractorError** - General extraction failure
- **SchemaValidationError** - Schema validation failed
- **ConnectionValidationError** - Connector validation failed

---

## Best Practices

1. **Validate connector first** - Always call `connector.validate()` before extraction
2. **Define schemas** - Don't rely on inference for production
3. **Use incremental loading** - Reduce data transfer and processing time
4. **Monitor extraction metrics** - Log record counts, duration, errors
5. **Handle failures gracefully** - Implement retry logic
6. **Test with sample data** - Validate before production runs

---

## Next Steps

See the [Extractor Guide](gorigamiDataFrame/ingestion/extractors/EXTRACTOR_GUIDE.md) for detailed documentation on:

- Schema application
- Bronze layer conventions
- Incremental extraction patterns
- Creating custom extractors
