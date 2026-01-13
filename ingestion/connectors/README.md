# File Connector

## Purpose

The File Connector validates file existence and accessibility for CSV, Parquet, XLSX, and JSON files **without reading the data**.

---

## Supported Formats

- **CSV/TSV** - Delimited text files
- **Parquet** - Columnar storage format
- **XLSX** - Microsoft Excel spreadsheets
- **JSON/JSONL** - JSON documents

---

## Responsibilities

- ✅ Verify file exists
- ✅ Check file permissions (readable/writable)
- ✅ Validate file format matches expected type
- ✅ Return file metadata (size, modified date, encoding)
- ✅ Support local filesystem (cloud storage ready)

---

## Configuration

### Basic Configuration

```yaml
path: "/data/source/sales_data.csv"
storage_type: local  # local, s3, gcs, azure
expected_format: csv  # Optional, auto-detected if not specified
```

### With Cloud Storage (Future)

```yaml
path: "s3://bucket/path/to/file.parquet"
storage_type: s3
credentials:
  aws_access_key_id: "${AWS_ACCESS_KEY}"
  aws_secret_access_key: "${AWS_SECRET_KEY}"
  region: us-east-1
```

---

## Usage

### Basic Validation

```python
from ingestion.connectors import FileConnector

connector = FileConnector(config={
    'path': '/data/sales.csv',
    'expected_format': 'csv'
})

# Validate file exists and is readable
connector.validate()  # Raises ConnectionValidationError if fails

# Get metadata
metadata = connector.get_metadata()
print(f"File: {metadata['filename']}")
print(f"Size: {metadata['size_mb']} MB")
print(f"Modified: {metadata['modified_at']}")
print(f"Format: {metadata['format']}")

connector.close()
```

### From YAML Configuration

```python
connector = FileConnector.from_yaml('config/sources/sales_file.yaml')
connector.validate()
```

### Context Manager

```python
with FileConnector(config={'path': '/data/sales.csv'}) as connector:
    metadata = connector.get_metadata()
    print(f"File size: {metadata['size_mb']} MB")
# Automatically closed
```

### Check File Existence

```python
connector = FileConnector(config={'path': '/data/sales.csv'})

if connector.file_exists():
    print("File is ready for extraction")
else:
    print("File not found")
```

---

## Metadata Response

```python
{
    'path': '/data/source/sales.csv',
    'filename': 'sales.csv',
    'size_bytes': 1048576,
    'size_mb': 1.0,
    'modified_at': '2026-01-13T10:30:00',
    'created_at': '2026-01-12T08:00:00',
    'format': 'csv',
    'exists': True,
    'readable': True,
    'writable': False,
    'storage_type': 'local'
}
```

---

## Error Handling

```python
from ingestion.connectors import ConnectionValidationError

try:
    connector.validate()
except ConnectionValidationError as e:
    print(f"Validation failed: {e}")
    # Handle error (alert, retry, etc.)
```

### Common Errors

- **File not found** - Path doesn't exist
- **Not a file** - Path points to directory
- **Not readable** - Insufficient permissions
- **Format mismatch** - File format doesn't match expected

---

## Format Detection

The connector automatically detects format from file extension:

| Extension | Detected Format |
| --- | --- |
| `.csv`, `.tsv` | csv |
| `.parquet`, `.pq` | parquet |
| `.xlsx`, `.xls` | xlsx |
| `.json` | json |
| `.jsonl`, `.ndjson` | jsonl |

---

## Best Practices

1. **Always validate before extraction** - Catch issues early
2. **Use expected_format** - Explicit is better than implicit
3. **Check metadata** - Verify file size before large extractions
4. **Use context managers** - Ensure proper cleanup
5. **Handle errors gracefully** - Don't fail silently

---

## Next Steps

After validating with FileConnector, use [FileExtractor](gorigamiDataFrame/ingestion/extractors/README.md) to extract the data.
