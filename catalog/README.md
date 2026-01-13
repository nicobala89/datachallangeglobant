# Gorigami Data Catalog API

## Overview

The Data Catalog API provides centralized data discovery, metadata management, and governance for all datasets in the Gorigami Data Framework.

---

## Features

### 1. Data Discovery

- List and search datasets across bronze/silver/gold layers
- Filter by layer, owner, tags
- Full-text search by name and description
- Pagination support

### 2. Metadata Management

- Auto-registration from extractors and transformers
- Schema versioning
- Dataset statistics
- Access tracking

### 3. Data Lineage

- Track upstream sources
- Track downstream consumers
- Transformation tracking
- Lineage visualization

### 4. Governance

- Access policies (public, restricted, private)
- PII detection and tagging
- Compliance tags (GDPR, etc.)
- Audit logging

---

## Quick Start

### Start the API

```bash
# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=gorigami_analytics
export POSTGRES_USER=gorigami
export POSTGRES_PASSWORD=gorigami_password

# Start API
uvicorn catalog.api.catalog_api:app --reload --port 8000
```

### Access API Documentation

Open browser: `http://localhost:8000/docs`

---

## API Endpoints

### Datasets

```bash
# List datasets
GET /api/catalog/datasets?layer=silver&page=1&page_size=20

# Get dataset by ID
GET /api/catalog/datasets/1

# Create dataset
POST /api/catalog/datasets
{
  "dataset_name": "silver.sales_transactions",
  "layer": "silver",
  "format": "table",
  "location": "postgresql://silver.sales_transactions",
  "description": "Curated sales data",
  "owner": "data_team",
  "tags": ["sales", "transactions"]
}
```

### Search

```bash
# Search datasets
GET /api/catalog/search?q=sales&layer=silver&limit=10
```

### Lineage

```bash
# Get dataset lineage
GET /api/catalog/datasets/1/lineage

Response:
{
  "dataset": {...},
  "upstream": [
    {
      "dataset_name": "bronze.sales_data",
      "transformation_name": "SalesTransformer"
    }
  ],
  "downstream": [...]
}
```

### Governance

```bash
# Get governance tags
GET /api/catalog/datasets/1/governance

# List PII datasets
GET /api/catalog/governance/pii
```

---

## Programmatic Access

### Using Catalog Client

```python
from catalog import catalog_client

# Register a dataset
dataset = catalog_client.register_dataset(
    dataset_name="silver.sales_transactions",
    layer="silver",
    location="postgresql://silver.sales_transactions",
    description="Curated sales data",
    tags=["sales", "transactions"],
    row_count=10000,
    size_mb=5.2
)

# Search datasets
results = catalog_client.search_datasets(
    query="sales",
    layer="silver"
)

# Get lineage
lineage = catalog_client.get_lineage(dataset_id=1)
```

---

## Auto-Registration

### From Extractors

Extractors can auto-register datasets:

```python
from ingestion.extractors import FileExtractor
from catalog import catalog_client

extractor = FileExtractor(...)
result = extractor.extract()

# Auto-register in catalog
catalog_client.register_dataset(
    dataset_name=f"bronze.{source_name}",
    layer="bronze",
    location=result.output_path,
    format="parquet",
    row_count=result.record_count,
    metadata=result.metadata
)
```

### From Transformers

Transformers can auto-register with lineage:

```python
from processing.transformers import BaseTransformer
from catalog import catalog_client

transformer = SalesTransformer(...)
result = transformer.run()

# Auto-register with lineage
catalog_client.register_dataset(
    dataset_name=transformer.silver_table,
    layer="silver",
    location=f"postgresql://{transformer.silver_table}",
    format="table",
    row_count=result.record_count,
    source_dataset_ids=[bronze_dataset_id],
    transformation_name="SalesTransformer"
)
```

---

## Catalog Schema

### Tables

- `catalog.datasets` - Dataset registry
- `catalog.dataset_schemas` - Schema versions
- `catalog.dataset_lineage` - Data lineage
- `catalog.access_policies` - Access control
- `catalog.governance_tags` - Governance tags
- `catalog.dataset_statistics` - Statistics
- `catalog.audit_log` - Audit trail

### Views

- `catalog.datasets_with_schema` - Datasets with current schema
- `catalog.datasets_with_lineage` - Datasets with lineage counts

---

## Example Queries

### Find all silver datasets

```bash
GET /api/catalog/datasets?layer=silver
```

### Search for PII datasets

```bash
GET /api/catalog/governance/pii
```

### Get dataset with lineage

```bash
GET /api/catalog/datasets/1
GET /api/catalog/datasets/1/lineage
```

---

## Best Practices

1. **Auto-register datasets** - Register from extractors/transformers
2. **Tag appropriately** - Use tags for discovery
3. **Track lineage** - Always specify source datasets
4. **Add descriptions** - Make datasets discoverable
5. **Set access policies** - Control data access
6. **Monitor audit logs** - Track data usage

---

## Next Steps

1. Initialize catalog schema: `psql < storage/sql/catalog_schema.sql`
2. Start catalog API: `uvicorn catalog.api.catalog_api:app --reload`
3. Access API docs: `http://localhost:8000/docs`
4. Register datasets programmatically
5. Integrate with extractors/transformers
6. Build catalog UI (optional)
