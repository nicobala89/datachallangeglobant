# SQL Storage Layer

This directory contains SQL scripts for managing the analytics serving layer.

## Purpose

The storage layer provides:

- Schema definitions for raw, curated, and consumption data
- Metadata tables for pipeline orchestration
- Views for business reporting
- Indexes for query performance

## Schema Organization

The framework uses a three-layer architecture:

### 1. Raw Schema

- Stores data as extracted from sources
- Minimal transformation
- Preserves source structure
- Used for data lineage and auditing

### 2. Curated Schema

- Cleaned and validated data
- Standardized formats
- Quality-checked
- Source of truth for analytics

### 3. Consumption Schema

- Business-friendly views
- Pre-aggregated metrics
- Optimized for reporting
- Exposed to BI tools

## Initialization

SQL scripts in this directory are automatically executed when the PostgreSQL container starts (via Docker Compose volume mount).

Scripts are executed in alphabetical order, so use prefixes for ordering:

- `01_init_schema.sql`
- `02_create_tables.sql`
- `03_create_views.sql`

## Best Practices

1. **Use schemas** to organize data by layer
2. **Create indexes** on frequently queried columns
3. **Document tables** with SQL comments
4. **Version control** all schema changes
5. **Test migrations** before applying to production
6. **Use views** to abstract complexity from consumers

## Connecting to PostgreSQL

```bash
# Using psql
psql -h localhost -p 5432 -U gorigami -d gorigami_analytics

# Using Python
from sqlalchemy import create_engine
engine = create_engine('postgresql://gorigami:gorigami_password@localhost:5432/gorigami_analytics')
```

## Migration Strategy

For schema changes:

1. Create a new migration script with timestamp prefix
2. Test in development environment
3. Apply to staging
4. Document rollback procedure
5. Apply to production during maintenance window
