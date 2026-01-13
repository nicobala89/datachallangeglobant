# Data Quality Checks

This directory contains data quality validation modules.

## Purpose

Quality checks ensure that:

- Data meets schema expectations
- Required fields are populated
- Data volume is within expected ranges
- Data is fresh and up-to-date
- Business rules are satisfied
- No duplicate records exist

## Philosophy

**Fail-fast approach**: Data that doesn't meet quality standards should not be published to downstream consumers.

## Available Checks

The `DataQualityChecker` class provides:

1. **Schema Validation** - Verify expected columns exist
2. **Null Checks** - Ensure critical fields are populated
3. **Volume Validation** - Confirm row counts are reasonable
4. **Freshness Checks** - Validate data recency
5. **Duplicate Detection** - Identify duplicate records

## Usage in Pipelines

Integrate quality checks into your Airflow DAGs:

```python
from quality.checks.data_quality_checks import DataQualityChecker

def validate_data(**context):
    df = pd.read_parquet('/path/to/data.parquet')
    
    checker = DataQualityChecker(fail_on_error=True)
    checker.check_schema(df, expected_columns=['id', 'name', 'created_at'])
    checker.check_nulls(df, columns=['id'], max_null_percentage=0)
    checker.check_volume(df, min_rows=100)
    
    return "Validation passed"
```

## Best Practices

1. **Define quality rules early** in the development process
2. **Document expectations** for each dataset
3. **Monitor validation results** over time
4. **Alert on failures** to enable quick response
5. **Version quality rules** alongside code

## Extending Quality Checks

Add custom business rule validations by:

1. Creating new methods in `DataQualityChecker`
2. Following the existing pattern (log, validate, record result)
3. Supporting both fail-fast and warning modes
