# Gorigami Data Framework - Quality Layer

## Overview

The quality layer provides dual data quality capabilities:

1. **Source Profiling** - Exploratory analysis and audit of raw sources
2. **Pipeline Validation** - Quality gates for bronze/silver/gold layers

---

## Architecture

```text
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│Source        │         │Pipeline      │         │Quality       │
│Profiling     │         │Validation    │         │Metadata      │
│(Audit)       │         │(Gates)       │         │(PostgreSQL)  │
└──────────────┘         └──────────────┘         └──────────────┘
```

---

## 1. Source Profiling (Audit & Exploration)

### Purpose

- Profile new data sources before ingestion
- Audit source data quality over time
- Generate data quality reports
- Identify data quality issues early

### Usage

```python
from quality.profiling import SourceProfiler

# Create profiler
profiler = SourceProfiler()

# Profile a file
profile = profiler.profile_file('/data/source/sales.csv')

print(f"Quality score: {profile.profile_data['quality_score']['overall_score']}")
print(f"Null percentage: {profile.profile_data['overview']['null_percentage']}%")

# Save as JSON
profile.to_json('/reports/sales_profile.json')
```

### Profile Contents

- **Overview**: Total rows, columns, duplicates, memory usage
- **Column Statistics**: For each column
  - Data type, null count/percentage
  - Distinct values
  - Min/max/mean/median/std (numeric)
  - Length statistics (string)
  - Date range (datetime)
  - Sample values
- **Quality Score**: Overall quality percentage (0-100)

---

## 2. Pipeline Validation (Quality Gates)

### 2.1 Purpose

- Validate data meets quality standards
- Block bad data from reaching silver/gold
- Monitor pipeline quality metrics
- Quarantine invalid records

### 2.2 Usage

```python
from quality.validation import PipelineValidator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Validate").getOrCreate()

# Create validator from YAML rules
validator = PipelineValidator(
    spark=spark,
    rules_path='config/quality/bronze_rules.yaml'
)

# Read data
df = spark.read.parquet('/data/bronze/sales/*.parquet')

# Validate
result = validator.validate_dataframe(df, layer='bronze')

if not result.passed:
    raise Exception(f"Validation failed: {result.failures}")
```

### Validation Rules

Define rules in YAML:

```yaml
rules:
  - name: required_columns
    type: schema
    severity: error
    config:
      columns: [transaction_id, customer_id]
  
  - name: no_nulls_in_keys
    type: null_check
    severity: error
    config:
      columns: [transaction_id]
      max_null_percentage: 0
  
  - name: positive_amount
    type: business_rule
    severity: error
    config:
      condition: "amount > 0"
```

### Rule Types

| Type | Description | Config |
| --- | --- | --- |
| `schema` | Required columns | `columns` |
| `null_check` | Null percentage limits | `columns`, `max_null_percentage` |
| `business_rule` | SQL conditions | `condition` |
| `freshness` | Data age limits | `column`, `max_age_hours` |
| `volume` | Row count limits | `min_rows`, `max_rows` |
| `uniqueness` | Duplicate detection | `columns` |

---

## Integration Patterns

### With Ingestion (Bronze)

```python
from ingestion.extractors import FileExtractor
from quality.profiling import SourceProfiler
from quality.validation import PipelineValidator

# 1. Profile source
profiler = SourceProfiler()
profile = profiler.profile_file('/data/source/sales.csv')

# 2. Extract to bronze
extractor = FileExtractor(connector, '/data/bronze/sales/')
result = extractor.extract()

# 3. Validate bronze
validator = PipelineValidator(spark, rules_path='config/quality/bronze_rules.yaml')
validation = validator.validate_dataframe(bronze_df, 'bronze')

if not validation.passed:
    raise QualityGateError("Bronze validation failed")
```

### With Transformation (Silver)

```python
from processing.transformers import BaseTransformer
from quality.validation import PipelineValidator

class SalesTransformer(BaseTransformer):
    def run(self):
        # Transform
        result = super().run()
        
        # Validate silver
        validator = PipelineValidator(
            self.spark,
            rules_path='config/quality/silver_rules.yaml'
        )
        
        silver_df = self.spark.read.table(self.silver_table)
        validation = validator.validate_dataframe(silver_df, 'silver')
        
        if not validation.passed:
            # Rollback or quarantine
            raise QualityGateError("Silver validation failed")
        
        return result
```

---

## Quality Metadata

All quality results are tracked in PostgreSQL:

### Source Profiles

```sql
SELECT * FROM quality_metadata.source_profiles 
ORDER BY profile_timestamp DESC LIMIT 10;
```

### Validation Runs

```sql
SELECT * FROM quality_metadata.validation_runs 
WHERE passed = false 
ORDER BY validation_timestamp DESC;
```

### Quality Summary

```sql
SELECT * FROM quality_metadata.quality_summary 
ORDER BY metric_timestamp DESC LIMIT 20;
```

---

## Examples

- [data_quality_example.py](gorigamiDataFrame/examples/data_quality_example.py) - Complete examples
- [bronze_rules.yaml](gorigamiDataFrame/config/quality/bronze_rules.yaml) - Bronze validation rules
- [silver_rules.yaml](gorigamiDataFrame/config/quality/silver_rules.yaml) - Silver validation rules

---

## Best Practices

1. **Profile before ingestion** - Understand source data characteristics
2. **Define strict rules** - Use `error` severity for critical validations
3. **Track trends** - Monitor quality scores over time
4. **Fail fast** - Block pipelines on quality failures
5. **Quarantine bad data** - Separate invalid records for review
6. **Alert on degradation** - Monitor for quality score drops

---

## Next Steps

1. Profile your source files
2. Define quality rules for each layer
3. Integrate validation into pipelines
4. Monitor quality metadata tables
5. Set up alerts for quality failures
