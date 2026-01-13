# Spark Processing Jobs

This directory contains PySpark jobs for data transformation and processing.

## Purpose

Spark jobs handle:

- Large-scale data transformations
- Data cleaning and normalization
- Feature engineering
- Aggregations and analytics
- Data quality enrichment

## Structure

Each Spark job should:

1. Create a configured SparkSession
2. Read data from a defined source
3. Apply transformations
4. Write results to a defined destination
5. Handle errors and log operations

## Running Spark Jobs

### Local Development

```bash
spark-submit \
  --master local[*] \
  processing/spark_jobs/example_transformation.py
```

### Cluster Execution

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --executor-memory 2G \
  --total-executor-cores 4 \
  processing/spark_jobs/example_transformation.py
```

### From Airflow

Use the `SparkSubmitOperator` in your DAG to submit jobs to the Spark cluster.

## Best Practices

1. **Partition data appropriately** for parallel processing
2. **Use DataFrame API** instead of RDDs when possible
3. **Enable adaptive query execution** for performance
4. **Avoid shuffles** when possible
5. **Use broadcast joins** for small dimension tables
6. **Write data in columnar formats** (Parquet, ORC)
7. **Implement checkpointing** for long-running jobs

## Data Layers

Follow the medallion architecture:

- **Raw** - Unprocessed data from sources
- **Curated** - Cleaned and transformed data
- **Consumption** - Analytics-ready datasets

## Testing

Test Spark jobs locally with sample data before deploying to production.
