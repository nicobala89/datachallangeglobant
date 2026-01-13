# Airflow DAGs

This directory contains Apache Airflow DAG definitions for orchestrating data pipelines.

## Purpose

DAGs (Directed Acyclic Graphs) define the workflow and dependencies between data pipeline tasks.

## Structure

- Each DAG file should be a self-contained Python module
- Use meaningful names that describe the pipeline purpose
- Follow the naming convention: `{domain}_{pipeline_name}.py`

## Best Practices

1. **Use default_args** for common task configurations
2. **Set appropriate retry policies** for fault tolerance
3. **Tag your DAGs** for easy filtering in the Airflow UI
4. **Document dependencies** clearly in task definitions
5. **Use sensors** for external dependencies
6. **Implement idempotency** in all tasks

## Example DAGs

- `example_pipeline.py` - Template for a complete data pipeline

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [DAG Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
