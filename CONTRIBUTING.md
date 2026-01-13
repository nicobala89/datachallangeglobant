# Contributing to Gorigami Data Framework

## Overview

The Gorigami Data Framework is designed as a **template** for building data engineering projects. This guide explains how to use the framework for your own projects.

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/gorigamidev/gorigamiDataFrame.git
cd gorigamiDataFrame
```

### 2. Create Your Project Branch

**Important**: Always create a new branch for your project. This allows the framework to evolve independently while your project remains stable.

```bash
# Create and switch to your project branch
git checkout -b my-project-name

# Example: For a sales analytics project
git checkout -b sales-analytics-pipeline
```

### 3. Initial Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your configuration

# Start infrastructure
docker-compose up -d
```

---

## Development Workflow

### Branch Strategy

- **`main`** - Framework template (do not modify directly)
- **`my-project-name`** - Your project branch (customize here)

### Making Changes

1. **Work on your project branch**:

   ```bash
   git checkout my-project-name
   ```

2. **Make your changes**:
   - Add new connectors for your data sources
   - Create transformers for your business logic
   - Define quality rules for your data
   - Build DAGs for your pipelines

3. **Commit regularly**:

   ```bash
   git add .
   git commit -m "Add sales data connector"
   git push origin my-project-name
   ```

### Updating from Framework

If the framework template is updated with new features:

```bash
# Fetch latest framework changes
git fetch origin main

# Merge framework updates into your project (carefully)
git checkout my-project-name
git merge origin/main

# Resolve any conflicts
# Test thoroughly after merge
```

---

## Customization Guide

### Adding New Connectors

1. **Create connector class**:

   ```python
   # ingestion/connectors/my_connector.py
   from ingestion.connectors import BaseConnector

   class MyConnector(BaseConnector):
       def validate(self):
           # Your validation logic
           pass

       def get_metadata(self):
           # Your metadata logic
           pass
   ```

2. **Update package init**:

   ```python
   # ingestion/connectors/__init__.py
   from .my_connector import MyConnector
   ```

3. **Create configuration**:

   ```yaml
   # config/sources/my_source.yaml
   type: my_connector
   config:
     # Your config
   ```

### Adding New Transformers

1. **Create transformer class**:

   ```python
   # processing/transformers/my_transformer.py
   from processing.transformers import BaseTransformer

   class MyTransformer(BaseTransformer):
       def transform(self, df):
           # Your transformation logic
           return df
   ```

2. **Use in DAG**:

   ```python
   # airflow/dags/my_pipeline_dag.py
   from processing.transformers.my_transformer import MyTransformer

   transform = TransformationOperator(
       task_id='transform',
       transformer_class=MyTransformer,
       # ...
   )
   ```

### Adding Quality Rules

1. **Create rules file**:

   ```yaml
   # config/quality/my_rules.yaml
   rules:
     - name: my_validation
       type: business_rule
       severity: error
       config:
         condition: "amount > 0"
   ```

2. **Use in validation**:

   ```python
   validator = PipelineValidator(
       spark=spark,
       rules_path='config/quality/my_rules.yaml'
   )
   ```

### Creating DAGs

1. **Create DAG file**:

   ```python
   # airflow/dags/my_pipeline_dag.py
   from airflow import DAG
   from airflow.utils.pipeline_factory import PipelineTaskFactory

   with DAG('my_pipeline', ...) as dag:
       tasks = PipelineTaskFactory.create_complete_pipeline(
           dag=dag,
           pipeline_name='my_data',
           # Your config
       )
   ```

2. **Deploy to Airflow**:

   ```bash
   cp airflow/dags/my_pipeline_dag.py $AIRFLOW_HOME/dags/
   ```

---

## Best Practices

### Code Organization

- Keep connectors in `ingestion/connectors/`
- Keep extractors in `ingestion/extractors/`
- Keep transformers in `processing/transformers/`
- Keep transformation functions in `processing/transformations/`
- Keep DAGs in `airflow/dags/`
- Keep configs in `config/`

### Configuration Management

- Use YAML for all configurations
- Use environment variables for secrets
- Never commit credentials to Git
- Use `.env` for local development

### Testing

- Test connectors before extractors
- Test transformations with sample data
- Validate quality rules with known data
- Test DAGs in development Airflow first

### Documentation

- Document custom connectors in docstrings
- Document transformers with usage examples
- Document quality rules in YAML comments
- Update project README with your specifics

---

## Common Tasks

### Adding a New Data Source

1. Create connector class
2. Create extractor (or use existing)
3. Create schema YAML
4. Create source config
5. Test extraction locally
6. Create DAG task
7. Register in catalog

### Adding a New Transformation

1. Create transformer class
2. Define transformation logic
3. Create transformation config
4. Test with sample data
5. Create DAG task
6. Add quality validation

### Adding Quality Checks

1. Define rules in YAML
2. Test rules with sample data
3. Integrate into DAG
4. Set fail_on_error appropriately
5. Monitor validation results

---

## Extending the Framework

### Adding New Abstractions

If you find patterns that could be abstracted:

1. Create the abstraction in your project branch
2. Test thoroughly
3. Document well
4. Consider contributing back to main framework

### Contributing Back to Framework

If you create useful abstractions that could benefit others:

1. Create a feature branch from `main`
2. Implement the feature
3. Add tests and documentation
4. Submit a pull request
5. Discuss with framework maintainers

---

## Troubleshooting

### Common Issues

**Import Errors**:

- Ensure you're in the project root directory
- Check `PYTHONPATH` includes project root
- Verify all `__init__.py` files exist

**Database Connection Errors**:

- Check `.env` configuration
- Verify Docker containers are running
- Check PostgreSQL credentials

**Airflow DAG Errors**:

- Check DAG syntax with `python my_dag.py`
- Verify all imports are available
- Check Airflow logs

**Spark Errors**:

- Verify Spark is installed
- Check PostgreSQL JDBC driver is available
- Verify Spark configuration

---

## Support

For framework-specific questions:

- Check documentation in each module
- Review example files
- Contact Gorigami data team

For project-specific questions:

- Consult your team
- Review your project documentation
- Check your project's issue tracker

---

## Version Control

### Recommended `.gitignore` Additions

Add project-specific ignores to `.gitignore`:

```gitignore
# Project-specific data
/data/my_project/

# Project-specific configs
/config/my_project/secrets.yaml

# Project-specific outputs
/output/my_project/
```

### Commit Messages

Use clear, descriptive commit messages:

```bash
# Good
git commit -m "Add MySQL connector for customer database"
git commit -m "Implement sales aggregation transformer"
git commit -m "Add PII detection rules for customer data"

# Bad
git commit -m "Update files"
git commit -m "Fix stuff"
git commit -m "WIP"
```

---

## License

Your project built on this framework inherits the Gorigami proprietary license. Contact <develop@gorigami.com> for licensing questions.
