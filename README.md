# Gorigami Data Framework

A comprehensive, modular data engineering framework for building reliable, scalable data pipelines from ingestion through transformation to analytics-ready data.

**Repository**: <https://github.com/gorigamidev/gorigamiDataFrame>

---

## Overview

The Gorigami Data Framework provides a complete solution for modern data engineering with five integrated layers:

- **Ingestion Layer** - Connectors + Extractors with schema validation
- **Transformation Layer** - PySpark with reusable transformation functions
- **Quality Layer** - Source profiling and pipeline validation
- **Orchestration Layer** - Custom Airflow operators and task factories
- **Data Catalog** - API for discovery, metadata, and governance

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────────┐
│                     Gorigami Data Framework                       │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Sources → Connectors → Extractors → Bronze (Parquet)            │
│              ↓             ↓             ↓                        │
│          Validate      Profile      Catalog                      │
│                                         ↓                         │
│                                    Transformers                   │
│                                         ↓                         │
│                                    Silver (PostgreSQL)            │
│                                         ↓                         │
│                                    Validate & Catalog             │
│                                         ↓                         │
│                                    Analytics & BI                 │
│                                                                   │
├──────────────────────────────────────────────────────────────────┤
│  Orchestration: Airflow DAGs with custom operators               │
│  Governance: Data Catalog API with lineage & access control      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Clone and Setup

```bash
# Clone repository
git clone https://github.com/gorigamidev/gorigamiDataFrame.git
cd gorigamiDataFrame

# Create your project branch
git checkout -b my-project-name

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your settings
```

### 2. Initialize Infrastructure

```bash
# Start Docker services
docker-compose up -d

# Initialize database schemas
psql -h localhost -U gorigami -d gorigami_analytics < storage/sql/init_schema.sql
psql -h localhost -U gorigami -d gorigami_analytics < storage/sql/silver_schema.sql
psql -h localhost -U gorigami -d gorigami_analytics < storage/sql/quality_metadata_schema.sql
psql -h localhost -U gorigami -d gorigami_analytics < storage/sql/catalog_schema.sql
```

### 3. Run Examples

```bash
# File ingestion example
python examples/file_ingestion_example.py

# Transformation example
python examples/sales_transformer_example.py

# Quality validation example
python examples/data_quality_example.py
```

### 4. Start Catalog API

```bash
uvicorn catalog.api.catalog_api:app --reload --port 8000
# Access docs at http://localhost:8000/docs
```

---

## Framework Layers

### 1. Ingestion Layer (Bronze)

Extract data from sources to bronze Parquet layer with full traceability.

**Components**:

- **Connectors** - Validate connections and entity existence
- **Extractors** - Extract data with schema application

**Supported**: CSV, Parquet, XLSX, JSON

**Documentation**: [ingestion/README.md](ingestion/README.md)

### 2. Transformation Layer (Silver)

Transform bronze data to curated silver PostgreSQL layer.

**Components**:

- **BaseTransformer** - PySpark transformation orchestration
- **12 Common Transformations** - Clean, deduplicate, enrich, aggregate

**Documentation**: [processing/README.md](processing/README.md)

### 3. Quality Layer

Ensure data quality through profiling and validation.

**Components**:

- **Source Profiler** - Audit with quality scoring
- **Pipeline Validator** - Rule-based validation with quality gates

**Documentation**: [quality/README.md](quality/README.md)

### 4. Orchestration Layer

Orchestrate pipelines with Airflow.

**Components**:

- **3 Custom Operators** - Ingestion, Transformation, Validation
- **Pipeline Factory** - Simplified DAG creation

**Documentation**: [airflow/README.md](airflow/README.md)

### 5. Data Catalog

Centralized discovery, metadata, and governance.

**Components**:

- **Catalog API** - FastAPI REST endpoints
- **Auto-Registration** - From extractors/transformers

**Documentation**: [catalog/README.md](catalog/README.md)

---

## Key Features

### ✅ Complete Traceability

- Bronze metadata: `_extraction_timestamp`, `_source_system`, `_source_entity`
- Silver metadata: `_silver_load_timestamp`, `_bronze_source_path`, `_transformer_name`

### ✅ Quality Gates

- Bronze validation blocks transformation if data is poor
- Silver validation ensures only quality data reaches consumers
- Configurable rules in YAML

### ✅ Data Lineage

- Track upstream sources and downstream consumers
- Transformation tracking
- Lineage visualization via catalog API

### ✅ Governance

- Access control policies
- PII detection and tagging
- Compliance tracking
- Audit logging

---

## Technology Stack

- **Python 3.9+** - Core language
- **PySpark 3.5** - Data processing
- **PostgreSQL** - Metadata and silver layer
- **Apache Airflow 2.8** - Orchestration
- **FastAPI** - Catalog API
- **Docker** - Infrastructure

---

## Project Structure

```text
gorigamiDataFrame/
├── ingestion/          # Connectors + extractors
├── processing/         # Transformers + transformations
├── quality/            # Profiling + validation
├── catalog/            # Data catalog API
├── airflow/            # DAGs + operators
├── storage/sql/        # Database schemas
├── config/             # Configurations
├── examples/           # Usage examples
└── tests/              # Tests
```

---

## Documentation

- **[Quick Start Guide](QUICKSTART.md)** - Detailed setup
- **[Contributing Guide](CONTRIBUTING.md)** - Development workflow
- **[Ingestion Layer](ingestion/README.md)** - Connectors and extractors
- **[Transformation Layer](processing/README.md)** - PySpark transformers
- **[Quality Layer](quality/README.md)** - Profiling and validation
- **[Orchestration](airflow/README.md)** - Airflow operators
- **[Data Catalog](catalog/README.md)** - Discovery and governance

---

## Development Workflow

This framework is designed as a **template** for building data projects:

1. **Clone** the repository
2. **Create a project branch** with your project name
3. **Customize** for your use case:
   - Add connectors for your sources
   - Create transformers for your business logic
   - Define quality rules for your data
   - Build DAGs for your pipelines
4. **Extend** as needed - The framework is modular

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

---

## License

Proprietary - Gorigami

This framework is provided to authorized users only for their internal business operations.

For licensing inquiries: <develop@gorigami.com>

---

## Support

For questions or issues, contact the Gorigami data team.

---

Built with modern data engineering best practices for scalability, reliability, and maintainability.
