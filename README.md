# Gorigami Data Framework

**Gorigami Data Framework** is a modular, end-to-end data framework designed to **ingest, process, validate, and expose data in a reliable and scalable way**, using open-source technologies and reproducible engineering practices.

The framework is built to run consistently **locally, on-premise, or in the cloud**, and can be adopted **incrementally**, based on an organization’s data maturity.

---

## 🎯 Purpose

The goal of the Gorigami Data Framework is to help organizations move from raw data to **trusted analytics and decision-ready information**, without locking themselves into proprietary platforms.

It enables teams to:

- Ingest data from multiple sources
- Orchestrate and automate data pipelines
- Transform data at scale
- Enforce data quality checks
- Serve curated datasets for analytics, reporting, and machine learning
- Grow toward advanced analytics and Data Science over time

This framework focuses on **operational data reliability**, not just dashboards.

---

## 🧠 Core Principles

- **Modularity**  
  Each capability can be implemented independently and combined progressively.

- **Data as Code**  
  Pipelines, transformations, and quality rules are versioned and auditable.

- **Clear Separation of Responsibilities**  
  Orchestration, processing, validation, and consumption are intentionally decoupled.

- **Portability First**  
  The same architecture runs in local development, on-premise environments, or cloud infrastructure.

- **Open-Source by Design**  
  No vendor lock-in. All core components are replaceable.

---

## 🏗️ High-Level Architecture

```text

[ Data Sources ]
        | 
        v
[ Data Ingestion ]
(Python / Spark Jobs)
        | 
        v
[ Orchestration ]
( Airflow )
        | 
        v
[ Data Processing ]
( Spark / PySpark )
        | 
        v
[ Data Quality ]
( Checks & Validations )
        | 
        v
[ Data Storage & Serving ]
( PostgreSQL )
        | 
        v
[ Reporting / Analytics / ML ]

```

---

## 📦 Framework Modules

### 1. Data Ingestion

Responsible for extracting data from external systems and bringing it into the platform.

#### **Capabilities Data Ingestion**

- API ingestion
- Database extraction
- File-based ingestion
- Incremental loads
- Execution logging and retries

Ingestion can be implemented using lightweight Python extractors or Spark jobs when volume requires it.

---

### 2. Orchestration

Pipeline execution and dependency management are handled centrally.

#### **Capabilities Orchestration**

- Scheduling and automation
- Dependency management
- Retries and failure handling
- Operational alerting

The orchestration layer ensures that data pipelines run in a controlled and observable manner.

---

### 3. Data Processing

This layer is responsible for transforming raw data into curated, analytics-ready datasets.

#### **Capabilities Data Processing**

- Batch transformations
- Data normalization and enrichment
- Feature engineering
- Layered data models (raw → curated → consumption)

Processing is designed for scalability and reproducibility.

---

### 4. Data Quality

Before data is exposed for consumption, it must pass explicit quality validations.

#### **Capabilities Data Quality**

- Schema and null checks
- Volume and freshness validation
- Business rule validation
- Fail-fast pipeline behavior

Data that does not meet quality expectations is not published.

---

### 5. Data Storage & Serving

Curated data is stored in a serving layer optimized for analytics and consumption.

#### **Capabilities Data Storage & Serving**

- Analytics-ready tables
- Semantic views
- Controlled access for consumers

This layer acts as the single source of truth for reporting and analytics.

---

### 6. Reporting & Dashboards

This module enables human-friendly access to data through web-based reporting tools.

#### **Capabilities Reporting & Dashboards**

- Interactive dashboards
- KPI visualization
- Web access via browser
- Report sharing via URLs

Reporting tools connect exclusively to curated and validated datasets.

---

### 7. Advanced Analytics & Data Science (Optional)

The framework is designed to support advanced analytics and machine learning workflows.

#### **Capabilities Advanced Analytics & Data Science**

- Feature datasets for ML
- Integration with notebooks
- Model training pipelines
- Reproducible experimentation

This module builds on top of the same trusted data foundations.

---

### 8. Reporting & BI / Business Analytics

The Reporting & BI module provides a **human-friendly consumption layer** for the curated and validated data produced by the framework.

Its purpose is to enable business users, analysts, and decision-makers to **explore, visualize, and share insights** without interacting directly with data pipelines or processing logic.

#### **Capabilities Reporting & BI / BA**

- Interactive dashboards and KPI visualization
- Web-based access (browser-only, no client installation)
- Report and dashboard sharing via URLs
- Filterable and drill-down analytics
- SQL access for advanced users (optional)

This module connects **exclusively** to the curated data storage and serving layer, ensuring consistency and trust in reported metrics.

#### **Default Implementation**

The framework uses **Metabase** as the default reporting and BI tool:

- Open-source and self-hosted
- Fully containerized (Docker-based)
- Cross-platform (Mac, Windows, Linux)
- Direct integration with PostgreSQL
- Designed for rapid adoption by non-technical users

Metabase is deployed as a standalone container and operates as a **pure consumption layer**, with no responsibility for data transformation or business logic.

This separation ensures that reporting remains simple, scalable, and aligned with the single source of truth defined by the framework.

---

## ⚙️ Technology Stack (Core)

The framework uses a small, focused set of open-source technologies:

- **Apache Airflow** – Pipeline orchestration
- **Apache Spark (PySpark)** – Distributed data processing
- **PostgreSQL** – Analytics serving layer
- **Python** – Ingestion, validation, and glue logic
- **Docker** – Execution and portability

All components are containerized and designed to run together using Docker Compose.

---

## 🧩 Repository Structure (Suggested)

```text
gorigami-data-framework-ProjectName/
├── airflow/
│   └── dags/
├── ingestion/
│   └── extractors/
├── processing/
│   └── spark_jobs/
├── quality/
│   └── checks/
├── storage/
│   └── sql/
├── reporting/
├── docker-compose.yml
└── README.md
└── LICENSE
└── requirements.txt
```

---

## 🚀 Getting Started

This repository is intended as a **framework template**, not a one-click product.

Typical usage:

1. Clone the repository
2. Configure data sources
3. Define ingestion and transformation jobs
4. Add quality rules
5. Expose curated datasets for reporting or analytics

Each organization can extend or adapt modules as needed.

---

## 🧭 When to Use This Framework

This framework is a good fit when:

- You want control over your data pipelines
- You need portability across environments
- You prefer open-source and transparency
- You want to scale from reporting to Data Science without re-architecting

---

## 📌 Final Note

The Gorigami Data Framework is intentionally **opinionated but flexible**.

It does not try to solve everything at once.  
It provides a solid, reproducible foundation on top of which analytics, reporting, and machine learning can grow safely.

---
