# Gorigami Data Framework - Quick Start Guide

This guide will help you get the Gorigami Data Framework up and running quickly.

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB of RAM available
- Python 3.11+ (for local development)

## Step 1: Initial Setup

1. **Clone or copy this template** to your project directory

2. **Configure environment variables**:

   ```bash
   cp .env.example .env
   # Edit .env with your specific configuration
   ```

3. **Review the Docker Compose configuration**:
   - PostgreSQL (port 5432)
   - Airflow (port 8080)
   - Spark (port 8081)
   - Metabase (port 3000)

## Step 2: Start the Framework

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

## Step 3: Initialize Airflow

```bash
# Initialize Airflow database (first time only)
docker-compose exec airflow-webserver airflow db init

# Create admin user
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@gorigami.com \
    --password admin
```

## Step 4: Access the Services

- **Airflow UI**: <http://localhost:8080> (admin/admin)
- **Spark Master UI**: <http://localhost:8081>
- **Metabase**: <http://localhost:3000>
- **PostgreSQL**: localhost:5432 (gorigami/gorigami_password)

## Step 5: Verify Installation

1. **Check Airflow**:
   - Navigate to <http://localhost:8080>
   - Login with admin/admin
   - You should see the example_pipeline DAG

2. **Check PostgreSQL**:

   ```bash
   docker-compose exec postgres psql -U gorigami -d gorigami_analytics -c "\dt curated.*"
   ```

3. **Check Spark**:
   - Navigate to <http://localhost:8081>
   - Verify master and worker are running

## Step 6: Run Your First Pipeline

1. **Enable the example DAG** in Airflow UI
2. **Trigger a manual run**
3. **Monitor execution** in the Graph view
4. **Check logs** for each task

## Next Steps

### Configure Data Sources

Edit your extractors in `ingestion/extractors/` to connect to your actual data sources:

```python
# Example: Configure API extractor
from ingestion.extractors.api_extractor import APIExtractor

extractor = APIExtractor(
    base_url=os.getenv('API_BASE_URL'),
    api_key=os.getenv('API_KEY')
)
```

### Create Your First Pipeline

1. **Copy the example DAG**: `airflow/dags/example_pipeline.py`
2. **Customize for your use case**
3. **Add your ingestion logic**
4. **Define transformations**
5. **Add quality checks**

### Set Up Metabase

1. Navigate to <http://localhost:3000>
2. Complete initial setup
3. Connect to PostgreSQL:
   - Host: `postgres`
   - Port: `5432`
   - Database: `gorigami_analytics`
   - Username: `gorigami`
   - Password: `gorigami_password`
4. Create your first dashboard

## Common Commands

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Restart a specific service
docker-compose restart airflow-scheduler

# View logs for a specific service
docker-compose logs -f postgres

# Execute commands in a container
docker-compose exec airflow-webserver bash
```

## Troubleshooting

### Airflow won't start

- Check if port 8080 is already in use
- Ensure database initialization completed
- Check logs: `docker-compose logs airflow-webserver`

### Spark jobs fail

- Verify Spark master is running
- Check worker resources (memory/cores)
- Review job logs in Spark UI

### PostgreSQL connection issues

- Verify container is running: `docker-compose ps postgres`
- Check credentials in .env file
- Ensure port 5432 is not blocked

## Development Workflow

1. **Develop locally** - Write and test code on your machine
2. **Test in Docker** - Run pipelines in containerized environment
3. **Validate quality** - Ensure all checks pass
4. **Deploy to production** - Use the same Docker Compose setup

## Support

For issues or questions, contact your Gorigami representative.
