FROM python:3.11-slim

# Install Java (required by PySpark) and build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        gcc \
        libpq-dev && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p backups data/csv

EXPOSE 8000

# Use $PORT if set (Railway injects it); fall back to 8000 for local Docker.
# --proxy-headers + --forwarded-allow-ips: required when running behind Railway's
# TLS-terminating reverse proxy so Swagger UI and redirect URLs use HTTPS correctly.
CMD ["sh", "-c", "uvicorn catalog.api.catalog_api:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips '*'"]
