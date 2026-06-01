FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY . .

RUN mkdir -p backups data/csv data/bronze

EXPOSE 8000

CMD ["uvicorn", "catalog.api.catalog_api:app", "--host", "0.0.0.0", "--port", "8000"]
