"""
Data Catalog FastAPI Application
Main API application for data discovery, metadata, and governance.
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from loguru import logger

# For the browser-facing URLs, prefer the explicit *_PUBLIC_URL vars.
# Fall back to the plain AIRFLOW_URL / METABASE_URL so a single env var
# covers both internal (backend) and public (browser) access on simple setups.
AIRFLOW_PUBLIC_URL  = os.getenv("AIRFLOW_PUBLIC_URL",  os.getenv("AIRFLOW_URL",  "http://localhost:8080"))
METABASE_PUBLIC_URL = os.getenv("METABASE_PUBLIC_URL", os.getenv("METABASE_URL", "http://localhost:3000"))

from catalog.api.models import (
    Dataset, DatasetCreate, DatasetSchema, LineageGraph,
    AccessPolicy, GovernanceTag, SearchResult, PaginatedResponse,
    LayerEnum
)


# FastAPI app
app = FastAPI(
    title="Globant Data Platform API",
    description="Data ingestion, pipeline control, analytics, and governance API",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi.responses import HTMLResponse
from catalog.api.routes.ingest import router as ingest_router

@app.on_event("startup")
def init_db_schema():
    schema_path = os.path.join(os.path.dirname(__file__), "../../storage/sql/globant_schema.sql")
    host = os.getenv("POSTGRES_HOST", "").strip()
    if not host:
        print("⚠ DB schema init skipped: POSTGRES_HOST not set")
        return
    try:
        conn = psycopg2.connect(
            host=host,
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "globant_analytics"),
            user=os.getenv("POSTGRES_USER", "globant"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        )
        conn.autocommit = True
        cur = conn.cursor()
        with open(schema_path, "r") as f:
            cur.execute(f.read())
        cur.close()
        conn.close()
        print("✓ DB schema initialized")
    except Exception as e:
        print(f"⚠ DB schema init warning: {e}")
from catalog.api.routes.backup import router as backup_router
from catalog.api.routes.restore import router as restore_router
from catalog.api.routes.metrics import router as metrics_router
from catalog.api.routes.dashboard import router as dashboard_router
from catalog.api.routes.airflow_proxy import router as pipeline_router
from catalog.api.routes.admin import router as admin_router

app.include_router(ingest_router)
app.include_router(backup_router)
app.include_router(restore_router)
app.include_router(metrics_router)
app.include_router(dashboard_router)
app.include_router(pipeline_router)
app.include_router(admin_router)


from catalog.api.db import get_db


# ============================================================================
# Dataset Endpoints
# ============================================================================

@app.get("/api/catalog/datasets", response_model=PaginatedResponse)
def list_datasets(
    layer: Optional[LayerEnum] = None,
    owner: Optional[str] = None,
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db=Depends(get_db)
):
    """
    List all datasets with optional filtering.
    
    Args:
        layer: Filter by layer (bronze, silver, gold)
        owner: Filter by owner
        tags: Filter by tags (comma-separated)
        page: Page number
        page_size: Items per page
    """
    cursor = db.cursor()
    
    # Build query
    query = "SELECT * FROM catalog.datasets WHERE 1=1"
    params = []
    
    if layer:
        query += " AND layer = %s"
        params.append(layer.value)
    
    if owner:
        query += " AND owner = %s"
        params.append(owner)
    
    if tags:
        tag_list = [t.strip() for t in tags.split(',')]
        query += " AND tags ?| %s"
        params.append(tag_list)
    
    # Get total count
    count_query = f"SELECT COUNT(*) as total FROM ({query}) as subq"
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # Add pagination
    offset = (page - 1) * page_size
    query += " ORDER BY updated_at DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])
    
    # Execute query
    cursor.execute(query, params)
    datasets = cursor.fetchall()
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": datasets
    }


@app.get("/api/catalog/datasets/{dataset_id}", response_model=Dataset)
def get_dataset(dataset_id: int, db=Depends(get_db)):
    """Get dataset by ID"""
    cursor = db.cursor()
    
    cursor.execute(
        "SELECT * FROM catalog.datasets WHERE dataset_id = %s",
        (dataset_id,)
    )
    
    dataset = cursor.fetchone()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Log access
    cursor.execute(
        """
        UPDATE catalog.datasets 
        SET last_accessed_at = CURRENT_TIMESTAMP, access_count = access_count + 1
        WHERE dataset_id = %s
        """,
        (dataset_id,)
    )
    db.commit()
    
    return dataset


@app.post("/api/catalog/datasets", response_model=Dataset, status_code=201)
def create_dataset(dataset: DatasetCreate, db=Depends(get_db)):
    """Create a new dataset"""
    cursor = db.cursor()
    
    cursor.execute(
        """
        INSERT INTO catalog.datasets 
        (dataset_name, layer, format, location, description, owner, row_count, size_mb, tags, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (
            dataset.dataset_name,
            dataset.layer.value,
            dataset.format.value if dataset.format else None,
            dataset.location,
            dataset.description,
            dataset.owner,
            dataset.row_count,
            dataset.size_mb,
            dataset.tags,
            dataset.metadata
        )
    )
    
    new_dataset = cursor.fetchone()
    db.commit()
    
    logger.info(f"Created dataset: {dataset.dataset_name}")
    
    return new_dataset


# ============================================================================
# Search Endpoints
# ============================================================================

@app.get("/api/catalog/search", response_model=List[SearchResult])
def search_datasets(
    q: str = Query(..., description="Search query"),
    layer: Optional[LayerEnum] = None,
    tags: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db)
):
    """
    Search datasets by name, description, or tags.
    
    Args:
        q: Search query
        layer: Filter by layer
        tags: Filter by tags (comma-separated)
        limit: Maximum results
    """
    cursor = db.cursor()
    
    # Simple text search (can be enhanced with PostgreSQL full-text search)
    query = """
        SELECT *, 
               CASE 
                   WHEN dataset_name ILIKE %s THEN 1.0
                   WHEN description ILIKE %s THEN 0.8
                   ELSE 0.5
               END as relevance_score
        FROM catalog.datasets
        WHERE (dataset_name ILIKE %s OR description ILIKE %s OR tags::text ILIKE %s)
    """
    
    search_pattern = f"%{q}%"
    params = [search_pattern] * 5
    
    if layer:
        query += " AND layer = %s"
        params.append(layer.value)
    
    if tags:
        tag_list = [t.strip() for t in tags.split(',')]
        query += " AND tags ?| %s"
        params.append(tag_list)
    
    query += " ORDER BY relevance_score DESC, updated_at DESC LIMIT %s"
    params.append(limit)
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    return [
        {
            "dataset": {k: v for k, v in r.items() if k != 'relevance_score'},
            "relevance_score": r['relevance_score'],
            "matched_fields": []  # Can be enhanced
        }
        for r in results
    ]


# ============================================================================
# Lineage Endpoints
# ============================================================================

@app.get("/api/catalog/datasets/{dataset_id}/lineage", response_model=LineageGraph)
def get_dataset_lineage(dataset_id: int, db=Depends(get_db)):
    """Get dataset lineage (upstream and downstream)"""
    cursor = db.cursor()
    
    # Get dataset
    cursor.execute("SELECT * FROM catalog.datasets WHERE dataset_id = %s", (dataset_id,))
    dataset = cursor.fetchone()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get upstream (sources)
    cursor.execute(
        """
        SELECT d.*, l.transformation_name, l.transformation_type
        FROM catalog.dataset_lineage l
        JOIN catalog.datasets d ON l.source_dataset_id = d.dataset_id
        WHERE l.dataset_id = %s
        """,
        (dataset_id,)
    )
    upstream = cursor.fetchall()
    
    # Get downstream (consumers)
    cursor.execute(
        """
        SELECT d.*, l.transformation_name, l.transformation_type
        FROM catalog.dataset_lineage l
        JOIN catalog.datasets d ON l.dataset_id = d.dataset_id
        WHERE l.source_dataset_id = %s
        """,
        (dataset_id,)
    )
    downstream = cursor.fetchall()
    
    return {
        "dataset": dataset,
        "upstream": upstream,
        "downstream": downstream
    }


# ============================================================================
# Governance Endpoints
# ============================================================================

@app.get("/api/catalog/datasets/{dataset_id}/governance", response_model=List[GovernanceTag])
def get_dataset_governance_tags(dataset_id: int, db=Depends(get_db)):
    """Get governance tags for a dataset"""
    cursor = db.cursor()
    
    cursor.execute(
        "SELECT * FROM catalog.governance_tags WHERE dataset_id = %s",
        (dataset_id,)
    )
    
    tags = cursor.fetchall()
    return tags


@app.get("/api/catalog/governance/pii", response_model=List[Dataset])
def list_pii_datasets(db=Depends(get_db)):
    """List all datasets with PII tags"""
    cursor = db.cursor()
    
    cursor.execute(
        """
        SELECT DISTINCT d.*
        FROM catalog.datasets d
        JOIN catalog.governance_tags g ON d.dataset_id = g.dataset_id
        WHERE g.tag_type = 'pii'
        ORDER BY d.updated_at DESC
        """
    )
    
    datasets = cursor.fetchall()
    return datasets


# ============================================================================
# Health Check
# ============================================================================

@app.get("/api/v1/config")
def get_frontend_config():
    """Public URLs the browser needs to link to Airflow and Metabase directly."""
    return {
        "airflow_url":  AIRFLOW_PUBLIC_URL,
        "metabase_url": METABASE_PUBLIC_URL,
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "catalog-api"}


# Root endpoint serving static minimalist UI
@app.get("/", response_class=HTMLResponse)
def root():
    """Serve Upload Portal UI"""
    static_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(static_path):
        with open(static_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Globant Data Migration Portal</h1><p>Static index.html not found.</p>")
