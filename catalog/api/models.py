"""
Pydantic Models for Data Catalog API
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class LayerEnum(str, Enum):
    """Data layer enumeration"""
    bronze = "bronze"
    silver = "silver"
    gold = "gold"


class FormatEnum(str, Enum):
    """Data format enumeration"""
    parquet = "parquet"
    table = "table"
    view = "view"


class PolicyTypeEnum(str, Enum):
    """Access policy type enumeration"""
    public = "public"
    restricted = "restricted"
    private = "private"


class Dataset(BaseModel):
    """Dataset model"""
    dataset_id: Optional[int] = None
    dataset_name: str
    layer: LayerEnum
    format: Optional[FormatEnum] = None
    location: str
    description: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    access_count: int = 0
    row_count: Optional[int] = None
    size_mb: Optional[float] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        from_attributes = True


class DatasetCreate(BaseModel):
    """Dataset creation model"""
    dataset_name: str
    layer: LayerEnum
    format: Optional[FormatEnum] = None
    location: str
    description: Optional[str] = None
    owner: Optional[str] = None
    row_count: Optional[int] = None
    size_mb: Optional[float] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DatasetSchema(BaseModel):
    """Dataset schema model"""
    schema_id: Optional[int] = None
    dataset_id: int
    schema_version: str
    schema_definition: Dict[str, Any]
    is_current: bool = True
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class DatasetLineage(BaseModel):
    """Dataset lineage model"""
    lineage_id: Optional[int] = None
    dataset_id: int
    source_dataset_id: Optional[int] = None
    transformation_name: Optional[str] = None
    transformation_type: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class LineageGraph(BaseModel):
    """Lineage graph model"""
    dataset: Dataset
    upstream: List[Dict[str, Any]] = Field(default_factory=list)
    downstream: List[Dict[str, Any]] = Field(default_factory=list)


class AccessPolicy(BaseModel):
    """Access policy model"""
    policy_id: Optional[int] = None
    dataset_id: int
    policy_type: PolicyTypeEnum
    allowed_roles: List[str] = Field(default_factory=list)
    allowed_users: List[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class GovernanceTag(BaseModel):
    """Governance tag model"""
    tag_id: Optional[int] = None
    dataset_id: int
    tag_type: str
    tag_value: Optional[str] = None
    column_name: Optional[str] = None
    confidence: Optional[float] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class DatasetStatistics(BaseModel):
    """Dataset statistics model"""
    stat_id: Optional[int] = None
    dataset_id: int
    stat_timestamp: Optional[datetime] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    null_percentage: Optional[float] = None
    duplicate_percentage: Optional[float] = None
    quality_score: Optional[float] = None
    statistics_json: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class SearchResult(BaseModel):
    """Search result model"""
    dataset: Dataset
    relevance_score: float
    matched_fields: List[str] = Field(default_factory=list)


class PaginatedResponse(BaseModel):
    """Paginated response model"""
    total: int
    page: int
    page_size: int
    items: List[Any]
