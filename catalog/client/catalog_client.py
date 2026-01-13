"""
Catalog Client
Client for interacting with the data catalog API.
Used for auto-registration from extractors and transformers.
"""

import requests
from typing import Dict, Any, Optional, List
from loguru import logger


class CatalogClient:
    """
    Client for data catalog API.
    
    Provides methods for registering datasets, updating metadata,
    and tracking lineage.
    """

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def register_dataset(
        self,
        dataset_name: str,
        layer: str,
        location: str,
        format: Optional[str] = None,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        row_count: Optional[int] = None,
        size_mb: Optional[float] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        schema: Optional[Dict[str, Any]] = None,
        source_dataset_ids: Optional[List[int]] = None,
        transformation_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Register a dataset in the catalog.

        Args:
            dataset_name: Dataset name
            layer: Data layer (bronze, silver, gold)
            location: Dataset location (path or table name)
            format: Data format (parquet, table)
            description: Dataset description
            owner: Dataset owner
            row_count: Number of rows
            size_mb: Size in MB
            tags: List of tags
            metadata: Additional metadata
            schema: Dataset schema
            source_dataset_ids: Source dataset IDs for lineage
            transformation_name: Transformation name for lineage

        Returns:
            Created dataset
        """
        try:
            # Create dataset
            payload = {
                "dataset_name": dataset_name,
                "layer": layer,
                "location": location,
                "format": format,
                "description": description,
                "owner": owner,
                "row_count": row_count,
                "size_mb": size_mb,
                "tags": tags or [],
                "metadata": metadata or {}
            }
            
            response = self.session.post(
                f"{self.base_url}/api/catalog/datasets",
                json=payload
            )
            
            if response.status_code == 201:
                dataset = response.json()
                logger.info(f"Registered dataset: {dataset_name} (ID: {dataset['dataset_id']})")
                
                # Register schema if provided
                if schema:
                    self.register_schema(dataset['dataset_id'], schema)
                
                # Register lineage if provided
                if source_dataset_ids:
                    for source_id in source_dataset_ids:
                        self.register_lineage(
                            dataset['dataset_id'],
                            source_id,
                            transformation_name
                        )
                
                return dataset
            else:
                logger.error(f"Failed to register dataset: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error registering dataset: {e}")
            return None

    def register_schema(
        self,
        dataset_id: int,
        schema_definition: Dict[str, Any],
        schema_version: str = "1.0"
    ) -> bool:
        """
        Register dataset schema.

        Args:
            dataset_id: Dataset ID
            schema_definition: Schema definition
            schema_version: Schema version

        Returns:
            Success status
        """
        try:
            # Note: This endpoint needs to be added to the API
            logger.info(f"Schema registration for dataset {dataset_id} (placeholder)")
            return True
        except Exception as e:
            logger.error(f"Error registering schema: {e}")
            return False

    def register_lineage(
        self,
        dataset_id: int,
        source_dataset_id: int,
        transformation_name: Optional[str] = None,
        transformation_type: Optional[str] = None
    ) -> bool:
        """
        Register dataset lineage.

        Args:
            dataset_id: Target dataset ID
            source_dataset_id: Source dataset ID
            transformation_name: Transformation name
            transformation_type: Transformation type

        Returns:
            Success status
        """
        try:
            # Note: This endpoint needs to be added to the API
            logger.info(
                f"Lineage registration: {source_dataset_id} → {dataset_id} "
                f"via {transformation_name}"
            )
            return True
        except Exception as e:
            logger.error(f"Error registering lineage: {e}")
            return False

    def get_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get dataset by ID"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/catalog/datasets/{dataset_id}"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            logger.error(f"Error getting dataset: {e}")
            return None

    def search_datasets(
        self,
        query: str,
        layer: Optional[str] = None,
        tags: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Search datasets"""
        try:
            params = {"q": query, "limit": limit}
            
            if layer:
                params["layer"] = layer
            if tags:
                params["tags"] = tags
            
            response = self.session.get(
                f"{self.base_url}/api/catalog/search",
                params=params
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return []
        except Exception as e:
            logger.error(f"Error searching datasets: {e}")
            return []

    def get_lineage(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get dataset lineage"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/catalog/datasets/{dataset_id}/lineage"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            logger.error(f"Error getting lineage: {e}")
            return None


# Global catalog client instance
catalog_client = CatalogClient()
