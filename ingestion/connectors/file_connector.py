"""
File Connector
Validates file existence and accessibility for CSV, Parquet, and XLSX files.
"""

from pathlib import Path
from typing import Dict, Any, Optional
import os
from datetime import datetime
from loguru import logger

from .base_connector import BaseConnector, ConnectionValidationError, ConfigurationError


class FileConnector(BaseConnector):
    """
    Connector for file-based data sources (CSV, Parquet, XLSX).
    
    Validates file existence, permissions, and format without reading data.
    Supports local filesystem and cloud storage (S3, GCS, Azure).
    """

    SUPPORTED_FORMATS = ['csv', 'parquet', 'xlsx', 'json', 'jsonl']

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize file connector.

        Args:
            config: Configuration dictionary with:
                - path: File path (local or cloud URL)
                - storage_type: 'local', 's3', 'gcs', or 'azure' (default: 'local')
                - expected_format: Expected file format (optional)
                - credentials: Cloud storage credentials (if needed)
        """
        super().__init__(config)
        self.path = Path(self.config['path']) if self.config.get('storage_type', 'local') == 'local' else self.config['path']
        self.storage_type = self.config.get('storage_type', 'local')
        self.expected_format = self.config.get('expected_format')

    def _validate_config(self) -> None:
        """Validate configuration"""
        self._require_config('path')
        
        if self.config.get('expected_format') and \
           self.config['expected_format'] not in self.SUPPORTED_FORMATS:
            raise ConfigurationError(
                f"Unsupported format: {self.config['expected_format']}. "
                f"Supported: {self.SUPPORTED_FORMATS}"
            )

    def validate(self) -> bool:
        """
        Validate file exists and is readable.

        Returns:
            True if validation succeeds

        Raises:
            ConnectionValidationError: If file doesn't exist or isn't readable
        """
        if self.storage_type == 'local':
            return self._validate_local_file()
        else:
            return self._validate_cloud_file()

    def _validate_local_file(self) -> bool:
        """Validate local file"""
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        
        # Check existence
        if not self.path.exists():
            raise ConnectionValidationError(f"File not found: {self.path}")
        
        # Check if it's a file (not directory)
        if not self.path.is_file():
            raise ConnectionValidationError(f"Path is not a file: {self.path}")
        
        # Check readability
        if not os.access(self.path, os.R_OK):
            raise ConnectionValidationError(f"File not readable: {self.path}")
        
        # Validate format if specified
        if self.expected_format:
            actual_format = self._detect_format()
            if actual_format != self.expected_format:
                raise ConnectionValidationError(
                    f"Format mismatch: expected {self.expected_format}, "
                    f"got {actual_format}"
                )
        
        self._connected = True
        logger.info(f"File validation successful: {self.path}")
        return True

    def _validate_cloud_file(self) -> bool:
        """Validate cloud storage file"""
        # Placeholder for cloud storage validation
        # In production, implement using boto3 (S3), google-cloud-storage (GCS), etc.
        logger.warning(f"Cloud storage validation not yet implemented for {self.storage_type}")
        raise NotImplementedError(f"Cloud storage validation for {self.storage_type} not implemented")

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get file metadata without reading contents.

        Returns:
            Dictionary with file metadata
        """
        if self.storage_type == 'local':
            return self._get_local_metadata()
        else:
            return self._get_cloud_metadata()

    def _get_local_metadata(self) -> Dict[str, Any]:
        """Get local file metadata"""
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        
        stat = self.path.stat()
        
        metadata = {
            'path': str(self.path.absolute()),
            'filename': self.path.name,
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'modified_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'created_at': datetime.fromtimestamp(stat.st_ctime).isoformat(),
            'format': self._detect_format(),
            'exists': True,
            'readable': os.access(self.path, os.R_OK),
            'writable': os.access(self.path, os.W_OK),
            'storage_type': 'local'
        }
        
        logger.debug(f"File metadata: {metadata}")
        return metadata

    def _get_cloud_metadata(self) -> Dict[str, Any]:
        """Get cloud storage file metadata"""
        raise NotImplementedError(f"Cloud storage metadata for {self.storage_type} not implemented")

    def _detect_format(self) -> str:
        """
        Detect file format from extension.

        Returns:
            File format (csv, parquet, xlsx, etc.)
        """
        if isinstance(self.path, Path):
            suffix = self.path.suffix.lower().lstrip('.')
        else:
            suffix = Path(self.path).suffix.lower().lstrip('.')
        
        # Map extensions to formats
        format_map = {
            'csv': 'csv',
            'tsv': 'csv',
            'parquet': 'parquet',
            'pq': 'parquet',
            'xlsx': 'xlsx',
            'xls': 'xlsx',
            'json': 'json',
            'jsonl': 'jsonl',
            'ndjson': 'jsonl'
        }
        
        return format_map.get(suffix, suffix)

    def file_exists(self) -> bool:
        """
        Check if file exists (convenience method).

        Returns:
            True if file exists
        """
        try:
            self.validate()
            return True
        except ConnectionValidationError:
            return False
