"""
Base Extractor Abstract Class
Defines the interface for all extractors in the Gorigami Data Framework.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import pandas as pd
from loguru import logger
import yaml


class ExtractorError(Exception):
    """Base exception for extractor errors"""
    pass


class SchemaValidationError(ExtractorError):
    """Raised when schema validation fails"""
    pass


class ExtractionResult:
    """
    Result of an extraction operation with metadata.
    """

    def __init__(
        self,
        success: bool,
        output_path: str,
        record_count: int,
        metadata: Dict[str, Any]
    ):
        self.success = success
        self.output_path = output_path
        self.record_count = record_count
        self.metadata = metadata
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            'success': self.success,
            'output_path': self.output_path,
            'record_count': self.record_count,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }

    def __repr__(self) -> str:
        return f"ExtractionResult(success={self.success}, records={self.record_count})"


class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.
    
    Extractors are responsible for:
    - Pulling data from sources using connectors
    - Applying or inferring schemas
    - Adding traceability metadata
    - Writing to bronze/raw layer
    - Tracking extraction metrics
    """

    def __init__(
        self,
        connector: 'BaseConnector',
        output_path: str,
        schema_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the extractor.

        Args:
            connector: Connector instance for source validation
            output_path: Path to bronze layer output directory
            schema_path: Optional path to schema definition file
            config: Optional extractor configuration
        """
        self.connector = connector
        self.output_path = Path(output_path)
        self.schema_path = schema_path
        self.config = config or {}
        self.schema = None
        
        if schema_path:
            self.schema = self._load_schema(schema_path)
        
        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized {self.__class__.__name__} extractor")

    def _load_schema(self, schema_path: str) -> Dict[str, Any]:
        """
        Load schema definition from YAML file.

        Args:
            schema_path: Path to schema YAML file

        Returns:
            Schema definition dictionary

        Raises:
            SchemaValidationError: If schema file is invalid
        """
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
            logger.info(f"Loaded schema from {schema_path}")
            return schema
        except FileNotFoundError:
            raise SchemaValidationError(f"Schema file not found: {schema_path}")
        except yaml.YAMLError as e:
            raise SchemaValidationError(f"Invalid schema YAML: {e}")

    @abstractmethod
    def extract(self) -> ExtractionResult:
        """
        Extract data from source and write to bronze layer.
        
        This method should:
        1. Validate connection using connector
        2. Extract data from source
        3. Apply or infer schema
        4. Add traceability metadata
        5. Write to bronze layer
        6. Return extraction result
        
        Returns:
            ExtractionResult with metadata
            
        Raises:
            ExtractorError: If extraction fails
        """
        pass

    @abstractmethod
    def extract_incremental(
        self,
        watermark_column: str,
        last_watermark: Any
    ) -> ExtractionResult:
        """
        Extract data incrementally based on watermark.

        Args:
            watermark_column: Column name for watermark (e.g., 'updated_at')
            last_watermark: Last successful watermark value

        Returns:
            ExtractionResult with metadata

        Raises:
            ExtractorError: If extraction fails
        """
        pass

    def _add_traceability_metadata(
        self,
        df: pd.DataFrame,
        source_info: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Add traceability metadata columns to DataFrame.

        Args:
            df: Source DataFrame
            source_info: Dictionary with source information

        Returns:
            DataFrame with added metadata columns
        """
        df = df.copy()
        
        # Add standard traceability columns
        df['_extraction_timestamp'] = datetime.now()
        df['_source_system'] = source_info.get('source_system', 'unknown')
        df['_source_entity'] = source_info.get('source_entity', 'unknown')
        df['_extractor_name'] = self.__class__.__name__
        df['_schema_version'] = source_info.get('schema_version', '1.0')
        
        # Add custom metadata if provided
        for key, value in source_info.items():
            if key.startswith('custom_'):
                df[f'_{key}'] = value
        
        logger.debug(f"Added traceability metadata: {list(df.columns)[-5:]}")
        return df

    def _apply_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply schema definition to DataFrame.

        Args:
            df: Source DataFrame

        Returns:
            DataFrame with schema applied

        Raises:
            SchemaValidationError: If schema validation fails
        """
        if not self.schema:
            logger.info("No schema defined, using inferred schema")
            return df
        
        # Validate required columns
        if 'columns' in self.schema:
            required_cols = [
                col['name'] for col in self.schema['columns']
                if col.get('required', False)
            ]
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise SchemaValidationError(f"Missing required columns: {missing_cols}")
        
        # Apply data types
        if 'columns' in self.schema:
            for col_def in self.schema['columns']:
                col_name = col_def['name']
                if col_name in df.columns and 'type' in col_def:
                    try:
                        df[col_name] = self._cast_column(df[col_name], col_def['type'])
                    except Exception as e:
                        logger.warning(f"Failed to cast {col_name} to {col_def['type']}: {e}")
        
        logger.info("Schema applied successfully")
        return df

    def _cast_column(self, series: pd.Series, dtype: str) -> pd.Series:
        """
        Cast pandas Series to specified data type.

        Args:
            series: Pandas Series
            dtype: Target data type (string, integer, float, boolean, datetime)

        Returns:
            Casted Series
        """
        type_map = {
            'string': 'str',
            'integer': 'int64',
            'float': 'float64',
            'boolean': 'bool',
            'datetime': 'datetime64[ns]'
        }
        
        pd_dtype = type_map.get(dtype, dtype)
        return series.astype(pd_dtype)

    def _generate_output_filename(self, prefix: str = "extract") -> str:
        """
        Generate timestamped output filename.

        Args:
            prefix: Filename prefix

        Returns:
            Filename with timestamp
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{prefix}_{timestamp}.parquet"

    def _write_to_bronze(
        self,
        df: pd.DataFrame,
        filename: str,
        partition_cols: Optional[List[str]] = None
    ) -> str:
        """
        Write DataFrame to bronze layer in Parquet format.

        Args:
            df: DataFrame to write
            filename: Output filename
            partition_cols: Optional columns to partition by

        Returns:
            Full path to written file
        """
        output_file = self.output_path / filename
        
        if partition_cols:
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                partition_cols=partition_cols,
                index=False
            )
        else:
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
        
        logger.info(f"Wrote {len(df)} records to {output_file}")
        return str(output_file)

    def __repr__(self) -> str:
        """String representation"""
        return f"{self.__class__.__name__}(output_path={self.output_path})"
