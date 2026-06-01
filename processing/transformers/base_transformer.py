"""
Base Transformer Abstract Class
Defines the interface for all transformers in the Data Framework.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from loguru import logger
import yaml


class TransformerError(Exception):
    """Base exception for transformer errors"""
    pass


class TransformationResult:
    """
    Result of a transformation operation with metadata.
    """

    def __init__(
        self,
        success: bool,
        target_table: str,
        record_count: int,
        metadata: Dict[str, Any]
    ):
        self.success = success
        self.target_table = target_table
        self.record_count = record_count
        self.metadata = metadata
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            'success': self.success,
            'target_table': self.target_table,
            'record_count': self.record_count,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }

    def __repr__(self) -> str:
        return f"TransformationResult(success={self.success}, records={self.record_count}, table={self.target_table})"


class BaseTransformer(ABC):
    """
    Abstract base class for all transformers.
    
    Transformers are responsible for:
    - Reading bronze Parquet data
    - Applying transformation logic
    - Writing to silver PostgreSQL layer
    - Tracking transformation metadata
    """

    def __init__(
        self,
        spark: SparkSession,
        bronze_path: str,
        silver_table: str,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the transformer.

        Args:
            spark: SparkSession instance
            bronze_path: Path to bronze layer Parquet files
            silver_table: Target silver table name (e.g., 'silver.sales_transactions')
            config: Optional transformer configuration
        """
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_table = silver_table
        self.config = config or {}
        
        logger.info(f"Initialized {self.__class__.__name__} transformer")
        logger.info(f"  Bronze: {bronze_path}")
        logger.info(f"  Silver: {silver_table}")

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply transformation logic to DataFrame.
        
        This method contains the business logic for transforming data.
        It should be pure transformation logic without I/O.

        Args:
            df: Input DataFrame from bronze layer

        Returns:
            Transformed DataFrame ready for silver layer

        Raises:
            TransformerError: If transformation fails
        """
        pass

    def run(self) -> TransformationResult:
        """
        Execute the complete transformation pipeline.
        
        This method orchestrates:
        1. Read from bronze
        2. Apply transformations
        3. Write to silver
        4. Track metadata
        
        Returns:
            TransformationResult with metadata

        Raises:
            TransformerError: If transformation fails
        """
        try:
            logger.info(f"Starting transformation: {self.__class__.__name__}")
            
            # Read from bronze
            bronze_df = self.read_bronze()
            logger.info(f"Read {bronze_df.count()} records from bronze")
            
            # Apply transformations
            transformed_df = self.transform(bronze_df)
            record_count = transformed_df.count()
            logger.info(f"Transformed to {record_count} records")
            
            # Add silver metadata
            silver_df = self._add_silver_metadata(transformed_df)
            
            # Write to silver
            self.write_silver(silver_df)
            logger.info(f"Wrote {record_count} records to {self.silver_table}")
            
            # Create result
            result = TransformationResult(
                success=True,
                target_table=self.silver_table,
                record_count=record_count,
                metadata={
                    'bronze_path': self.bronze_path,
                    'transformer': self.__class__.__name__,
                    'config': self.config
                }
            )
            
            logger.info(f"Transformation completed successfully: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise TransformerError(f"Failed to transform data: {e}")

    def read_bronze(self) -> DataFrame:
        """
        Read Parquet data from bronze layer.

        Returns:
            DataFrame with bronze data

        Raises:
            TransformerError: If reading fails
        """
        try:
            df = self.spark.read.parquet(self.bronze_path)
            logger.debug(f"Read bronze data: {df.schema}")
            return df
        except Exception as e:
            raise TransformerError(f"Failed to read bronze data from {self.bronze_path}: {e}")

    def read_bronze_incremental(
        self,
        watermark_column: str,
        last_watermark: Any
    ) -> DataFrame:
        """
        Read bronze data incrementally based on watermark.

        Args:
            watermark_column: Column name for watermark filtering
            last_watermark: Last successful watermark value

        Returns:
            DataFrame with incremental data

        Raises:
            TransformerError: If reading fails
        """
        try:
            df = self.read_bronze()
            
            # Filter by watermark
            incremental_df = df.filter(df[watermark_column] > last_watermark)
            
            logger.info(
                f"Incremental read: filtered by {watermark_column} > {last_watermark}"
            )
            
            return incremental_df
            
        except Exception as e:
            raise TransformerError(f"Failed to read incremental bronze data: {e}")

    def write_silver(
        self,
        df: DataFrame,
        mode: str = "append"
    ) -> None:
        """
        Write DataFrame to silver PostgreSQL layer.

        Args:
            df: DataFrame to write
            mode: Write mode ('append', 'overwrite')

        Raises:
            TransformerError: If writing fails
        """
        try:
            # Get PostgreSQL connection properties
            jdbc_url, properties = self._get_postgres_config()
            
            # Write to PostgreSQL
            df.write \
                .jdbc(
                    url=jdbc_url,
                    table=self.silver_table,
                    mode=mode,
                    properties=properties
                )
            
            logger.info(f"Wrote to {self.silver_table} in {mode} mode")
            
        except Exception as e:
            raise TransformerError(f"Failed to write to silver layer: {e}")

    def _get_postgres_config(self) -> tuple:
        """
        Get PostgreSQL JDBC configuration.

        Returns:
            Tuple of (jdbc_url, properties)
        """
        # Get from config or environment
        host = self.config.get('postgres_host', 'localhost')
        port = self.config.get('postgres_port', 5432)
        database = self.config.get('postgres_database', 'globant_analytics')
        user = self.config.get('postgres_user', 'globant')
        password = self.config.get('postgres_password', 'globant_password')
        
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        
        properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }
        
        return jdbc_url, properties

    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add silver layer metadata columns.

        Args:
            df: Source DataFrame

        Returns:
            DataFrame with silver metadata columns
        """
        from pyspark.sql import functions as F
        
        df = df.withColumn('_silver_load_timestamp', F.current_timestamp())
        df = df.withColumn('_bronze_source_path', F.lit(self.bronze_path))
        df = df.withColumn('_transformer_name', F.lit(self.__class__.__name__))
        df = df.withColumn('_transformation_version', F.lit('1.0'))
        
        logger.debug("Added silver metadata columns")
        return df

    def read_reference(self, table_name: str) -> DataFrame:
        """
        Read reference data from silver layer.

        Args:
            table_name: Silver table name to read

        Returns:
            DataFrame with reference data
        """
        try:
            jdbc_url, properties = self._get_postgres_config()
            
            df = self.spark.read \
                .jdbc(
                    url=jdbc_url,
                    table=table_name,
                    properties=properties
                )
            
            logger.debug(f"Read reference data from {table_name}")
            return df
            
        except Exception as e:
            raise TransformerError(f"Failed to read reference data from {table_name}: {e}")

    @classmethod
    def from_yaml(cls, spark: SparkSession, config_path: str) -> 'BaseTransformer':
        """
        Create a transformer instance from YAML configuration.

        Args:
            spark: SparkSession instance
            config_path: Path to YAML configuration file

        Returns:
            Transformer instance
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            return cls(
                spark=spark,
                bronze_path=config['bronze_path'],
                silver_table=config['silver_table'],
                config=config.get('config', {})
            )
        except Exception as e:
            raise TransformerError(f"Failed to load transformer from {config_path}: {e}")

    def __repr__(self) -> str:
        """String representation"""
        return f"{self.__class__.__name__}(bronze={self.bronze_path}, silver={self.silver_table})"
