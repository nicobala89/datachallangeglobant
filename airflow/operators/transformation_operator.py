"""
Transformation Operator
Airflow operator for running PySpark transformation tasks.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from loguru import logger

from processing.transformers import BaseTransformer


class TransformationOperator(BaseOperator):
    """
    Airflow operator for PySpark transformations.
    
    Reads bronze data, applies transformations,
    and writes to silver layer.
    """

    template_fields = ['bronze_path', 'silver_table']
    ui_color = '#87CEEB'  # Sky blue

    @apply_defaults
    def __init__(
        self,
        transformer_class: type,
        bronze_path: str,
        silver_table: str,
        transformer_config: Optional[Dict[str, Any]] = None,
        spark_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        """
        Initialize transformation operator.

        Args:
            transformer_class: Transformer class to instantiate
            bronze_path: Path to bronze Parquet files
            silver_table: Target silver table name
            transformer_config: Transformer configuration
            spark_config: Spark configuration
        """
        super().__init__(*args, **kwargs)
        self.transformer_class = transformer_class
        self.bronze_path = bronze_path
        self.silver_table = silver_table
        self.transformer_config = transformer_config or {}
        self.spark_config = spark_config or {}

    def execute(self, context):
        """Execute transformation task"""
        logger.info(f"Starting transformation: {self.bronze_path} → {self.silver_table}")
        
        # Create Spark session
        spark = self._create_spark_session()
        
        try:
            # Create transformer
            transformer = self.transformer_class(
                spark=spark,
                bronze_path=self.bronze_path,
                silver_table=self.silver_table,
                config=self.transformer_config
            )
            
            # Run transformation
            result = transformer.run()
            
            logger.info(f"✓ Transformation complete: {result.record_count} records")
            logger.info(f"  Target: {result.target_table}")
            
            # Push metadata to XCom
            context['task_instance'].xcom_push(
                key='transformation_result',
                value=result.to_dict()
            )
            
            return result.to_dict()
            
        finally:
            # Stop Spark session
            spark.stop()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with configuration"""
        builder = SparkSession.builder.appName(self.task_id)
        
        # Apply custom Spark config
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
