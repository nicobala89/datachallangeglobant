"""
Quality Validation Operator
Airflow operator for data quality validation.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from loguru import logger

from quality.validation import PipelineValidator


class QualityValidationOperator(BaseOperator):
    """
    Airflow operator for data quality validation.
    
    Validates data against quality rules and fails
    task if validation does not pass.
    """

    template_fields = ['data_path', 'rules_path']
    ui_color = '#FFD700'  # Gold

    @apply_defaults
    def __init__(
        self,
        data_path: str,
        layer: str,
        rules_path: str,
        fail_on_error: bool = True,
        spark_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        """
        Initialize quality validation operator.

        Args:
            data_path: Path to data (Parquet files or table name)
            layer: Data layer (bronze, silver, gold)
            rules_path: Path to quality rules YAML
            fail_on_error: Whether to fail task on validation errors
            spark_config: Spark configuration
        """
        super().__init__(*args, **kwargs)
        self.data_path = data_path
        self.layer = layer
        self.rules_path = rules_path
        self.fail_on_error = fail_on_error
        self.spark_config = spark_config or {}

    def execute(self, context):
        """Execute quality validation task"""
        logger.info(f"Starting quality validation: {self.layer} layer")
        logger.info(f"  Data: {self.data_path}")
        logger.info(f"  Rules: {self.rules_path}")
        
        # Create Spark session
        spark = self._create_spark_session()
        
        try:
            # Create validator
            validator = PipelineValidator(
                spark=spark,
                rules_path=self.rules_path
            )
            
            # Read data
            df = self._read_data(spark)
            
            # Validate
            result = validator.validate_dataframe(df, layer=self.layer)
            
            # Log results
            logger.info(f"Validation result: {result}")
            logger.info(f"  Rules executed: {result.rules_executed}")
            logger.info(f"  Rules passed: {result.rules_passed}")
            logger.info(f"  Rules failed: {result.rules_failed}")
            
            if result.failures:
                logger.error(f"  Failures: {result.failures}")
            
            if result.warnings:
                logger.warning(f"  Warnings: {result.warnings}")
            
            # Push metadata to XCom
            context['task_instance'].xcom_push(
                key='validation_result',
                value=result.to_dict()
            )
            
            # Fail task if validation failed and fail_on_error is True
            if not result.passed and self.fail_on_error:
                raise AirflowException(
                    f"Quality validation failed for {self.layer} layer: "
                    f"{result.rules_failed} rules failed"
                )
            
            return result.to_dict()
            
        finally:
            # Stop Spark session
            spark.stop()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session"""
        builder = SparkSession.builder.appName(self.task_id)
        
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()

    def _read_data(self, spark: SparkSession):
        """Read data from path or table"""
        if self.data_path.endswith('.parquet') or '*' in self.data_path:
            # Read Parquet files
            return spark.read.parquet(self.data_path)
        else:
            # Read table
            return spark.read.table(self.data_path)
