"""
Pipeline Task Factory
Factory for creating common pipeline task patterns in Airflow DAGs.
"""

from typing import Dict, Any, Optional, List
from airflow.operators.python import PythonOperator

from airflow.operators.ingestion_operator import IngestionOperator
from airflow.operators.transformation_operator import TransformationOperator
from airflow.operators.quality_validation_operator import QualityValidationOperator


class PipelineTaskFactory:
    """
    Factory for creating common pipeline task patterns.
    
    Simplifies DAG creation by providing reusable task builders.
    """

    @staticmethod
    def create_ingestion_task(
        task_id: str,
        source_path: str,
        bronze_path: str,
        schema_path: Optional[str] = None,
        **kwargs
    ) -> IngestionOperator:
        """
        Create an ingestion task.

        Args:
            task_id: Task ID
            source_path: Source file path
            bronze_path: Bronze layer output path
            schema_path: Optional schema path
            **kwargs: Additional operator arguments

        Returns:
            IngestionOperator
        """
        return IngestionOperator(
            task_id=task_id,
            source_path=source_path,
            bronze_path=bronze_path,
            schema_path=schema_path,
            **kwargs
        )

    @staticmethod
    def create_transformation_task(
        task_id: str,
        transformer_class: type,
        bronze_path: str,
        silver_table: str,
        **kwargs
    ) -> TransformationOperator:
        """
        Create a transformation task.

        Args:
            task_id: Task ID
            transformer_class: Transformer class
            bronze_path: Bronze Parquet path
            silver_table: Silver table name
            **kwargs: Additional operator arguments

        Returns:
            TransformationOperator
        """
        return TransformationOperator(
            task_id=task_id,
            transformer_class=transformer_class,
            bronze_path=bronze_path,
            silver_table=silver_table,
            **kwargs
        )

    @staticmethod
    def create_quality_task(
        task_id: str,
        data_path: str,
        layer: str,
        rules_path: str,
        fail_on_error: bool = True,
        **kwargs
    ) -> QualityValidationOperator:
        """
        Create a quality validation task.

        Args:
            task_id: Task ID
            data_path: Data path or table name
            layer: Data layer (bronze, silver, gold)
            rules_path: Quality rules YAML path
            fail_on_error: Fail task on validation errors
            **kwargs: Additional operator arguments

        Returns:
            QualityValidationOperator
        """
        return QualityValidationOperator(
            task_id=task_id,
            data_path=data_path,
            layer=layer,
            rules_path=rules_path,
            fail_on_error=fail_on_error,
            **kwargs
        )

    @staticmethod
    def create_complete_pipeline(
        dag,
        pipeline_name: str,
        source_path: str,
        bronze_path: str,
        transformer_class: type,
        silver_table: str,
        schema_path: Optional[str] = None,
        bronze_rules_path: str = 'config/quality/bronze_rules.yaml',
        silver_rules_path: str = 'config/quality/silver_rules.yaml',
        transformer_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a complete pipeline with ingestion, validation, and transformation.

        Args:
            dag: Airflow DAG
            pipeline_name: Pipeline name prefix
            source_path: Source file path
            bronze_path: Bronze output path
            transformer_class: Transformer class
            silver_table: Silver table name
            schema_path: Optional schema path
            bronze_rules_path: Bronze quality rules path
            silver_rules_path: Silver quality rules path
            transformer_config: Transformer configuration

        Returns:
            Dictionary of created tasks
        """
        with dag:
            # Ingestion task
            ingest = IngestionOperator(
                task_id=f'{pipeline_name}_ingest',
                source_path=source_path,
                bronze_path=bronze_path,
                schema_path=schema_path
            )

            # Bronze validation
            validate_bronze = QualityValidationOperator(
                task_id=f'{pipeline_name}_validate_bronze',
                data_path=f'{bronze_path}/*.parquet',
                layer='bronze',
                rules_path=bronze_rules_path
            )

            # Transformation
            transform = TransformationOperator(
                task_id=f'{pipeline_name}_transform',
                transformer_class=transformer_class,
                bronze_path=f'{bronze_path}/*.parquet',
                silver_table=silver_table,
                transformer_config=transformer_config
            )

            # Silver validation
            validate_silver = QualityValidationOperator(
                task_id=f'{pipeline_name}_validate_silver',
                data_path=silver_table,
                layer='silver',
                rules_path=silver_rules_path
            )

            # Set dependencies
            ingest >> validate_bronze >> transform >> validate_silver

            return {
                'ingest': ingest,
                'validate_bronze': validate_bronze,
                'transform': transform,
                'validate_silver': validate_silver
            }
