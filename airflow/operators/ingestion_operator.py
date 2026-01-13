"""
Ingestion Operator
Airflow operator for running ingestion tasks (connector + extractor).
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any, Optional
from loguru import logger

from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor


class IngestionOperator(BaseOperator):
    """
    Airflow operator for data ingestion.
    
    Validates connection, extracts data to bronze layer,
    and returns extraction metadata.
    """

    template_fields = ['source_path', 'bronze_path', 'schema_path']
    ui_color = '#90EE90'  # Light green

    @apply_defaults
    def __init__(
        self,
        source_path: str,
        bronze_path: str,
        connector_type: str = 'file',
        connector_config: Optional[Dict[str, Any]] = None,
        schema_path: Optional[str] = None,
        extractor_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        """
        Initialize ingestion operator.

        Args:
            source_path: Path to source data
            bronze_path: Path to bronze layer output
            connector_type: Type of connector ('file', 'database', 'api')
            connector_config: Connector configuration
            schema_path: Optional path to schema YAML
            extractor_config: Optional extractor configuration
        """
        super().__init__(*args, **kwargs)
        self.source_path = source_path
        self.bronze_path = bronze_path
        self.connector_type = connector_type
        self.connector_config = connector_config or {}
        self.schema_path = schema_path
        self.extractor_config = extractor_config or {}

    def execute(self, context):
        """Execute ingestion task"""
        logger.info(f"Starting ingestion: {self.source_path} → {self.bronze_path}")
        
        # Create connector
        connector = self._create_connector()
        
        # Validate connection
        connector.validate()
        logger.info("✓ Connection validated")
        
        # Get metadata
        metadata = connector.get_metadata()
        logger.info(f"Source metadata: {metadata}")
        
        # Create extractor
        extractor = self._create_extractor(connector)
        
        # Extract data
        result = extractor.extract()
        
        logger.info(f"✓ Ingestion complete: {result.record_count} records")
        logger.info(f"  Output: {result.output_path}")
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key='ingestion_result',
            value=result.to_dict()
        )
        
        # Cleanup
        connector.close()
        
        return result.to_dict()

    def _create_connector(self):
        """Create connector based on type"""
        if self.connector_type == 'file':
            config = {
                'path': self.source_path,
                **self.connector_config
            }
            return FileConnector(config=config)
        else:
            raise ValueError(f"Unsupported connector type: {self.connector_type}")

    def _create_extractor(self, connector):
        """Create extractor"""
        if self.connector_type == 'file':
            return FileExtractor(
                connector=connector,
                output_path=self.bronze_path,
                schema_path=self.schema_path,
                config=self.extractor_config
            )
        else:
            raise ValueError(f"Unsupported extractor type: {self.connector_type}")
