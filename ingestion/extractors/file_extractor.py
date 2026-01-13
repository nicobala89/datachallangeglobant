"""
File Extractor
Extracts data from CSV, Parquet, and XLSX files with schema application and traceability.
"""

from typing import Dict, Any, Optional
from pathlib import Path
import pandas as pd
from loguru import logger

from .base_extractor import BaseExtractor, ExtractionResult, ExtractorError
from ingestion.connectors.file_connector import FileConnector


class FileExtractor(BaseExtractor):
    """
    Extractor for file-based data sources (CSV, Parquet, XLSX).
    
    Reads files, applies schema, adds traceability metadata,
    and writes to bronze layer in standardized Parquet format.
    """

    def __init__(
        self,
        connector: FileConnector,
        output_path: str,
        schema_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize file extractor.

        Args:
            connector: FileConnector instance
            output_path: Path to bronze layer output directory
            schema_path: Optional path to schema definition
            config: Optional configuration (e.g., CSV parsing options)
        """
        if not isinstance(connector, FileConnector):
            raise ExtractorError("Connector must be a FileConnector instance")
        
        super().__init__(connector, output_path, schema_path, config)
        self.file_format = connector._detect_format()

    def extract(self) -> ExtractionResult:
        """
        Extract data from file and write to bronze layer.

        Returns:
            ExtractionResult with metadata

        Raises:
            ExtractorError: If extraction fails
        """
        try:
            # Validate file exists
            self.connector.validate()
            
            # Get file metadata
            file_metadata = self.connector.get_metadata()
            logger.info(f"Extracting from {file_metadata['path']}")
            
            # Read file based on format
            df = self._read_file()
            
            logger.info(f"Read {len(df)} records from {file_metadata['filename']}")
            
            # Apply schema if defined
            if self.schema:
                df = self._apply_schema(df)
            
            # Add traceability metadata
            source_info = {
                'source_system': 'file',
                'source_entity': file_metadata['filename'],
                'source_path': file_metadata['path'],
                'source_format': self.file_format,
                'source_size_mb': file_metadata['size_mb'],
                'schema_version': self.schema.get('version', '1.0') if self.schema else 'inferred'
            }
            df = self._add_traceability_metadata(df, source_info)
            
            # Generate output filename
            output_filename = self._generate_output_filename(
                prefix=Path(file_metadata['filename']).stem
            )
            
            # Write to bronze layer
            output_file = self._write_to_bronze(df, output_filename)
            
            # Create result
            result = ExtractionResult(
                success=True,
                output_path=output_file,
                record_count=len(df),
                metadata={
                    'source_file': file_metadata['path'],
                    'source_format': self.file_format,
                    'columns': list(df.columns),
                    'schema_applied': self.schema is not None
                }
            )
            
            logger.info(f"Extraction successful: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise ExtractorError(f"Failed to extract from file: {e}")

    def extract_incremental(
        self,
        watermark_column: str,
        last_watermark: Any
    ) -> ExtractionResult:
        """
        Extract data incrementally based on watermark.

        For file-based sources, this filters records after extraction.

        Args:
            watermark_column: Column name for watermark filtering
            last_watermark: Last successful watermark value

        Returns:
            ExtractionResult with filtered data

        Raises:
            ExtractorError: If extraction fails
        """
        try:
            # First, do full extraction
            result = self.extract()
            
            # Read the extracted file and filter
            df = pd.read_parquet(result.output_path)
            
            # Filter by watermark
            if watermark_column not in df.columns:
                raise ExtractorError(f"Watermark column '{watermark_column}' not found in data")
            
            original_count = len(df)
            df = df[df[watermark_column] > last_watermark]
            filtered_count = len(df)
            
            logger.info(
                f"Incremental filter: {original_count} -> {filtered_count} records "
                f"(watermark: {watermark_column} > {last_watermark})"
            )
            
            # Overwrite with filtered data
            if filtered_count > 0:
                self._write_to_bronze(df, Path(result.output_path).name)
                result.record_count = filtered_count
                result.metadata['incremental'] = True
                result.metadata['watermark_column'] = watermark_column
                result.metadata['last_watermark'] = str(last_watermark)
            else:
                logger.info("No new records found in incremental extraction")
            
            return result
            
        except Exception as e:
            logger.error(f"Incremental extraction failed: {e}")
            raise ExtractorError(f"Failed to extract incrementally: {e}")

    def _read_file(self) -> pd.DataFrame:
        """
        Read file based on format.

        Returns:
            DataFrame with file contents

        Raises:
            ExtractorError: If file reading fails
        """
        file_path = self.connector.path
        
        try:
            if self.file_format == 'csv':
                return self._read_csv(file_path)
            elif self.file_format == 'parquet':
                return self._read_parquet(file_path)
            elif self.file_format == 'xlsx':
                return self._read_xlsx(file_path)
            elif self.file_format in ['json', 'jsonl']:
                return self._read_json(file_path)
            else:
                raise ExtractorError(f"Unsupported file format: {self.file_format}")
        except Exception as e:
            raise ExtractorError(f"Failed to read {self.file_format} file: {e}")

    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        """Read CSV file"""
        csv_config = self.config.get('csv', {})
        
        return pd.read_csv(
            file_path,
            sep=csv_config.get('delimiter', ','),
            encoding=csv_config.get('encoding', 'utf-8'),
            header=csv_config.get('header', 0),
            skiprows=csv_config.get('skiprows', None),
            na_values=csv_config.get('na_values', None)
        )

    def _read_parquet(self, file_path: Path) -> pd.DataFrame:
        """Read Parquet file"""
        return pd.read_parquet(file_path, engine='pyarrow')

    def _read_xlsx(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file"""
        xlsx_config = self.config.get('xlsx', {})
        
        return pd.read_excel(
            file_path,
            sheet_name=xlsx_config.get('sheet_name', 0),
            header=xlsx_config.get('header', 0),
            skiprows=xlsx_config.get('skiprows', None),
            engine='openpyxl'
        )

    def _read_json(self, file_path: Path) -> pd.DataFrame:
        """Read JSON/JSONL file"""
        json_config = self.config.get('json', {})
        
        if self.file_format == 'jsonl':
            return pd.read_json(file_path, lines=True)
        else:
            return pd.read_json(
                file_path,
                orient=json_config.get('orient', 'records')
            )
