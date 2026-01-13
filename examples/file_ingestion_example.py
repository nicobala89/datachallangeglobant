"""
Example: File Connector and Extractor Usage
Demonstrates how to use the connector/extractor architecture for file ingestion.
"""

from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor
from loguru import logger

# Configure logging
logger.add("logs/ingestion.log", rotation="1 day")


def example_csv_extraction():
    """
    Example: Extract data from a CSV file
    """
    print("\n=== CSV Extraction Example ===\n")
    
    # Step 1: Create connector
    connector = FileConnector(config={
        'path': '/data/source/sales_data.csv',
        'storage_type': 'local',
        'expected_format': 'csv'
    })
    
    # Step 2: Validate file exists
    try:
        connector.validate()
        print("✓ File validation successful")
    except Exception as e:
        print(f"✗ Validation failed: {e}")
        return
    
    # Step 3: Get file metadata
    metadata = connector.get_metadata()
    print(f"File: {metadata['filename']}")
    print(f"Size: {metadata['size_mb']} MB")
    print(f"Modified: {metadata['modified_at']}")
    
    # Step 4: Create extractor
    extractor = FileExtractor(
        connector=connector,
        output_path='/data/bronze/sales/',
        schema_path='ingestion/schemas/sales_schema.yaml',
        config={
            'csv': {
                'delimiter': ',',
                'encoding': 'utf-8'
            }
        }
    )
    
    # Step 5: Extract data
    result = extractor.extract()
    
    print(f"\n✓ Extraction successful!")
    print(f"  Records: {result.record_count}")
    print(f"  Output: {result.output_path}")
    print(f"  Timestamp: {result.timestamp}")
    
    # Cleanup
    connector.close()


def example_parquet_extraction():
    """
    Example: Extract data from a Parquet file
    """
    print("\n=== Parquet Extraction Example ===\n")
    
    # Using context manager (automatic cleanup)
    with FileConnector(config={'path': '/data/source/events.parquet'}) as connector:
        
        # Validate
        connector.validate()
        print("✓ File validation successful")
        
        # Extract
        extractor = FileExtractor(
            connector=connector,
            output_path='/data/bronze/events/'
        )
        
        result = extractor.extract()
        print(f"✓ Extracted {result.record_count} records")


def example_xlsx_extraction():
    """
    Example: Extract data from an Excel file
    """
    print("\n=== Excel Extraction Example ===\n")
    
    connector = FileConnector(config={
        'path': '/data/source/monthly_report.xlsx',
        'expected_format': 'xlsx'
    })
    
    connector.validate()
    
    extractor = FileExtractor(
        connector=connector,
        output_path='/data/bronze/reports/',
        config={
            'xlsx': {
                'sheet_name': 'Sales Data',
                'header': 0
            }
        }
    )
    
    result = extractor.extract()
    print(f"✓ Extracted {result.record_count} records from Excel")


def example_incremental_extraction():
    """
    Example: Incremental extraction using watermark
    """
    print("\n=== Incremental Extraction Example ===\n")
    
    from datetime import datetime, timedelta
    
    connector = FileConnector(config={'path': '/data/source/transactions.csv'})
    connector.validate()
    
    extractor = FileExtractor(
        connector=connector,
        output_path='/data/bronze/transactions/'
    )
    
    # Get last watermark (in production, load from metadata store)
    last_run = datetime.now() - timedelta(days=1)
    
    # Extract only new records
    result = extractor.extract_incremental(
        watermark_column='transaction_date',
        last_watermark=last_run
    )
    
    print(f"✓ Incremental extraction: {result.record_count} new records")


def example_from_yaml_config():
    """
    Example: Create connector from YAML configuration
    """
    print("\n=== YAML Configuration Example ===\n")
    
    # Assumes config file exists at: config/sources/sales_file.yaml
    # Content:
    # path: "/data/source/sales.csv"
    # storage_type: local
    # expected_format: csv
    
    connector = FileConnector.from_yaml('config/sources/sales_file.yaml')
    connector.validate()
    
    extractor = FileExtractor(
        connector=connector,
        output_path='/data/bronze/sales/'
    )
    
    result = extractor.extract()
    print(f"✓ Extracted from YAML config: {result.record_count} records")


if __name__ == "__main__":
    print("Gorigami Data Framework - File Ingestion Examples")
    print("=" * 60)
    
    # Run examples (comment out as needed)
    try:
        example_csv_extraction()
    except Exception as e:
        print(f"CSV example failed: {e}")
    
    try:
        example_parquet_extraction()
    except Exception as e:
        print(f"Parquet example failed: {e}")
    
    try:
        example_xlsx_extraction()
    except Exception as e:
        print(f"Excel example failed: {e}")
    
    try:
        example_incremental_extraction()
    except Exception as e:
        print(f"Incremental example failed: {e}")
    
    print("\n" + "=" * 60)
    print("Examples completed!")
