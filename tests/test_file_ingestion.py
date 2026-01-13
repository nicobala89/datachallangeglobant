"""
Simple test to verify connector and extractor functionality
"""

import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.connectors import FileConnector
from ingestion.extractors import FileExtractor


def create_sample_data():
    """Create sample CSV file for testing"""
    data = {
        'transaction_id': ['TXN001', 'TXN002', 'TXN003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003'],
        'product_id': ['PROD001', 'PROD002', 'PROD001'],
        'quantity': [2, 1, 3],
        'unit_price': [29.99, 49.99, 29.99],
        'total_amount': [59.98, 49.99, 89.97],
        'transaction_date': ['2026-01-13 10:00:00', '2026-01-13 11:30:00', '2026-01-13 14:15:00'],
        'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
    }
    
    df = pd.DataFrame(data)
    
    # Create test data directory
    test_dir = Path('/tmp/gorigami_test/source')
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Save as CSV
    csv_path = test_dir / 'sales_test.csv'
    df.to_csv(csv_path, index=False)
    print(f"✓ Created test CSV: {csv_path}")
    
    # Save as Parquet
    parquet_path = test_dir / 'sales_test.parquet'
    df.to_parquet(parquet_path, index=False)
    print(f"✓ Created test Parquet: {parquet_path}")
    
    # Save as Excel
    xlsx_path = test_dir / 'sales_test.xlsx'
    df.to_excel(xlsx_path, index=False, engine='openpyxl')
    print(f"✓ Created test Excel: {xlsx_path}")
    
    return csv_path, parquet_path, xlsx_path


def test_csv_extraction(csv_path):
    """Test CSV file extraction"""
    print("\n=== Testing CSV Extraction ===")
    
    # Create connector
    connector = FileConnector(config={
        'path': str(csv_path),
        'expected_format': 'csv'
    })
    
    # Validate
    connector.validate()
    print("✓ Connector validation passed")
    
    # Get metadata
    metadata = connector.get_metadata()
    print(f"✓ File metadata: {metadata['filename']}, {metadata['size_bytes']} bytes")
    
    # Create extractor
    extractor = FileExtractor(
        connector=connector,
        output_path='/tmp/gorigami_test/bronze/sales/'
    )
    
    # Extract
    result = extractor.extract()
    print(f"✓ Extraction successful: {result.record_count} records")
    print(f"  Output: {result.output_path}")
    
    # Verify bronze layer output
    bronze_df = pd.read_parquet(result.output_path)
    print(f"✓ Bronze layer verification:")
    print(f"  - Original columns: {result.metadata['columns'][:3]}...")
    print(f"  - Traceability columns added: {[c for c in bronze_df.columns if c.startswith('_')]}")
    
    connector.close()
    return True


def test_parquet_extraction(parquet_path):
    """Test Parquet file extraction"""
    print("\n=== Testing Parquet Extraction ===")
    
    connector = FileConnector(config={'path': str(parquet_path)})
    connector.validate()
    print("✓ Connector validation passed")
    
    extractor = FileExtractor(
        connector=connector,
        output_path='/tmp/gorigami_test/bronze/sales_parquet/'
    )
    
    result = extractor.extract()
    print(f"✓ Extraction successful: {result.record_count} records")
    
    connector.close()
    return True


def test_xlsx_extraction(xlsx_path):
    """Test Excel file extraction"""
    print("\n=== Testing Excel Extraction ===")
    
    connector = FileConnector(config={
        'path': str(xlsx_path),
        'expected_format': 'xlsx'
    })
    connector.validate()
    print("✓ Connector validation passed")
    
    extractor = FileExtractor(
        connector=connector,
        output_path='/tmp/gorigami_test/bronze/sales_excel/'
    )
    
    result = extractor.extract()
    print(f"✓ Extraction successful: {result.record_count} records")
    
    connector.close()
    return True


def main():
    """Run all tests"""
    print("Gorigami Data Framework - Connector & Extractor Tests")
    print("=" * 60)
    
    try:
        # Create sample data
        csv_path, parquet_path, xlsx_path = create_sample_data()
        
        # Run tests
        test_csv_extraction(csv_path)
        test_parquet_extraction(parquet_path)
        test_xlsx_extraction(xlsx_path)
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("\nBronze layer output:")
        print("  /tmp/gorigami_test/bronze/sales/")
        print("  /tmp/gorigami_test/bronze/sales_parquet/")
        print("  /tmp/gorigami_test/bronze/sales_excel/")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
