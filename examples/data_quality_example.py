"""
Example: Data Quality Usage
Demonstrates source profiling and pipeline validation.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from quality.profiling import SourceProfiler
from quality.validation import PipelineValidator
from pyspark.sql import SparkSession


def example_source_profiling():
    """
    Example: Profile a source file for audit purposes
    """
    print("\n=== Source Profiling Example ===\n")
    
    # Create profiler
    profiler = SourceProfiler(config={'sample_size': 5})
    
    # Profile a file
    profile = profiler.profile_file('/tmp/gorigami_test/source/sales_test.csv')
    
    print(f"✓ Profiled: {profile.source_name}")
    print(f"\nOverview:")
    print(f"  Total rows: {profile.profile_data['overview']['total_rows']}")
    print(f"  Total columns: {profile.profile_data['overview']['total_columns']}")
    print(f"  Duplicates: {profile.profile_data['overview']['duplicate_percentage']}%")
    
    print(f"\nQuality Score:")
    print(f"  Overall: {profile.profile_data['quality_score']['overall_score']}")
    print(f"  Completeness: {profile.profile_data['quality_score']['completeness']}%")
    print(f"  Uniqueness: {profile.profile_data['quality_score']['uniqueness']}%")
    
    print(f"\nColumn Statistics:")
    for col in profile.profile_data['columns'][:3]:  # Show first 3 columns
        print(f"  {col['name']}:")
        print(f"    - Type: {col['dtype']}")
        print(f"    - Nulls: {col['null_percentage']}%")
        print(f"    - Distinct: {col['distinct_count']}")
    
    # Save profile as JSON
    profile.to_json('/tmp/gorigami_test/reports/sales_profile.json')
    print(f"\n✓ Saved profile to /tmp/gorigami_test/reports/sales_profile.json")


def example_pipeline_validation():
    """
    Example: Validate bronze data with quality rules
    """
    print("\n=== Pipeline Validation Example ===\n")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("QualityValidation") \
        .getOrCreate()
    
    # Read bronze data
    df = spark.read.parquet('/tmp/gorigami_test/bronze/sales/*.parquet')
    print(f"✓ Read {df.count()} records from bronze layer")
    
    # Create validator with rules
    validator = PipelineValidator(
        spark=spark,
        rules=[
            {
                'name': 'required_columns',
                'type': 'schema',
                'severity': 'error',
                'config': {
                    'columns': ['transaction_id', 'customer_id', 'product_id']
                }
            },
            {
                'name': 'no_nulls_in_keys',
                'type': 'null_check',
                'severity': 'error',
                'config': {
                    'columns': ['transaction_id'],
                    'max_null_percentage': 0
                }
            },
            {
                'name': 'positive_quantity',
                'type': 'business_rule',
                'severity': 'error',
                'config': {
                    'condition': 'quantity > 0'
                }
            },
            {
                'name': 'volume_check',
                'type': 'volume',
                'severity': 'warning',
                'config': {
                    'min_rows': 1,
                    'max_rows': 1000000
                }
            }
        ]
    )
    
    # Validate
    result = validator.validate_dataframe(df, layer='bronze')
    
    print(f"\nValidation Result:")
    print(f"  Status: {'PASSED ✓' if result.passed else 'FAILED ✗'}")
    print(f"  Rules executed: {result.rules_executed}")
    print(f"  Rules passed: {result.rules_passed}")
    print(f"  Rules failed: {result.rules_failed}")
    
    if result.failures:
        print(f"\n  Failures:")
        for failure in result.failures:
            print(f"    - {failure['rule_name']}: {failure['message']}")
    
    if result.warnings:
        print(f"\n  Warnings:")
        for warning in result.warnings:
            print(f"    - {warning['rule_name']}: {warning['message']}")
    
    spark.stop()


def example_validation_from_yaml():
    """
    Example: Validate using rules from YAML file
    """
    print("\n=== Validation from YAML Example ===\n")
    
    spark = SparkSession.builder \
        .appName("YAMLValidation") \
        .getOrCreate()
    
    # Read data
    df = spark.read.parquet('/tmp/gorigami_test/bronze/sales/*.parquet')
    
    # Create validator from YAML
    validator = PipelineValidator(
        spark=spark,
        rules_path='config/quality/bronze_rules.yaml'
    )
    
    # Validate
    result = validator.validate_dataframe(df, layer='bronze')
    
    print(f"Validation: {'PASSED ✓' if result.passed else 'FAILED ✗'}")
    print(f"Rules: {result.rules_passed}/{result.rules_executed} passed")
    
    spark.stop()


if __name__ == "__main__":
    print("Gorigami Data Framework - Data Quality Examples")
    print("=" * 60)
    
    # Run examples
    try:
        example_source_profiling()
    except Exception as e:
        print(f"Profiling example failed: {e}")
    
    try:
        example_pipeline_validation()
    except Exception as e:
        print(f"Validation example failed: {e}")
    
    print("\n" + "=" * 60)
    print("Examples completed!")
