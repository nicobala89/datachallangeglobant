"""
Example Sales Transformer
Demonstrates how to create a custom transformer for sales data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from processing.transformers import BaseTransformer
from processing.transformations import common_transformations as ct
from loguru import logger


class SalesTransformer(BaseTransformer):
    """
    Transformer for sales transaction data.
    
    Reads bronze sales data, applies cleaning and enrichment,
    and writes to silver.sales_transactions table.
    """

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply sales-specific transformations.

        Args:
            df: Bronze DataFrame with raw sales data

        Returns:
            Transformed DataFrame ready for silver layer
        """
        logger.info("Starting sales transformation")
        
        # 1. Remove traceability columns from bronze (not needed in silver)
        bronze_metadata_cols = [c for c in df.columns if c.startswith('_')]
        df = df.drop(*bronze_metadata_cols)
        logger.debug(f"Removed {len(bronze_metadata_cols)} bronze metadata columns")
        
        # 2. Clean null values in required fields
        df = ct.clean_nulls(
            df,
            columns=['transaction_id', 'customer_id', 'product_id'],
            strategy='drop'
        )
        
        # 3. Standardize date format
        df = ct.standardize_dates(
            df,
            column='transaction_date',
            output_format='yyyy-MM-dd HH:mm:ss'
        )
        
        # 4. Remove duplicates based on transaction_id
        df = ct.deduplicate(
            df,
            key_columns=['transaction_id'],
            keep='last'
        )
        
        # 5. Add calculated columns
        df = ct.add_calculated_column(
            df,
            column_name='total_amount_calculated',
            expression='quantity * unit_price'
        )
        
        # 6. Validate calculated amount matches reported amount
        df = df.withColumn(
            'amount_discrepancy',
            F.abs(F.col('total_amount') - F.col('total_amount_calculated'))
        )
        
        # Flag records with discrepancies > $0.01
        df = df.withColumn(
            'has_discrepancy',
            F.when(F.col('amount_discrepancy') > 0.01, True).otherwise(False)
        )
        
        # 7. Cast columns to proper types
        df = ct.cast_columns(
            df,
            column_types={
                'quantity': 'integer',
                'unit_price': 'double',
                'total_amount': 'double'
            }
        )
        
        # 8. Select final columns for silver layer
        final_columns = [
            'transaction_id',
            'customer_id',
            'product_id',
            'quantity',
            'unit_price',
            'total_amount',
            'transaction_date',
            'customer_email',
            'notes',
            'has_discrepancy'
        ]
        
        df = ct.select_columns(df, final_columns)
        
        logger.info(f"Sales transformation completed: {df.count()} records")
        
        return df


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SalesTransformer") \
        .config("spark.jars", "/path/to/postgresql-jdbc.jar") \
        .getOrCreate()
    
    # Create transformer
    transformer = SalesTransformer(
        spark=spark,
        bronze_path='/data/bronze/sales/*.parquet',
        silver_table='silver.sales_transactions',
        config={
            'postgres_host': 'localhost',
            'postgres_port': 5432,
            'postgres_database': 'gorigami_analytics',
            'postgres_user': 'gorigami',
            'postgres_password': 'gorigami_password'
        }
    )
    
    # Run transformation
    result = transformer.run()
    
    print(f"Transformation result: {result}")
    print(f"Records processed: {result.record_count}")
    print(f"Target table: {result.target_table}")
    
    spark.stop()
