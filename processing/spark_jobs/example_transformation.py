"""
Example Spark Transformation Job
Demonstrates data transformation patterns using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from loguru import logger


def create_spark_session(app_name: str = "GorigamiTransformation") -> SparkSession:
    """
    Create and configure a Spark session.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def transform_data(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Example transformation: Clean, enrich, and aggregate data.

    Args:
        spark: SparkSession instance
        input_path: Path to input data
        output_path: Path to save transformed data
    """
    logger.info(f"Reading data from {input_path}")
    
    # Read raw data
    df = spark.read.parquet(input_path)
    
    logger.info(f"Input data: {df.count()} rows, {len(df.columns)} columns")
    
    # Example transformations
    transformed_df = df \
        .filter(F.col("status").isNotNull()) \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("year", F.year(F.col("created_at"))) \
        .withColumn("month", F.month(F.col("created_at"))) \
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))) \
        .dropDuplicates(["id"]) \
        .orderBy("created_at")
    
    logger.info(f"Transformed data: {transformed_df.count()} rows")
    
    # Write to output
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    logger.info(f"Data written to {output_path}")


def aggregate_metrics(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Example aggregation: Calculate summary metrics.

    Args:
        spark: SparkSession instance
        input_path: Path to input data
        output_path: Path to save aggregated data
    """
    logger.info(f"Reading data from {input_path}")
    
    df = spark.read.parquet(input_path)
    
    # Calculate aggregations
    aggregated_df = df.groupBy("category", "year", "month") \
        .agg(
            F.count("*").alias("total_records"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.min("created_at").alias("first_record"),
            F.max("created_at").alias("last_record")
        ) \
        .orderBy("year", "month", "category")
    
    logger.info(f"Aggregated to {aggregated_df.count()} summary rows")
    
    # Write aggregated data
    aggregated_df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"Aggregated data written to {output_path}")


def main():
    """
    Main execution function.
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Example paths (adjust based on your environment)
        raw_data_path = "/opt/spark-apps/data/raw/input.parquet"
        transformed_path = "/opt/spark-apps/data/curated/transformed.parquet"
        aggregated_path = "/opt/spark-apps/data/curated/aggregated.parquet"
        
        # Run transformations
        logger.info("Starting data transformation pipeline")
        transform_data(spark, raw_data_path, transformed_path)
        aggregate_metrics(spark, transformed_path, aggregated_path)
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
