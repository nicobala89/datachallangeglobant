import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from processing.transformers.base_transformer import BaseTransformer, TransformationResult, TransformerError

class GlobantTransformer(BaseTransformer):
    """
    Globant PySpark Transformer.
    Handles data validation and ingestion of CSV files into raw database tables,
    capturing any validation failures into raw.rejected_log.
    """

    def __init__(self, spark: SparkSession, table_name: str, csv_path: str, silver_table: str, config=None):
        super().__init__(spark, csv_path, silver_table, config)
        self.table_name = table_name
        self.csv_path = csv_path

    def read_bronze(self) -> DataFrame:
        """Read CSV directly using Spark"""
        try:
            logger = self._get_logger()
            logger.info(f"Reading CSV file from: {self.csv_path}")
            # Read CSV with headers, schema inference can be off so we cast manually
            df = self.spark.read.option("header", "true").csv(self.csv_path)
            return df
        except Exception as e:
            raise TransformerError(f"Failed to read CSV from {self.csv_path}: {e}")

    def _get_logger(self):
        from loguru import logger
        return logger

    def transform(self, df: DataFrame) -> DataFrame:
        """
        No-op since transform is handled inside the run pipeline with partitioning
        valid vs invalid records.
        """
        return df

    def run_pipeline(self) -> TransformationResult:
        """
        Executes the validation and load pipeline:
        1. Reads CSV
        2. Validates rows based on table schema contract
        3. Writes valid rows to PG target table
        4. Writes invalid rows to PG raw.rejected_log
        """
        logger = self._get_logger()
        try:
            logger.info(f"Starting CSV pipeline for table: {self.table_name}")
            df = self.read_bronze()
            total_records = df.count()
            logger.info(f"Loaded {total_records} records from CSV")

            if self.table_name == "departments":
                # Cast id to Integer
                df_cast = df.withColumn("id", F.col("id").cast(IntegerType()))
                
                # Validation condition
                valid_cond = (
                    F.col("id").isNotNull() &
                    F.col("department").isNotNull() &
                    (F.trim(F.col("department")) != "")
                )
                
                reason_col = F.concat_ws("; ",
                    F.when(F.col("id").isNull(), "id is null/invalid").otherwise(F.lit("")),
                    F.when(F.col("department").isNull() | (F.trim(F.col("department")) == ""), "department is empty").otherwise(F.lit(""))
                )

            elif self.table_name == "jobs":
                # Cast id to Integer
                df_cast = df.withColumn("id", F.col("id").cast(IntegerType()))
                
                # Validation condition
                valid_cond = (
                    F.col("id").isNotNull() &
                    F.col("job").isNotNull() &
                    (F.trim(F.col("job")) != "")
                )
                
                reason_col = F.concat_ws("; ",
                    F.when(F.col("id").isNull(), "id is null/invalid").otherwise(F.lit("")),
                    F.when(F.col("job").isNull() | (F.trim(F.col("job")) == ""), "job is empty").otherwise(F.lit(""))
                )

            elif self.table_name == "hired_employees":
                # Cast integer columns
                df_cast = df.withColumn("id", F.col("id").cast(IntegerType())) \
                            .withColumn("department_id", F.col("department_id").cast(IntegerType())) \
                            .withColumn("job_id", F.col("job_id").cast(IntegerType()))
                
                # Validation condition
                valid_cond = (
                    F.col("id").isNotNull() &
                    F.col("name").isNotNull() & (F.trim(F.col("name")) != "") &
                    F.col("datetime").isNotNull() & (F.to_timestamp(F.col("datetime")).isNotNull()) &
                    F.col("department_id").isNotNull() &
                    F.col("job_id").isNotNull()
                )
                
                reason_col = F.concat_ws("; ",
                    F.when(F.col("id").isNull(), "id is null/invalid").otherwise(F.lit("")),
                    F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""), "name is empty").otherwise(F.lit("")),
                    F.when(F.col("datetime").isNull() | (F.to_timestamp(F.col("datetime")).isNull()), "datetime is empty/invalid format").otherwise(F.lit("")),
                    F.when(F.col("department_id").isNull(), "department_id is null/invalid").otherwise(F.lit("")),
                    F.when(F.col("job_id").isNull(), "job_id is null/invalid").otherwise(F.lit(""))
                )
            else:
                raise TransformerError(f"Unsupported table name: {self.table_name}")

            # Format the error reason text
            reason_col = F.regexp_replace(reason_col, "^; |; $", "")
            reason_col = F.regexp_replace(reason_col, "; ; ", "; ")

            # Split into valid and invalid dataframes
            valid_df = df_cast.filter(valid_cond)
            invalid_df = df.filter(~valid_cond).withColumn("reason", reason_col)

            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            logger.info(f"Validation summary for {self.table_name}: {valid_count} valid, {invalid_count} rejected")

            # Write valid records to DB
            if valid_count > 0:
                # Add default loaded at column
                valid_db_df = valid_df.withColumn("_loaded_at", F.current_timestamp())
                logger.info(f"Writing valid records to PostgreSQL: {self.silver_table}")
                self.write_silver(valid_db_df, mode="append")

            # Write invalid records to DB rejected_log
            if invalid_count > 0:
                logger.info(f"Writing {invalid_count} rejected records to raw.rejected_log")
                
                # Format to raw.rejected_log schema: table_name, raw_row (JSON), reason, rejected_at
                # raw_row needs to be a JSON string of the original columns
                raw_row_struct = F.struct([F.col(c) for c in df.columns])
                rejected_log_df = invalid_df.withColumn("table_name", F.lit(self.table_name)) \
                    .withColumn("raw_row", F.to_json(raw_row_struct)) \
                    .withColumn("rejected_at", F.current_timestamp()) \
                    .select("table_name", "raw_row", "reason") # rejected_at is handled by default value or we specify it if needed
                
                # Write to raw.rejected_log using JDBC
                jdbc_url, properties = self._get_postgres_config()
                rejected_log_df.write.jdbc(
                    url=jdbc_url,
                    table="raw.rejected_log",
                    mode="append",
                    properties=properties
                )

            return TransformationResult(
                success=True,
                target_table=self.silver_table,
                record_count=valid_count,
                metadata={
                    "total_csv_records": total_records,
                    "valid_records": valid_count,
                    "rejected_records": invalid_count,
                    "csv_path": self.csv_path
                }
            )

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise TransformerError(f"Failed to execute ingestion pipeline: {e}")
