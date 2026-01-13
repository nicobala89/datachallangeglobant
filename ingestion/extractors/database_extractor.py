"""
Database Data Extractor
Handles data extraction from relational databases with incremental loading support.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, Dict
from datetime import datetime
from loguru import logger


class DatabaseExtractor:
    """
    Generic database extractor for relational databases.
    """

    def __init__(self, connection_string: str):
        """
        Initialize the database extractor.

        Args:
            connection_string: SQLAlchemy connection string
        """
        self.engine = create_engine(connection_string)
        logger.info("Database connection established")

    def extract_table(
        self,
        table_name: str,
        schema: Optional[str] = None,
        columns: Optional[list] = None
    ) -> pd.DataFrame:
        """
        Extract an entire table from the database.

        Args:
            table_name: Name of the table to extract
            schema: Optional schema name
            columns: Optional list of columns to extract

        Returns:
            DataFrame containing the extracted data
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        if columns:
            column_list = ", ".join(columns)
            query = f"SELECT {column_list} FROM {full_table_name}"
        else:
            query = f"SELECT * FROM {full_table_name}"
        
        logger.info(f"Extracting table: {full_table_name}")
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Extracted {len(df)} rows from {full_table_name}")
        
        return df

    def extract_incremental(
        self,
        table_name: str,
        timestamp_column: str,
        last_run_timestamp: Optional[datetime] = None,
        schema: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract data incrementally based on a timestamp column.

        Args:
            table_name: Name of the table to extract
            timestamp_column: Column name for filtering by timestamp
            last_run_timestamp: Timestamp of last successful run
            schema: Optional schema name

        Returns:
            DataFrame containing records updated since last run
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        if last_run_timestamp:
            query = text(
                f"SELECT * FROM {full_table_name} "
                f"WHERE {timestamp_column} > :last_timestamp "
                f"ORDER BY {timestamp_column}"
            )
            params = {"last_timestamp": last_run_timestamp}
            logger.info(f"Extracting incremental data since {last_run_timestamp}")
        else:
            query = text(f"SELECT * FROM {full_table_name}")
            params = {}
            logger.info("Performing full extraction (no previous timestamp)")
        
        df = pd.read_sql(query, self.engine, params=params)
        logger.info(f"Extracted {len(df)} rows from {full_table_name}")
        
        return df

    def extract_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Extract data using a custom SQL query.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            DataFrame containing the query results
        """
        logger.info("Executing custom query")
        
        df = pd.read_sql(text(query), self.engine, params=params or {})
        logger.info(f"Query returned {len(df)} rows")
        
        return df

    def save_to_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save DataFrame to Parquet format.

        Args:
            df: DataFrame to save
            output_path: Path to output file
        """
        df.to_parquet(output_path, index=False, compression='snappy')
        logger.info(f"Saved {len(df)} rows to {output_path}")

    def close(self):
        """Close the database connection."""
        self.engine.dispose()
        logger.info("Database connection closed")


# Example usage
if __name__ == "__main__":
    # Example: Extract data from PostgreSQL
    connection_string = "postgresql://user:password@localhost:5432/database"
    extractor = DatabaseExtractor(connection_string)
    
    try:
        # Extract full table
        df = extractor.extract_table("users", schema="public")
        
        # Save to Parquet
        extractor.save_to_parquet(df, "/tmp/users.parquet")
        
    finally:
        extractor.close()
