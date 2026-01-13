"""
Data Quality Checks
Implements validation rules to ensure data quality before publishing.
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger


class DataQualityChecker:
    """
    Validates data quality using configurable rules.
    """

    def __init__(self, fail_on_error: bool = True):
        """
        Initialize the data quality checker.

        Args:
            fail_on_error: Whether to raise exceptions on validation failures
        """
        self.fail_on_error = fail_on_error
        self.validation_results = []

    def check_schema(
        self,
        df: pd.DataFrame,
        expected_columns: List[str],
        check_name: str = "schema_check"
    ) -> bool:
        """
        Validate that DataFrame has expected columns.

        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names
            check_name: Name of the check for logging

        Returns:
            True if validation passes
        """
        missing_columns = set(expected_columns) - set(df.columns)
        
        if missing_columns:
            message = f"{check_name} FAILED: Missing columns: {missing_columns}"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        logger.info(f"{check_name} PASSED: All expected columns present")
        self.validation_results.append({"check": check_name, "status": "PASSED"})
        return True

    def check_nulls(
        self,
        df: pd.DataFrame,
        columns: List[str],
        max_null_percentage: float = 0.0,
        check_name: str = "null_check"
    ) -> bool:
        """
        Validate that columns don't exceed null threshold.

        Args:
            df: DataFrame to validate
            columns: Columns to check for nulls
            max_null_percentage: Maximum allowed null percentage (0-100)
            check_name: Name of the check for logging

        Returns:
            True if validation passes
        """
        total_rows = len(df)
        failed_columns = []
        
        for col in columns:
            null_count = df[col].isna().sum()
            null_percentage = (null_count / total_rows) * 100
            
            if null_percentage > max_null_percentage:
                failed_columns.append(f"{col} ({null_percentage:.2f}% nulls)")
        
        if failed_columns:
            message = f"{check_name} FAILED: Columns exceed null threshold: {failed_columns}"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        logger.info(f"{check_name} PASSED: Null checks passed for all columns")
        self.validation_results.append({"check": check_name, "status": "PASSED"})
        return True

    def check_volume(
        self,
        df: pd.DataFrame,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
        check_name: str = "volume_check"
    ) -> bool:
        """
        Validate that DataFrame has expected row count.

        Args:
            df: DataFrame to validate
            min_rows: Minimum expected rows
            max_rows: Maximum expected rows
            check_name: Name of the check for logging

        Returns:
            True if validation passes
        """
        row_count = len(df)
        
        if min_rows and row_count < min_rows:
            message = f"{check_name} FAILED: Row count {row_count} below minimum {min_rows}"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        if max_rows and row_count > max_rows:
            message = f"{check_name} FAILED: Row count {row_count} exceeds maximum {max_rows}"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        logger.info(f"{check_name} PASSED: Row count {row_count} within expected range")
        self.validation_results.append({"check": check_name, "status": "PASSED"})
        return True

    def check_freshness(
        self,
        df: pd.DataFrame,
        timestamp_column: str,
        max_age_hours: int = 24,
        check_name: str = "freshness_check"
    ) -> bool:
        """
        Validate that data is fresh (recent).

        Args:
            df: DataFrame to validate
            timestamp_column: Column containing timestamps
            max_age_hours: Maximum allowed age in hours
            check_name: Name of the check for logging

        Returns:
            True if validation passes
        """
        if timestamp_column not in df.columns:
            message = f"{check_name} FAILED: Timestamp column '{timestamp_column}' not found"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        max_timestamp = pd.to_datetime(df[timestamp_column]).max()
        age = datetime.now() - max_timestamp
        
        if age > timedelta(hours=max_age_hours):
            message = f"{check_name} FAILED: Data is {age.total_seconds() / 3600:.1f} hours old (max: {max_age_hours})"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        logger.info(f"{check_name} PASSED: Data is {age.total_seconds() / 3600:.1f} hours old")
        self.validation_results.append({"check": check_name, "status": "PASSED"})
        return True

    def check_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        check_name: str = "duplicate_check"
    ) -> bool:
        """
        Validate that there are no duplicate records.

        Args:
            df: DataFrame to validate
            key_columns: Columns that define uniqueness
            check_name: Name of the check for logging

        Returns:
            True if validation passes
        """
        duplicate_count = df.duplicated(subset=key_columns).sum()
        
        if duplicate_count > 0:
            message = f"{check_name} FAILED: Found {duplicate_count} duplicate records"
            logger.error(message)
            self.validation_results.append({"check": check_name, "status": "FAILED", "message": message})
            
            if self.fail_on_error:
                raise ValueError(message)
            return False
        
        logger.info(f"{check_name} PASSED: No duplicates found")
        self.validation_results.append({"check": check_name, "status": "PASSED"})
        return True

    def get_results(self) -> List[Dict]:
        """
        Get all validation results.

        Returns:
            List of validation results
        """
        return self.validation_results


# Example usage
if __name__ == "__main__":
    # Create sample data
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', None, 'David', 'Eve'],
        'created_at': pd.date_range(start='2026-01-13', periods=5, freq='H')
    })
    
    # Run quality checks
    checker = DataQualityChecker(fail_on_error=False)
    
    checker.check_schema(df, expected_columns=['id', 'name', 'created_at'])
    checker.check_nulls(df, columns=['id'], max_null_percentage=0)
    checker.check_volume(df, min_rows=1, max_rows=1000)
    checker.check_freshness(df, timestamp_column='created_at', max_age_hours=48)
    checker.check_duplicates(df, key_columns=['id'])
    
    # Print results
    print("\nValidation Results:")
    for result in checker.get_results():
        print(f"  {result}")
