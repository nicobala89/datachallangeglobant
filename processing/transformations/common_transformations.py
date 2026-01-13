"""
Common Transformations
Reusable transformation functions for data processing.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional, Dict, Any
from loguru import logger


def clean_nulls(
    df: DataFrame,
    columns: Optional[List[str]] = None,
    strategy: str = 'drop'
) -> DataFrame:
    """
    Handle null values in DataFrame.

    Args:
        df: Input DataFrame
        columns: Columns to clean (None = all columns)
        strategy: 'drop', 'fill_zero', 'fill_empty'

    Returns:
        DataFrame with nulls handled
    """
    if strategy == 'drop':
        df = df.dropna(subset=columns) if columns else df.dropna()
        logger.debug(f"Dropped rows with nulls in {columns or 'all columns'}")
    
    elif strategy == 'fill_zero':
        fill_dict = {col: 0 for col in (columns or df.columns)}
        df = df.fillna(fill_dict)
        logger.debug(f"Filled nulls with 0 in {columns or 'all columns'}")
    
    elif strategy == 'fill_empty':
        fill_dict = {col: '' for col in (columns or df.columns)}
        df = df.fillna(fill_dict)
        logger.debug(f"Filled nulls with empty string in {columns or 'all columns'}")
    
    return df


def standardize_dates(
    df: DataFrame,
    column: str,
    output_format: str = 'yyyy-MM-dd HH:mm:ss'
) -> DataFrame:
    """
    Standardize date column to consistent format.

    Args:
        df: Input DataFrame
        column: Date column name
        output_format: Target date format

    Returns:
        DataFrame with standardized dates
    """
    df = df.withColumn(
        column,
        F.date_format(F.col(column), output_format)
    )
    
    logger.debug(f"Standardized dates in column '{column}' to format '{output_format}'")
    return df


def deduplicate(
    df: DataFrame,
    key_columns: List[str],
    keep: str = 'last'
) -> DataFrame:
    """
    Remove duplicate records based on key columns.

    Args:
        df: Input DataFrame
        key_columns: Columns that define uniqueness
        keep: 'first' or 'last' record to keep

    Returns:
        DataFrame with duplicates removed
    """
    if keep == 'last':
        # Order by all columns descending, then take first per key
        window = Window.partitionBy(key_columns).orderBy(*[F.col(c).desc() for c in df.columns])
    else:
        # Order by all columns ascending, then take first per key
        window = Window.partitionBy(key_columns).orderBy(*[F.col(c).asc() for c in df.columns])
    
    df = df.withColumn('_row_num', F.row_number().over(window)) \
           .filter(F.col('_row_num') == 1) \
           .drop('_row_num')
    
    logger.debug(f"Deduplicated on {key_columns}, keeping {keep} record")
    return df


def add_surrogate_key(
    df: DataFrame,
    key_column: str = 'id',
    start_value: int = 1
) -> DataFrame:
    """
    Add auto-incrementing surrogate key column.

    Args:
        df: Input DataFrame
        key_column: Name for surrogate key column
        start_value: Starting value for key

    Returns:
        DataFrame with surrogate key
    """
    df = df.withColumn(key_column, F.monotonically_increasing_id() + start_value)
    
    logger.debug(f"Added surrogate key column '{key_column}'")
    return df


def enrich_with_lookup(
    df: DataFrame,
    lookup_df: DataFrame,
    join_column: str,
    join_type: str = 'left'
) -> DataFrame:
    """
    Enrich DataFrame with lookup/reference data.

    Args:
        df: Input DataFrame
        lookup_df: Lookup DataFrame
        join_column: Column to join on
        join_type: Type of join ('left', 'inner', etc.)

    Returns:
        Enriched DataFrame
    """
    df = df.join(lookup_df, join_column, join_type)
    
    logger.debug(f"Enriched with lookup data on '{join_column}' using {join_type} join")
    return df


def aggregate_metrics(
    df: DataFrame,
    group_by_columns: List[str],
    aggregations: Dict[str, str]
) -> DataFrame:
    """
    Aggregate metrics by group.

    Args:
        df: Input DataFrame
        group_by_columns: Columns to group by
        aggregations: Dict of {column: agg_function} (e.g., {'amount': 'sum'})

    Returns:
        Aggregated DataFrame
    """
    agg_exprs = [
        F.expr(f"{agg_func}({col}) as {col}_{agg_func}")
        for col, agg_func in aggregations.items()
    ]
    
    df = df.groupBy(group_by_columns).agg(*agg_exprs)
    
    logger.debug(f"Aggregated by {group_by_columns} with {len(aggregations)} metrics")
    return df


def add_calculated_column(
    df: DataFrame,
    column_name: str,
    expression: str
) -> DataFrame:
    """
    Add calculated column using SQL expression.

    Args:
        df: Input DataFrame
        column_name: Name for new column
        expression: SQL expression for calculation

    Returns:
        DataFrame with calculated column
    """
    df = df.withColumn(column_name, F.expr(expression))
    
    logger.debug(f"Added calculated column '{column_name}': {expression}")
    return df


def filter_by_condition(
    df: DataFrame,
    condition: str
) -> DataFrame:
    """
    Filter DataFrame by SQL condition.

    Args:
        df: Input DataFrame
        condition: SQL WHERE condition

    Returns:
        Filtered DataFrame
    """
    df = df.filter(condition)
    
    logger.debug(f"Filtered by condition: {condition}")
    return df


def rename_columns(
    df: DataFrame,
    column_mapping: Dict[str, str]
) -> DataFrame:
    """
    Rename columns based on mapping.

    Args:
        df: Input DataFrame
        column_mapping: Dict of {old_name: new_name}

    Returns:
        DataFrame with renamed columns
    """
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    logger.debug(f"Renamed {len(column_mapping)} columns")
    return df


def select_columns(
    df: DataFrame,
    columns: List[str]
) -> DataFrame:
    """
    Select specific columns from DataFrame.

    Args:
        df: Input DataFrame
        columns: List of column names to select

    Returns:
        DataFrame with selected columns
    """
    df = df.select(columns)
    
    logger.debug(f"Selected {len(columns)} columns")
    return df


def cast_columns(
    df: DataFrame,
    column_types: Dict[str, str]
) -> DataFrame:
    """
    Cast columns to specified data types.

    Args:
        df: Input DataFrame
        column_types: Dict of {column: type} (e.g., {'amount': 'double'})

    Returns:
        DataFrame with casted columns
    """
    for column, dtype in column_types.items():
        df = df.withColumn(column, F.col(column).cast(dtype))
    
    logger.debug(f"Casted {len(column_types)} columns")
    return df
