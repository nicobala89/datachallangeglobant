"""
Bronze Layer Writer
-------------------
Lands raw, validated rows as partitioned Parquet files.

Partition layout:
  data/bronze/{table}/ingested_date={YYYY-MM-DD}/part-0.parquet

Why Parquet:
  - Schema-on-read, columnar → cheap for PySpark to scan later
  - Preserves types (int, timestamp) that CSV loses
  - Immutable audit trail — Bronze is append-only, never modified
  - Partitioned by date → PySpark can prune partitions on range scans
"""

import os
from datetime import date
from typing import List, Dict, Any

import pandas as pd
from loguru import logger


BRONZE_BASE = os.getenv("BRONZE_BASE_PATH", "data/bronze")


def write_bronze(table_name: str, rows: List[Dict[str, Any]]) -> str:
    """
    Append rows to the Bronze Parquet partition for today.

    Returns the partition directory path.
    """
    if not rows:
        return ""

    partition = f"ingested_date={date.today().isoformat()}"
    partition_path = os.path.join(BRONZE_BASE, table_name, partition)
    os.makedirs(partition_path, exist_ok=True)

    df_new = pd.DataFrame(rows)
    filepath = os.path.join(partition_path, "part-0.parquet")

    if os.path.exists(filepath):
        df_existing = pd.read_parquet(filepath)
        df_new = pd.concat([df_existing, df_new], ignore_index=True)

    df_new.to_parquet(filepath, index=False, engine="pyarrow")
    logger.info(f"[Bronze] {table_name} → {filepath}  ({len(rows)} new rows, {len(df_new)} total in partition)")
    return partition_path


def list_bronze_partitions(table_name: str) -> List[str]:
    """Return all available date partitions for a table, sorted ascending."""
    base = os.path.join(BRONZE_BASE, table_name)
    if not os.path.isdir(base):
        return []
    partitions = sorted(
        d for d in os.listdir(base)
        if d.startswith("ingested_date=") and os.path.isdir(os.path.join(base, d))
    )
    return [os.path.join(base, p) for p in partitions]


def get_latest_bronze_path(table_name: str) -> str:
    """Return the most recent partition directory, or empty string if none exists."""
    partitions = list_bronze_partitions(table_name)
    return partitions[-1] if partitions else ""
