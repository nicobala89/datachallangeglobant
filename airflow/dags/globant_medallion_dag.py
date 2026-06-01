"""
Globant Medallion Pipeline DAG
================================
Implements the full Bronze → Silver → Gold pipeline.

  Bronze  : Raw CSVs landed as partitioned Parquet (append-only, schema preserved)
  Silver  : Validated, typed rows in PostgreSQL raw.* (pandas + psycopg2)
  Gold    : Aggregated analytics marts in PostgreSQL analytics.* (dbt-core)

Design decisions
-----------------
- Bronze is immutable: landing new data never overwrites existing partitions.
- Silver uses ON CONFLICT (id) DO NOTHING so re-runs are idempotent.
- Gold uses dbt table materialisation (TRUNCATE + INSERT) for a consistent snapshot.
- Tasks are ordered by FK dependency: departments → jobs → hired_employees.
- dbt only runs after all three Silver tables are loaded.
- At scale, Bronze→Silver would move to a SparkSubmitOperator targeting a cluster;
  the dbt Gold layer stays the same since dbt pushes compute down into the DB.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "globant",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

CSV_DIR    = os.getenv("CSV_DIR", "data/csv")
BRONZE_DIR = os.getenv("BRONZE_BASE_PATH", "data/bronze")
DBT_DIR    = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")


# ── Stage 1: Bronze ────────────────────────────────────────────────────────────

def land_to_bronze(**ctx):
    """
    Read each CSV and write it as a Parquet partition in the Bronze layer.
    Purely a landing step — no validation, no transformation.
    The raw shape of the source is preserved exactly.
    """
    import pandas as pd

    tables = {
        "departments":     ["id", "department"],
        "jobs":            ["id", "job"],
        "hired_employees": ["id", "name", "datetime", "department_id", "job_id"],
    }
    partition = f"ingested_date={ctx['ds']}"

    for table, cols in tables.items():
        csv_path = os.path.join(CSV_DIR, f"{table}.csv")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Source CSV not found: {csv_path}")

        df = pd.read_csv(csv_path, header=None, names=cols)
        out_dir = os.path.join(BRONZE_DIR, table, partition)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "part-0.parquet")
        df.to_parquet(out_path, index=False, engine="pyarrow")
        print(f"[Bronze] {table} → {out_path}  ({len(df)} rows)")


# ── Stage 2: Silver ────────────────────────────────────────────────────────────

def bronze_to_silver(table_name: str, **ctx):
    """
    Read validated rows from Bronze Parquet and write to Silver (PostgreSQL raw.*).

    Idempotency:
      - new row     → inserted
      - exact match → skipped (no write)
      - same id, different data → logged to raw.rejected_log as CONFLICT, not written
      - invalid row → logged to raw.rejected_log as REJECTED

    At scale this step would be replaced by a SparkSubmitOperator targeting a cluster.
    """
    import json
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values, RealDictCursor
    from pydantic import ValidationError
    from datetime import datetime as dt_type

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from ingestion.schemas.globant_schemas import TABLE_SCHEMAS

    partition = f"ingested_date={ctx['ds']}"
    parquet_file = os.path.join(BRONZE_DIR, table_name, partition, "part-0.parquet")

    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Bronze Parquet not found: {parquet_file}")

    df = pd.read_parquet(parquet_file)

    int_cols = {"id", "department_id", "job_id"}
    for col in int_cols.intersection(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    schema = TABLE_SCHEMAS[table_name]
    valid_rows, rejected_log = [], []

    for _, row in df.iterrows():
        raw = {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for col in int_cols.intersection(raw.keys()):
            if raw[col] is not None:
                raw[col] = int(raw[col])
        try:
            validated = schema(**raw)
            valid_rows.append(validated.model_dump())
        except ValidationError as e:
            reason = "; ".join(f"{err['loc'][0]}: {err['msg']}" for err in e.errors())
            rejected_log.append((table_name, json.dumps(raw, default=str), reason))
        except Exception as e:
            rejected_log.append((table_name, json.dumps(raw, default=str), str(e)))

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "globant_analytics"),
        user=os.getenv("POSTGRES_USER", "globant"),
        password=os.getenv("POSTGRES_PASSWORD", "globant_password"),
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ── Idempotency check ────────────────────────────────────────────────────
    def _normalize(val) -> str:
        if val is None:
            return ""
        if isinstance(val, dt_type):
            return val.replace(tzinfo=None).isoformat(timespec="seconds")
        if isinstance(val, str):
            try:
                parsed = dt_type.fromisoformat(val.replace("Z", "+00:00"))
                return parsed.replace(tzinfo=None).isoformat(timespec="seconds")
            except ValueError:
                pass
        return str(val)

    new_rows, skipped, conflicts = [], 0, 0

    if valid_rows:
        incoming_ids = [r["id"] for r in valid_rows]
        cur.execute(
            f"SELECT * FROM raw.{table_name} WHERE id = ANY(%s)",
            (incoming_ids,),
        )
        existing = {row["id"]: dict(row) for row in cur.fetchall()}

        for row in valid_rows:
            row_id = row["id"]
            if row_id not in existing:
                new_rows.append(row)
            else:
                db_row = {k: v for k, v in existing[row_id].items() if not k.startswith("_")}
                changes = {
                    k: {"current": db_row.get(k), "incoming": row[k]}
                    for k in row
                    if _normalize(db_row.get(k)) != _normalize(row[k])
                }
                if not changes:
                    skipped += 1
                else:
                    conflicts += 1
                    diff_str = ", ".join(
                        f"{k}: {v['current']!r} → {v['incoming']!r}" for k, v in changes.items()
                    )
                    rejected_log.append((
                        table_name,
                        json.dumps(row, default=str),
                        f"CONFLICT id={row_id}: {diff_str}",
                    ))

    # ── Write to rejected_log ────────────────────────────────────────────────
    if rejected_log:
        execute_values(
            cur,
            "INSERT INTO raw.rejected_log (table_name, raw_row, reason) VALUES %s",
            rejected_log,
        )

    # ── Insert new rows only ─────────────────────────────────────────────────
    if new_rows:
        columns = list(new_rows[0].keys())
        execute_values(
            cur,
            f"INSERT INTO raw.{table_name} ({', '.join(columns)}) VALUES %s",
            [tuple(r[c] for c in columns) for r in new_rows],
        )

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"[Silver] {table_name}: "
        f"{len(new_rows)} inserted, {skipped} skipped (exact duplicate), "
        f"{conflicts} conflicts, {len(rejected_log) - conflicts} invalid"
    )


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    "globant_medallion_pipeline",
    default_args=default_args,
    description="Bronze → Silver (pandas) → Gold (dbt) medallion pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["globant", "medallion", "bronze", "silver", "gold", "dbt"],
) as dag:

    # Stage 1 — land all three CSVs atomically
    t_bronze = PythonOperator(
        task_id="land_csv_to_bronze",
        python_callable=land_to_bronze,
    )

    # Stage 2 — Silver loads in FK-safe order (departments and jobs in parallel)
    t_silver_dept = PythonOperator(
        task_id="bronze_to_silver_departments",
        python_callable=bronze_to_silver,
        op_kwargs={"table_name": "departments"},
    )
    t_silver_jobs = PythonOperator(
        task_id="bronze_to_silver_jobs",
        python_callable=bronze_to_silver,
        op_kwargs={"table_name": "jobs"},
    )
    t_silver_emp = PythonOperator(
        task_id="bronze_to_silver_hired_employees",
        python_callable=bronze_to_silver,
        op_kwargs={"table_name": "hired_employees"},
    )

    # Stage 3 — Gold via dbt (runs after all Silver tables are ready)
    t_gold = BashOperator(
        task_id="dbt_run_gold_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_DIR} --select marts "
            f"--vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
        ),
        env={
            "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "localhost"),
            "POSTGRES_PORT":     os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB":       os.getenv("POSTGRES_DB", "globant_analytics"),
            "POSTGRES_USER":     os.getenv("POSTGRES_USER", "globant"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "globant_password"),
            "PATH":              os.getenv("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
        append_env=True,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    #
    #   land_csv_to_bronze
    #        ├── bronze_to_silver_departments ─┐
    #        └── bronze_to_silver_jobs         ├── bronze_to_silver_hired_employees
    #                                          │
    #                                          └── dbt_run_gold_marts
    #
    t_bronze >> [t_silver_dept, t_silver_jobs]
    [t_silver_dept, t_silver_jobs] >> t_silver_emp
    t_silver_emp >> t_gold
