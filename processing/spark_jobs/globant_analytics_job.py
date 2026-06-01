import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def run_analytics_marts(spark: SparkSession):
    """
    Computes both Challenge #2 analytics marts using PySpark
    and materializes them in the postgres analytics schema.
    """
    # 1. Read configuration from environment
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', 5432)
    database = os.getenv('POSTGRES_DB', 'globant_analytics')
    user = os.getenv('POSTGRES_USER', 'globant')
    password = os.getenv('POSTGRES_PASSWORD', 'globant_password')
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    props = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    print("Reading raw tables from PostgreSQL...")
    df_emp = spark.read.jdbc(url=jdbc_url, table="raw.hired_employees", properties=props)
    df_dept = spark.read.jdbc(url=jdbc_url, table="raw.departments", properties=props)
    df_job = spark.read.jdbc(url=jdbc_url, table="raw.jobs", properties=props)

    # Filter employees hired in 2021
    # Cast datetime string to timestamp and extract year
    df_emp_2021 = df_emp.filter(F.year(F.to_timestamp("datetime")) == 2021)

    # ---------------------------------------------------------
    # Mart 1: Hires by job and department in 2021 split by quarter
    # ---------------------------------------------------------
    print("Computing Mart 1: Hires by job and department in 2021 by quarter...")
    df_joined_1 = df_emp_2021 \
        .join(df_dept, df_emp_2021.department_id == df_dept.id) \
        .join(df_job, df_emp_2021.job_id == df_job.id) \
        .withColumn("quarter", F.quarter(F.to_timestamp("datetime")))

    # Pivot quarter and aggregate hires count
    mart_1 = df_joined_1.groupBy("department", "job").pivot("quarter", [1, 2, 3, 4]).count()
    
    # Fill nulls with 0 and rename pivoted columns
    mart_1 = mart_1.na.fill(0) \
        .withColumnRenamed("1", "q1") \
        .withColumnRenamed("2", "q2") \
        .withColumnRenamed("3", "q3") \
        .withColumnRenamed("4", "q4") \
        .withColumn("_refreshed_at", F.current_timestamp()) \
        .orderBy("department", "job")

    print(f"Writing Mart 1 to analytics.mart_hires_by_quarter...")
    mart_1.write.jdbc(url=jdbc_url, table="analytics.mart_hires_by_quarter", mode="overwrite", properties=props)

    # ---------------------------------------------------------
    # Mart 2: Departments above mean hires in 2021
    # ---------------------------------------------------------
    print("Computing Mart 2: Departments above mean hires in 2021...")
    
    # Left join to include departments with 0 hires
    dept_counts = df_dept.join(df_emp_2021, df_dept.id == df_emp_2021.department_id, "left") \
        .groupBy(df_dept.id, "department") \
        .agg(F.count(df_emp_2021.id).alias("hired"))

    # Compute mean hires per department in 2021
    mean_row = dept_counts.select(F.avg("hired")).first()
    mean_hires = mean_row[0] if mean_row and mean_row[0] is not None else 0.0
    print(f"Computed mean hires per department in 2021: {mean_hires}")

    # Filter above mean departments
    mart_2 = dept_counts.filter(F.col("hired") > mean_hires) \
        .withColumn("_refreshed_at", F.current_timestamp()) \
        .orderBy(F.col("hired").desc())

    print("Writing Mart 2 to analytics.mart_departments_above_mean...")
    mart_2.write.jdbc(url=jdbc_url, table="analytics.mart_departments_above_mean", mode="overwrite", properties=props)
    
    print("PySpark Mart execution finished successfully!")

if __name__ == "__main__":
    # If run standalone, build SparkSession and execute
    spark_session = SparkSession.builder \
        .appName("Globant Analytics Marts - Standalone") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
        
    try:
        run_analytics_marts(spark_session)
    finally:
        spark_session.stop()
