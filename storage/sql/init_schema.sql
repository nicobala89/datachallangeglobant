-- Gorigami Data Framework - Initial Schema Setup
-- This script creates the foundational schema for the analytics serving layer

-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS consumption;

-- Example: Create a metadata table for tracking pipeline runs
CREATE TABLE IF NOT EXISTS curated.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    run_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER,
    execution_time_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_name 
ON curated.pipeline_runs(pipeline_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_date 
ON curated.pipeline_runs(run_date DESC);

-- Example: Create a data quality metrics table
CREATE TABLE IF NOT EXISTS curated.data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    metric_value NUMERIC,
    threshold_value NUMERIC,
    message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_dataset 
ON curated.data_quality_metrics(dataset_name, check_timestamp DESC);

-- Example: Create a sample consumption-ready view
-- This demonstrates how to expose curated data for reporting

-- COMMENT ON SCHEMA raw IS 'Raw data from source systems, minimal transformation';
-- COMMENT ON SCHEMA curated IS 'Cleaned and validated data, ready for analytics';
-- COMMENT ON SCHEMA consumption IS 'Business-friendly views and aggregations for reporting';

-- Grant appropriate permissions (adjust based on your security requirements)
-- GRANT USAGE ON SCHEMA raw TO airflow_user;
-- GRANT USAGE ON SCHEMA curated TO airflow_user;
-- GRANT USAGE ON SCHEMA consumption TO reporting_user;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Gorigami Data Framework schema initialized successfully';
END $$;
