-- Silver Layer Schema Setup
-- Creates schemas and tables for the curated silver layer

-- Create silver schema for curated data
CREATE SCHEMA IF NOT EXISTS silver;

-- Create silver_metadata schema for transformation tracking
CREATE SCHEMA IF NOT EXISTS silver_metadata;

-- Transformation runs metadata table
CREATE TABLE IF NOT EXISTS silver_metadata.transformation_runs (
    run_id SERIAL PRIMARY KEY,
    transformer_name VARCHAR(255) NOT NULL,
    bronze_source_path VARCHAR(500) NOT NULL,
    silver_target_table VARCHAR(255) NOT NULL,
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    records_processed INTEGER,
    execution_time_seconds INTEGER,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    transformation_version VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transformation_runs_timestamp 
ON silver_metadata.transformation_runs(run_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transformation_runs_transformer 
ON silver_metadata.transformation_runs(transformer_name);

-- Example silver table: sales_transactions
CREATE TABLE IF NOT EXISTS silver.sales_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    customer_email VARCHAR(255),
    notes TEXT,
    -- Silver metadata columns
    _silver_load_timestamp TIMESTAMP NOT NULL,
    _bronze_source_path VARCHAR(500),
    _transformer_name VARCHAR(255),
    _transformation_version VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sales_transactions_customer 
ON silver.sales_transactions(customer_id);

CREATE INDEX IF NOT EXISTS idx_sales_transactions_date 
ON silver.sales_transactions(transaction_date DESC);

CREATE INDEX IF NOT EXISTS idx_sales_transactions_load_timestamp 
ON silver.sales_transactions(_silver_load_timestamp DESC);

-- Comments for documentation
COMMENT ON SCHEMA silver IS 'Curated data layer - cleaned, validated, and business-ready';
COMMENT ON SCHEMA silver_metadata IS 'Metadata and lineage tracking for silver layer transformations';

COMMENT ON TABLE silver_metadata.transformation_runs IS 'Tracks all transformation executions with metrics and lineage';
COMMENT ON TABLE silver.sales_transactions IS 'Curated sales transaction data from bronze layer';

-- Grant permissions (adjust based on your security requirements)
-- GRANT USAGE ON SCHEMA silver TO analytics_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA silver TO analytics_user;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Silver layer schema initialized successfully';
END $$;
