-- Quality Metadata Schema
-- Tracks data quality profiling and validation results

-- Create quality_metadata schema
CREATE SCHEMA IF NOT EXISTS quality_metadata;

-- Source profiling results
CREATE TABLE IF NOT EXISTS quality_metadata.source_profiles (
    profile_id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL,
    source_type VARCHAR(50), -- file, table, api
    profile_timestamp TIMESTAMP NOT NULL,
    total_rows BIGINT,
    total_columns INTEGER,
    null_percentage NUMERIC(5,2),
    duplicate_percentage NUMERIC(5,2),
    quality_score NUMERIC(5,2),
    profile_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_source_profiles_name 
ON quality_metadata.source_profiles(source_name);

CREATE INDEX IF NOT EXISTS idx_source_profiles_timestamp 
ON quality_metadata.source_profiles(profile_timestamp DESC);

-- Pipeline validation results
CREATE TABLE IF NOT EXISTS quality_metadata.validation_runs (
    validation_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    layer VARCHAR(50) NOT NULL, -- bronze, silver, gold
    data_path VARCHAR(500),
    validation_timestamp TIMESTAMP NOT NULL,
    rules_executed INTEGER NOT NULL,
    rules_passed INTEGER NOT NULL,
    rules_failed INTEGER NOT NULL,
    passed BOOLEAN NOT NULL,
    validation_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_validation_runs_pipeline 
ON quality_metadata.validation_runs(pipeline_name);

CREATE INDEX IF NOT EXISTS idx_validation_runs_timestamp 
ON quality_metadata.validation_runs(validation_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_validation_runs_passed 
ON quality_metadata.validation_runs(passed);

-- Quality alerts (for significant quality degradation)
CREATE TABLE IF NOT EXISTS quality_metadata.quality_alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL, -- profile_degradation, validation_failure
    source_name VARCHAR(255),
    pipeline_name VARCHAR(255),
    layer VARCHAR(50),
    severity VARCHAR(20) NOT NULL, -- critical, warning, info
    message TEXT NOT NULL,
    details JSONB,
    alert_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_quality_alerts_timestamp 
ON quality_metadata.quality_alerts(alert_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_quality_alerts_acknowledged 
ON quality_metadata.quality_alerts(acknowledged);

-- Quality metrics summary (aggregated view)
CREATE OR REPLACE VIEW quality_metadata.quality_summary AS
SELECT 
    'profiling' as metric_type,
    source_name as entity_name,
    profile_timestamp as metric_timestamp,
    quality_score as score,
    null_percentage,
    duplicate_percentage
FROM quality_metadata.source_profiles
UNION ALL
SELECT 
    'validation' as metric_type,
    pipeline_name as entity_name,
    validation_timestamp as metric_timestamp,
    ROUND((rules_passed::NUMERIC / NULLIF(rules_executed, 0)) * 100, 2) as score,
    NULL as null_percentage,
    NULL as duplicate_percentage
FROM quality_metadata.validation_runs
ORDER BY metric_timestamp DESC;

-- Comments
COMMENT ON SCHEMA quality_metadata IS 'Data quality profiling and validation metadata';
COMMENT ON TABLE quality_metadata.source_profiles IS 'Source data profiling results for audit and exploration';
COMMENT ON TABLE quality_metadata.validation_runs IS 'Pipeline validation results for quality gates';
COMMENT ON TABLE quality_metadata.quality_alerts IS 'Quality alerts for degradation and failures';

-- Grant permissions (adjust based on your security requirements)
-- GRANT USAGE ON SCHEMA quality_metadata TO analytics_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA quality_metadata TO analytics_user;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Quality metadata schema initialized successfully';
END $$;
