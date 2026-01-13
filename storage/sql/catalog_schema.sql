-- Data Catalog Schema
-- Provides metadata management, discovery, and governance

-- Create catalog schema
CREATE SCHEMA IF NOT EXISTS catalog;

-- Datasets catalog table
CREATE TABLE IF NOT EXISTS catalog.datasets (
    dataset_id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) UNIQUE NOT NULL,
    layer VARCHAR(50) NOT NULL, -- bronze, silver, gold
    format VARCHAR(50), -- parquet, table, view
    location VARCHAR(500) NOT NULL,
    description TEXT,
    owner VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_accessed_at TIMESTAMP,
    access_count INTEGER DEFAULT 0,
    row_count BIGINT,
    size_mb NUMERIC(10, 2),
    tags JSONB DEFAULT '[]'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_datasets_name ON catalog.datasets(dataset_name);
CREATE INDEX IF NOT EXISTS idx_datasets_layer ON catalog.datasets(layer);
CREATE INDEX IF NOT EXISTS idx_datasets_owner ON catalog.datasets(owner);
CREATE INDEX IF NOT EXISTS idx_datasets_tags ON catalog.datasets USING GIN(tags);

-- Dataset schemas table
CREATE TABLE IF NOT EXISTS catalog.dataset_schemas (
    schema_id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES catalog.datasets(dataset_id) ON DELETE CASCADE,
    schema_version VARCHAR(50) NOT NULL,
    schema_definition JSONB NOT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, schema_version)
);

CREATE INDEX IF NOT EXISTS idx_dataset_schemas_dataset ON catalog.dataset_schemas(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_schemas_current ON catalog.dataset_schemas(dataset_id, is_current);

-- Data lineage table
CREATE TABLE IF NOT EXISTS catalog.dataset_lineage (
    lineage_id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES catalog.datasets(dataset_id) ON DELETE CASCADE,
    source_dataset_id INTEGER REFERENCES catalog.datasets(dataset_id) ON DELETE CASCADE,
    transformation_name VARCHAR(255),
    transformation_type VARCHAR(50), -- extractor, transformer, aggregation
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lineage_dataset ON catalog.dataset_lineage(dataset_id);
CREATE INDEX IF NOT EXISTS idx_lineage_source ON catalog.dataset_lineage(source_dataset_id);

-- Access policies table
CREATE TABLE IF NOT EXISTS catalog.access_policies (
    policy_id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES catalog.datasets(dataset_id) ON DELETE CASCADE,
    policy_type VARCHAR(50) NOT NULL, -- public, restricted, private
    allowed_roles JSONB DEFAULT '[]'::jsonb,
    allowed_users JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_access_policies_dataset ON catalog.access_policies(dataset_id);

-- Governance tags table
CREATE TABLE IF NOT EXISTS catalog.governance_tags (
    tag_id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES catalog.datasets(dataset_id) ON DELETE CASCADE,
    tag_type VARCHAR(50) NOT NULL, -- pii, sensitive, compliance, gdpr
    tag_value VARCHAR(255),
    column_name VARCHAR(255), -- Optional: tag specific column
    confidence NUMERIC(3, 2), -- Confidence score for auto-detected tags
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_governance_tags_dataset ON catalog.governance_tags(dataset_id);
CREATE INDEX IF NOT EXISTS idx_governance_tags_type ON catalog.governance_tags(tag_type);

-- Dataset statistics table
CREATE TABLE IF NOT EXISTS catalog.dataset_statistics (
    stat_id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES catalog.datasets(dataset_id) ON DELETE CASCADE,
    stat_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count BIGINT,
    column_count INTEGER,
    null_percentage NUMERIC(5, 2),
    duplicate_percentage NUMERIC(5, 2),
    quality_score NUMERIC(5, 2),
    statistics_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_dataset_statistics_dataset ON catalog.dataset_statistics(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_statistics_timestamp ON catalog.dataset_statistics(stat_timestamp DESC);

-- Audit log table
CREATE TABLE IF NOT EXISTS catalog.audit_log (
    audit_id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES catalog.datasets(dataset_id) ON DELETE SET NULL,
    user_name VARCHAR(100),
    action VARCHAR(50) NOT NULL, -- view, download, update, delete
    details JSONB,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_log_dataset ON catalog.audit_log(dataset_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_user ON catalog.audit_log(user_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON catalog.audit_log(timestamp DESC);

-- Views for common queries

-- Datasets with latest schema
CREATE OR REPLACE VIEW catalog.datasets_with_schema AS
SELECT 
    d.*,
    s.schema_version,
    s.schema_definition
FROM catalog.datasets d
LEFT JOIN catalog.dataset_schemas s ON d.dataset_id = s.dataset_id AND s.is_current = TRUE;

-- Datasets with lineage count
CREATE OR REPLACE VIEW catalog.datasets_with_lineage AS
SELECT 
    d.*,
    COUNT(DISTINCT l.source_dataset_id) as upstream_count,
    COUNT(DISTINCT l2.dataset_id) as downstream_count
FROM catalog.datasets d
LEFT JOIN catalog.dataset_lineage l ON d.dataset_id = l.dataset_id
LEFT JOIN catalog.dataset_lineage l2 ON d.dataset_id = l2.source_dataset_id
GROUP BY d.dataset_id;

-- Comments
COMMENT ON SCHEMA catalog IS 'Data catalog for discovery, metadata, and governance';
COMMENT ON TABLE catalog.datasets IS 'Central registry of all datasets across bronze/silver/gold layers';
COMMENT ON TABLE catalog.dataset_lineage IS 'Tracks data lineage and transformations';
COMMENT ON TABLE catalog.access_policies IS 'Access control policies for datasets';
COMMENT ON TABLE catalog.governance_tags IS 'Governance and compliance tags (PII, sensitive data, etc.)';

-- Grant permissions (adjust based on your security requirements)
-- GRANT USAGE ON SCHEMA catalog TO catalog_api;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA catalog TO catalog_api;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Catalog schema initialized successfully';
END $$;
