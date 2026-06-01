-- Raw schema: ingested data lives here
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.departments (
    id           INTEGER PRIMARY KEY,
    department   VARCHAR(255) NOT NULL,
    _loaded_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.jobs (
    id    INTEGER PRIMARY KEY,
    job   VARCHAR(255) NOT NULL,
    _loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.hired_employees (
    id              INTEGER PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    datetime        TIMESTAMP NOT NULL,
    department_id   INTEGER NOT NULL REFERENCES raw.departments(id),
    job_id          INTEGER NOT NULL REFERENCES raw.jobs(id),
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.rejected_log (
    id           SERIAL PRIMARY KEY,
    table_name   VARCHAR(50) NOT NULL,
    raw_row      JSONB NOT NULL,
    reason       TEXT NOT NULL,
    rejected_at  TIMESTAMP DEFAULT NOW()
);

-- Analytics schema: dbt materialises here
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.mart_hires_by_quarter (
    department  VARCHAR(255),
    job         VARCHAR(255),
    hire_year   INTEGER,
    q1          INTEGER DEFAULT 0,
    q2          INTEGER DEFAULT 0,
    q3          INTEGER DEFAULT 0,
    q4          INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analytics.mart_departments_above_mean (
    id          INTEGER,
    department  VARCHAR(255),
    hired       INTEGER,
    _refreshed_at TIMESTAMP DEFAULT NOW()
);
