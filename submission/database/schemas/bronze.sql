CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_records (
    bronze_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_record_id TEXT,
    payload JSONB NOT NULL,
    file_sha256 TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_file, source_row_number, file_sha256)
);