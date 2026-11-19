CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    current_stage TEXT,
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at_utc TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ops.raw_files (
    pipeline_run_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_run_id, file_path)
);

CREATE TABLE IF NOT EXISTS ops.quality_results (
    pipeline_run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    failed_rows NUMERIC NOT NULL,
    passed BOOLEAN NOT NULL,
    checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_run_id, check_name)
);