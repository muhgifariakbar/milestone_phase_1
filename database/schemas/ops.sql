-- Ops layer: local pipeline and quality metadata.
--
-- Every stage of dags/dag.py writes here so a run can be audited without
-- reading Airflow logs: what ran, how long each stage took, which quality
-- checks passed, and which manifest files failed checksum validation.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline_run_id   TEXT PRIMARY KEY,
    triggered_by      TEXT NOT NULL DEFAULT 'manual',
    status            TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'succeeded', 'failed')),
    current_stage     TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    error_message     TEXT
);

CREATE TABLE IF NOT EXISTS ops.stage_events (
    id               BIGSERIAL PRIMARY KEY,
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    stage            TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
    row_count        INTEGER,
    error_message    TEXT,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.manifest_validation (
    id                 BIGSERIAL PRIMARY KEY,
    pipeline_run_id    TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    relative_path      TEXT NOT NULL,
    expected_checksum  TEXT,
    actual_checksum    TEXT,
    is_valid           BOOLEAN NOT NULL,
    detail             TEXT,
    checked_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.quality_check_results (
    id               BIGSERIAL PRIMARY KEY,
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    check_name       TEXT NOT NULL,
    check_category   TEXT NOT NULL,
    is_blocking       BOOLEAN NOT NULL DEFAULT true,
    passed           BOOLEAN NOT NULL,
    expected_value   TEXT,
    actual_value     TEXT,
    detail           TEXT,
    checked_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_stage_events_run ON ops.stage_events (pipeline_run_id);
CREATE INDEX IF NOT EXISTS ix_manifest_validation_run ON ops.manifest_validation (pipeline_run_id);
CREATE INDEX IF NOT EXISTS ix_quality_check_results_run ON ops.quality_check_results (pipeline_run_id);
