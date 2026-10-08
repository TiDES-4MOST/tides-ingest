-- 002: light-touch provenance (see tides_flows/provenance.py).

-- Something happened to a TiDES object. event_type values:
--   alert, manual_submission, created, selected, activated, deactivated,
--   4most_created, 4most_mag_update
CREATE TABLE IF NOT EXISTS tides_events (
    event_id         BIGSERIAL PRIMARY KEY,
    event_time       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tides_id         BIGINT NOT NULL REFERENCES tides_master(tides_id) ON DELETE CASCADE,
    event_type       TEXT NOT NULL,
    reason           TEXT,              -- e.g. redetected, stale_10d, faint_alert
    survey_id        INT,               -- survey_ids.survey_id
    survey_object_id TEXT,              -- the survey's own id (diaObjectId, ZTF name, ...)
    pipeline_id      INT,               -- pipelines.pipeline_id (the selection function + version)
    submitted_by     TEXT,              -- person / external source, for manual submissions
    details          JSONB,
    git_sha          TEXT,
    flow_run_id      TEXT
);
CREATE INDEX IF NOT EXISTS idx_tides_events_tides_id ON tides_events (tides_id);
CREATE INDEX IF NOT EXISTS idx_tides_events_time ON tides_events (event_time);

-- Every request we made to the 4MOST API (and every dry-run payload)
CREATE TABLE IF NOT EXISTS tides_4most_log (
    log_id        BIGSERIAL PRIMARY KEY,
    request_time  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tides_ids     BIGINT[] NOT NULL,     -- one, or the whole batch
    method        TEXT NOT NULL,         -- POST | PATCH | GET
    url           TEXT,
    payload       JSONB,
    status_code   INT,                   -- NULL: no response (timeout, connection error) or dry_run
    outcome       TEXT NOT NULL,         -- ok | rejected | ambiguous | retry | dry_run
    response_text TEXT,                  -- first 4000 characters
    git_sha       TEXT,
    flow_run_id   TEXT
);
CREATE INDEX IF NOT EXISTS idx_tides_4most_log_ids ON tides_4most_log USING gin (tides_ids);

-- Pipelines stay human-maintained. Two optional columns to make edits self-documenting,
-- and a row for targets that a person or an external source asked us to observe.
ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS notes TEXT;
INSERT INTO pipelines (pipeline_name, version, notes)
VALUES ('manual', 'v1.0', 'Targets given to us by a person or external source; see tides_events.submitted_by')
ON CONFLICT (pipeline_name, version) DO NOTHING;
