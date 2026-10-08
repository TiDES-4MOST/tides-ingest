-- The per-survey staging table. Explicit column types (no guessing from pandas dtypes).
-- ON COMMIT DROP: it disappears with the survey's transaction.
-- tides_id is filled in by matchStageToMaster.sql: the master row each staged alert belongs to.
DROP TABLE IF EXISTS tides_stage;
CREATE TEMPORARY TABLE tides_stage (
    stage_row     BIGINT PRIMARY KEY,
    object_id     TEXT NOT NULL,
    survey_id     INT NOT NULL,
    pipeline_id   INT NOT NULL,
    ra            DOUBLE PRECISION NOT NULL,
    dec           DOUBLE PRECISION NOT NULL,
    jdmin         DOUBLE PRECISION,
    jdmax         DOUBLE PRECISION NOT NULL,
    latest_filter JSONB NOT NULL,     -- {band: MJD of latest_mag}
    latest_mag    JSONB NOT NULL,     -- {band: mag}
    n_sources     JSONB NOT NULL,     -- {band: count}
    tides_id      BIGINT
) ON COMMIT DROP;
