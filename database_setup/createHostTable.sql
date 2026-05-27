-- Create the tides_host table to store host galaxy information
CREATE TABLE IF NOT EXISTS tides_host (
    host_id BIGSERIAL PRIMARY KEY,
    tides_id BIGINT REFERENCES tides_master(tides_id) ON DELETE CASCADE,
    host_name VARCHAR DEFAULT NULL,
    ra double precision,
    dec double precision,
    mag JSONB,
    rank INT,
    selection_fn INT REFERENCES pipelines(pipeline_id),
    metadata JSONB,
    UNIQUE (tides_id, rank, selection_fn)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_tides_host_tides_id ON tides_host(tides_id);
CREATE INDEX IF NOT EXISTS idx_tides_host_name ON tides_host(host_name);
CREATE INDEX IF NOT EXISTS idx_tides_host_ra_dec ON tides_host (q3c_ang2ipix(ra, dec));
