-- 1. Create a table to store unique host galaxies (deduplicated)
CREATE TABLE IF NOT EXISTS tides_host_catalog (
    host_id BIGSERIAL PRIMARY KEY,
    host_name VARCHAR UNIQUE NOT NULL, -- e.g. SDSS/NED/SIMBAD catalog ID
    ra double precision NOT NULL,
    dec double precision NOT NULL,
    mag JSONB -- e.g. {"Mag": 18.5, "MagErr": 0.1, "MagFilter": "g"}
);
CREATE INDEX IF NOT EXISTS idx_tides_host_catalog_name ON tides_host_catalog(host_name);
CREATE INDEX IF NOT EXISTS idx_tides_host_catalog_ra_dec ON tides_host_catalog(q3c_ang2ipix(ra, dec));

-- 2. Create tides_host to map transients to the catalog table and track 4MOST queue status
CREATE TABLE IF NOT EXISTS tides_host (
    association_id BIGSERIAL PRIMARY KEY,
    tides_id BIGINT REFERENCES tides_master(tides_id) ON DELETE CASCADE,
    host_id BIGINT REFERENCES tides_host_catalog(host_id) ON DELETE CASCADE,
    rank INT CHECK (rank >= 1 AND rank <= 3), -- Limit to top 3
    selection_fn INT REFERENCES pipelines(pipeline_id),
    metadata JSONB, -- stores photoZ, separation, classification reliability, etc.
    pk_4most BIGINT DEFAULT NULL, -- 4MOST ID of the host target
    sync_pending BOOLEAN DEFAULT FALSE, -- Flag to sync host to 4MOST
    active BOOLEAN DEFAULT FALSE, -- Active state in follow-up queue
    UNIQUE (tides_id, rank, selection_fn)
);
CREATE INDEX IF NOT EXISTS idx_tides_host_tides_id ON tides_host(tides_id);
CREATE INDEX IF NOT EXISTS idx_tides_host_host_id ON tides_host(host_id);
