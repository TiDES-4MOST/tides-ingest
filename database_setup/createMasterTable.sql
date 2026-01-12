CREATE TABLE IF NOT EXISTS tides_master(
    tides_id BIGSERIAL PRIMARY KEY,
    pk_4most BIGINT default null,
    name VARCHAR NOT NULL,
    ra double precision,
    dec double precision,
    jdmin double precision,
    jdmax double precision,
    jd_obs_trigger double precision,
    latest_mag real,
    active BOOL DEFAULT FALSE,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tides_master_id ON tides_master(tides_id);
CREATE INDEX IF NOT EXISTS idx_tides_master_pk_4most ON tides_master(pk_4most);
CREATE INDEX IF NOT EXISTS idx_tides_master_ra_dec ON tides_master (q3c_ang2ipix(ra, dec));