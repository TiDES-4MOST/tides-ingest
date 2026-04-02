CREATE TABLE IF NOT EXISTS tides_ztf(
    tides_id BIGINT PRIMARY KEY REFERENCES tides_master(tides_id),
    name VARCHAR NOT NULL,
    ra double precision,
    dec double precision,
    jdmin double precision,
    jdmax double precision,
    latest_mag real,
    latest_filter VARCHAR,