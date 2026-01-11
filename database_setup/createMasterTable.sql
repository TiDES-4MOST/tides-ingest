CREATE TABLE tides_master(
    tides_id BIGSERIAL PRIMARY KEY,
    pk_4most BIGINT default null,
    name VARCHAR NOT NULL,
    ra double precision,
    dec double precision,
    jdmin double precision,
    jdmax double precision,
    latest_mag real,
    active BOOL DEFAULT FALSE,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);