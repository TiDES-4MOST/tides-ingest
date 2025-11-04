CREATE TABLE tides_master(
    tides_id  SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    ra double precision,
    dec double precision,
    jdmin double precision,
    jdmax double precision,
    magrmin real,
    maggmin real,
    rmag real,
    gmag real,
    jdgmax double precision,
    jdrmax double precision,
    ncandgp real,
    sherlock_class VARCHAR,
    active BOOL DEFAULT FALSE,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
