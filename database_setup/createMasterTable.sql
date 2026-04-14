-- Create the tides_master table
CREATE TABLE IF NOT EXISTS tides_master(
    tides_id BIGSERIAL PRIMARY KEY,
    pk_4most BIGINT default null,
    ostd_targ_id BIGINT default null,
    ostd_u_obj_id BIGINT default null,
    name TEXT UNIQUE,
    ra double precision,
    dec double precision,
    jdmin double precision,
    jdmax double precision,
    jd_obs_trigger double precision,
    -- Replaced glatest/rlatest with a dynamic JSONB dictionary.
    -- Stores the most recent magnitude for any given filter map, e.g. {"g": 21.3, "i": 22.0}
    latest_mags JSONB DEFAULT '{}'::jsonb,
    active BOOL DEFAULT FALSE,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
-- Create indexes
CREATE INDEX IF NOT EXISTS idx_tides_master_id ON tides_master(tides_id);
CREATE INDEX IF NOT EXISTS idx_tides_master_pk_4most ON tides_master(pk_4most);
CREATE INDEX IF NOT EXISTS idx_tides_master_ra_dec ON tides_master (q3c_ang2ipix(ra, dec));
--
-- Create a sequence for name, i.e. TiDES26aaa
CREATE SEQUENCE tides_seq MAXVALUE 474551 CYCLE;
--
-- Creating the aaa, aab, ... zzz, aaaa, aaab ... zzzz, etc.
CREATE OR REPLACE FUNCTION to_dynamic_alpha(val integer) RETURNS text AS $$
DECLARE chars text := 'abcdefghijklmnopqrstuvwxyz';
BEGIN -- Range 0 to 17,575 -> 3 letters (aaa to zzz)
IF val < 17576 THEN RETURN substr(chars, (val / 676) + 1, 1) || substr(chars, ((val % 676) / 26) + 1, 1) || substr(chars, (val % 26) + 1, 1);
-- Range 17,576 to 474,551 -> 4 letters (aaaa to zzzz)
ELSE val := val - 17576;
RETURN substr(chars, (val / 17576) + 1, 1) || substr(chars, ((val % 17576) / 676) + 1, 1) || substr(chars, ((val % 676) / 26) + 1, 1) || substr(chars, (val % 26) + 1, 1);
END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
--
-- The trigger function to create the TiDES26aaa name
CREATE OR REPLACE FUNCTION trg_fn_generate_tides_name() RETURNS TRIGGER AS $$
DECLARE current_yy text := to_char(current_date, 'YY');
last_yy text;
BEGIN -- Updated: substring starts at 9 because 'TiDES-SN' is 8 chars long
SELECT substring(
        name
        from 9 for 2
    ) INTO last_yy
FROM tides_master
ORDER BY tides_id DESC
LIMIT 1;
IF last_yy IS NOT NULL
AND last_yy != current_yy THEN PERFORM setval('tides_seq', 0, false);
END IF;
-- Construct the name
NEW.name := 'TiDES-SN' || current_yy || to_dynamic_alpha(nextval('tides_seq')::int);
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER tides_name_auto_generator BEFORE
INSERT ON tides_master FOR EACH ROW EXECUTE FUNCTION trg_fn_generate_tides_name();