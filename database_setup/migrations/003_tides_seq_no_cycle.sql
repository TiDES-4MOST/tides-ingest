-- 003 (P1, one line): at 474,552 names in one year, fail loudly instead of silently wrapping
-- to TiDESyyaaa and then failing every run on the UNIQUE(name) constraint.
ALTER SEQUENCE tides_seq NO CYCLE;
