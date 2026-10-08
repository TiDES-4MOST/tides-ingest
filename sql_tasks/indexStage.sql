-- Run after the rows are loaded: a q3c index for the stage-vs-stage neighbour check,
-- and fresh statistics (autovacuum never analyzes temporary tables).
CREATE INDEX ON tides_stage (q3c_ang2ipix(ra, dec));
ANALYZE tides_stage;
