-- 004: protect the provenance record, and index the 4MOST log by time.
-- Every statement is idempotent: running this file twice is harmless.

-- tides_events is the provenance record and cannot be rebuilt. It used to be ON DELETE CASCADE,
-- so one DELETE on tides_master silently erased an object's whole history. Now such a DELETE fails
-- while the object has events. (TRUNCATE ... CASCADE, used only by the tests, is unaffected.)
ALTER TABLE tides_events DROP CONSTRAINT IF EXISTS tides_events_tides_id_fkey;
ALTER TABLE tides_events ADD CONSTRAINT tides_events_tides_id_fkey
    FOREIGN KEY (tides_id) REFERENCES tides_master(tides_id) ON DELETE RESTRICT;

-- Health checks and the milestone back-off query tides_4most_log by request_time.
CREATE INDEX IF NOT EXISTS idx_tides_4most_log_time ON tides_4most_log (request_time);
