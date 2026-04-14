CREATE TABLE IF NOT EXISTS surveys (
    tides_id BIGINT REFERENCES tides_master(tides_id),
    transient_name VARCHAR NOT NULL, -- Native naming given by the external survey string
    source_survey_id INT REFERENCES survey_ids(survey_id),
    -- Composite primary key ensures a transient can be matched to multiple surveys securely
    PRIMARY KEY (tides_id, source_survey_id)
);