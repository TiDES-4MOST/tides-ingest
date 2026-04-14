CREATE TABLE IF NOT EXISTS surveys (
    tides_id BIGINT REFERENCES tides_master(tides_id),
    transient_name VARCHAR NOT NULL,
    source_survey_id INT REFERENCES survey_ids(survey_id),
    PRIMARY KEY (tides_id, source_survey_id)
);