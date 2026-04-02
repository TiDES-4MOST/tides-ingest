CREATE TABLE IF NOT EXISTS surveys (
    tides_id BIGINT PRIMARY KEY REFERENCES tides_master(tides_id),
    transient_name VARCHAR NOT NULL,
    source_survey_id INT REFERENCES survey_ids(survey_id)
);