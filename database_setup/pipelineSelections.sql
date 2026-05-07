CREATE TABLE IF NOT EXISTS pipeline_selections (
    tides_id BIGINT,
    source_survey_id INT,
    pipeline_id INT REFERENCES pipelines(pipeline_id),
    selection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (tides_id, source_survey_id, pipeline_id),
    -- This enforces that the transient must already exist in the surveys table
    FOREIGN KEY (tides_id, source_survey_id) REFERENCES surveys(tides_id, source_survey_id) ON DELETE CASCADE
);
