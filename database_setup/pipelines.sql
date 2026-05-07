CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    UNIQUE (pipeline_name, version)
);
