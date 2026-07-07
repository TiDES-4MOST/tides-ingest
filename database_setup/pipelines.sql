CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    UNIQUE (pipeline_name, version)
);
INSERT INTO pipelines (pipeline_name, version)
VALUES ('TiDES-LSST-default', 'v1.0'),
    ('TiDES-ZTF-default', 'v1.0'),
    ('TiDES-LS4-default', 'v1.0'),
    ('TiDES-4MOST-default', 'v1.0'),
    ('TiDES-ATLAS-default', 'v1.0'),
    ('TiDES-GOTO-default', 'v1.0'),
    ('TiDES-HSC-default', 'v1.0'),
    ('TiDES-BlackGEM-default', 'v1.0'),
    ('TiDES-TNS-default', 'v1.0'),
    ('sherlock', 'v1.0') ON CONFLICT (pipeline_name, version) DO NOTHING;