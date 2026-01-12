CREATE TABLE IF NOT EXISTS survey_ids (
    survey_name VARCHAR NOT NULL,
    survey_id INT PRIMARY KEY NOT NULL
);
INSERT INTO survey_ids (survey_name, survey_id)
VALUES ('LSST', 1),
    ('ZTF', 2),
    ('LS4', 3),
    ('4MOST', 4),
    ('ATLAS', 5),
    ('GOTO', 6),
    ('HSC', 7),
    ('BlackGEM', 8),
    ('TNS', 9) ON CONFLICT (survey_id) DO NOTHING;