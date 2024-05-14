CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id SERIAL PRIMARY KEY,
    workflow_key VARCHAR NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);


