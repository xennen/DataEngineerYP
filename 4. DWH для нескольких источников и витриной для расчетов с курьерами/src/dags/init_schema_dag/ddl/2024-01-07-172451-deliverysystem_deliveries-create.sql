CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);
