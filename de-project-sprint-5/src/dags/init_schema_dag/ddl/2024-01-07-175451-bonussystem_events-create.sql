CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id INTEGER NOT NULL PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    event_type VARCHAR NOT NULL,
    event_value TEXT NOT NULL
);
