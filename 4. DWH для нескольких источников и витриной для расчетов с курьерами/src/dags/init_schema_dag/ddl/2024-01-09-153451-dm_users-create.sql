CREATE TABLE IF NOT EXISTS dds.dm_users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    user_name VARCHAR NOT NULL,
    user_login VARCHAR NOT NULL,
    CONSTRAINT user_id_unique UNIQUE (user_id)
);
