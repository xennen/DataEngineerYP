CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name VARCHAR NOT NULL
);
