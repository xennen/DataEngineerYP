CREATE TABLE IF NOT EXISTS dds.dm_products (
    id SERIAL PRIMARY KEY,
    restaurant_id INTEGER NOT NULL REFERENCES dds.dm_restaurants,
    product_id VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL CHECK (product_price >= 0),
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL,
    CONSTRAINT product_id_unique UNIQUE (product_id)
);
