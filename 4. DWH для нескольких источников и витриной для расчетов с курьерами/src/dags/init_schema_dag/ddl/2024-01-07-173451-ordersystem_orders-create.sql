CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,
    CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id)
);



