CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER NOT NULL,
    order_key VARCHAR NOT NULL,
    order_status VARCHAR NOT NULL,
    CONSTRAINT dm_orders_to_users_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users,
    CONSTRAINT dm_orders_to_restaurants_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants,
    CONSTRAINT dm_orders_to_timestamps_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps,
    CONSTRAINT order_key_unique UNIQUE (order_key)
);
