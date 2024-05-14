CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES dds.dm_products,
    order_id INTEGER NOT NULL REFERENCES dds.dm_orders,
    count INTEGER DEFAULT 0 NOT NULL CHECK (count >= 0),
    price NUMERIC(14, 2) DEFAULT 0 NOT NULL CHECK (price >= 0),
    total_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL CHECK (total_sum >= 0),
    bonus_payment NUMERIC(14, 2) DEFAULT 0 NOT NULL CHECK (bonus_payment >= 0),
    bonus_grant NUMERIC(14, 2) DEFAULT 0 NOT NULL CHECK (bonus_grant >= 0)
);