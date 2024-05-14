CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    period_name VARCHAR(10) NOT NULL,
    period_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL REFERENCES mart.d_item(item_id),
    new_customers_count INTEGER,
    returning_customers_count INTEGER,
    refunded_customer_count INTEGER,
    new_customers_revenue NUMERIC(10, 2),
    returning_customers_revenue NUMERIC(10, 2),
    customers_refunded INTEGER
);
