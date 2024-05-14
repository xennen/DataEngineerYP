INSERT INTO mart.f_customer_retention (
    period_name,
    period_id,
    item_id,
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT uniq_id) AS order_count,
        SUM(payment_amount) AS total_revenue
    FROM staging.user_order_log
    GROUP BY customer_id
)

SELECT
    'weekly' AS period_name,
    EXTRACT(week FROM date_time) AS period_id,
    item_id,
    COUNT(CASE WHEN order_count = 1 THEN 1 END) AS new_customers_count,
    COUNT(CASE WHEN order_count > 1 THEN 1 END) AS returning_customers_count,
    COUNT(CASE WHEN status = 'refunded' THEN 1 END) AS refunded_customer_count,
    SUM(CASE WHEN order_count = 1 THEN total_revenue END) AS new_customers_revenue,
    SUM(CASE WHEN order_count > 1 THEN total_revenue END) AS returning_customers_revenue,
    COALESCE(SUM(CASE WHEN status = 'refunded' THEN total_revenue END), 0) AS customers_refunded
FROM staging.user_order_log
JOIN customer_orders ON user_order_log.customer_id = customer_orders.customer_id
GROUP BY 1, 2, 3
ORDER BY 2;
