CREATE OR REPLACE VIEW analysis.orders AS
SELECT 
    o.order_id, 
    o.order_ts,
    o.user_id,
    o.bonus_payment,
    o.payment,
    o.cost,
    o.bonus_grant,
    ol.status_id AS status
FROM 
    production.orders o
JOIN 
    orderstatuslog ol ON o.order_id = ol.order_id AND o.order_ts = ol.dttm;
