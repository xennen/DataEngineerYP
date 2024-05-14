CREATE VIEW shipping_datamart AS
SELECT 
    si.shipping_id, 
    vendor_id, 
    st.transfer_type, 
    date_part('day', shipping_end_fact_datetime - shipping_start_fact_datetime) AS full_day_at_shipping,
    CASE
        WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1
        ELSE 0
    END AS is_delay,
    CASE
        WHEN ss.status = 'finished' THEN 1
        ELSE 0
    END AS is_shipping_finish,
    CASE
        WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN date_part('day', shipping_end_fact_datetime - shipping_plan_datetime)
        ELSE 0
    END AS delay_day_at_shipping, 
    payment_amount,
    payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) AS vat,
    payment_amount * agreement_commission AS profit
FROM 
    shipping_info si
JOIN 
    shipping_status ss ON si.shipping_id = ss.shipping_id
JOIN 
    shipping_transfer st ON si.shipping_transfer_id = st.id
JOIN 
    shipping_agreement sa ON si.shipping_agreement_id = sa.agreement_id 
JOIN 
    shipping_country_rates scr ON si.shipping_country_rate_id = scr.id;

SELECT *
FROM shipping_datamart;
