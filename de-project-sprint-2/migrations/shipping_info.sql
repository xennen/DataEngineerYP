CREATE TABLE shipping_info(
    shipping_id int,
    vendor_id int,
    payment_amount numeric(14,2),
    shipping_plan_datetime timestamp,
    shipping_country_rate_id int,
    shipping_agreement_id int,
    shipping_transfer_id int,
    PRIMARY KEY (shipping_id),
    FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer(id),
    FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement(agreement_id),
    FOREIGN KEY (shipping_country_rate_id) REFERENCES shipping_country_rates(id)
);

INSERT INTO shipping_info (shipping_id, vendor_id, payment_amount, shipping_plan_datetime, shipping_country_rate_id, shipping_agreement_id, shipping_transfer_id)
SELECT 
    shippingid, 
    vendorid, 
    payment_amount, 
    shipping_plan_datetime, 
    scr.id AS country_rate_id, 
    sa.agreement_id, 
    st.id AS transfer_id
FROM
    (SELECT DISTINCT ON (shippingid) 
        shippingid, 
        vendorid, 
        shipping_plan_datetime, 
        payment_amount,
        regexp_split_to_array(shipping_transfer_description, ':+') AS transfer_description, 
        shipping_transfer_rate, 
        shipping_country,
        regexp_split_to_array(vendor_agreement_description, ':+') AS attributes_agreement
    FROM 
        shipping s) AS sh
JOIN 
    shipping_transfer st ON sh.shipping_transfer_rate = st.shipping_transfer_rate AND
                            transfer_description[1] = transfer_type AND
                            transfer_description[2] = transfer_model
JOIN 
    shipping_country_rates scr ON sh.shipping_country = scr.shipping_country
JOIN 
    shipping_agreement sa ON attributes_agreement[1]::int = sa.agreement_id 
ORDER BY 
    1;
