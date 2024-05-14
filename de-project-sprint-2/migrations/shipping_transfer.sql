CREATE TABLE shipping_transfer (
    id serial,
    transfer_type text,
    transfer_model text,
    shipping_transfer_rate numeric(14,3),
    PRIMARY KEY(id)
);

INSERT INTO shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
SELECT 
    attributes_transfer[1]::text AS transfer_type,
    attributes_transfer[2]::text AS transfer_model,
    shipping_transfer_rate::numeric(14,3)
FROM 
    (
        SELECT 
            regexp_split_to_array(shipping_transfer_description, ':+') AS attributes_transfer,
            shipping_transfer_rate
        FROM 
            shipping
    ) AS shipping_trans
GROUP BY 
    1, 2, 3;
