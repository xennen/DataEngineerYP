CREATE TABLE shipping_agreement (
    agreement_id int4,
    agreement_number text,
    agreement_rate numeric(14,2),
    agreement_commission numeric(14,2),
    PRIMARY KEY (agreement_id)
);

INSERT INTO shipping_agreement (agreement_id, agreement_number, agreement_rate, agreement_commission)
SELECT 
    attributes_agreement[1]::int AS agreement_id,
    attributes_agreement[2]::text AS agreement_number,
    attributes_agreement[3]::numeric(14,2) AS agreement_rate,
    attributes_agreement[4]::numeric(14,2) AS agreement_commission
FROM (
    SELECT regexp_split_to_array(vendor_agreement_description, ':+') AS attributes_agreement
    FROM shipping
) AS shipping_agree
GROUP BY 1,2,3,4;
