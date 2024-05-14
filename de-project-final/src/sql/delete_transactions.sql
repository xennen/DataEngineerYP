DELETE FROM STV2024012248__STAGING.transactions
 WHERE transaction_dt::date = (%s)