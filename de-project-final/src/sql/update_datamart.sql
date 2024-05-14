INSERT INTO STV2024012248__DWH.global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
WITH cte AS (
SELECT (%s)::timestamp AS date_update,
        t.operation_id,
        t.account_number_from,
        t.currency_code,
        c.currency_with_div,
        ROUND(t.amount * c.currency_with_div) / 100 AS amount_us
  FROM STV2024012248__STAGING.transactions t
  LEFT JOIN STV2024012248__STAGING.currencies c ON t.transaction_dt::date = c.date_update::date
   AND t.currency_code = c.currency_code
 WHERE t.transaction_dt::date = (%s)
   AND c.currency_code_with = 420
   AND t.account_number_from > 0
   AND t.account_number_to > 0
   AND t.status = 'done'
)

SELECT date_update,
       currency_code AS currency_from,
       SUM(amount_us) AS amount_total,
       COUNT(operation_id) AS cnt_transactions,
       COUNT(operation_id) / COUNT(DISTINCT account_number_from) AS avg_transactions_per_account,
       COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
  FROM cte
 GROUP BY 1, 2