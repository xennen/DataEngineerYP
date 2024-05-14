CREATE TABLE IF NOT EXISTS STV2024012248__STAGING.transactions
(
    operation_id uuid NOT NULL,
    account_number_from int NOT NULL,
    account_number_to int NOT NULL,
    currency_code numeric(3) NOT NULL,
    country varchar(100) NOT NULL,
    status varchar(100) NOT NULL,
    transaction_type varchar(100) NOT NULL,
    amount int NOT NULL,
    transaction_dt timestamp(3) NOT NULL
)
ORDER BY transaction_dt
SEGMENTED BY hash(transactions.transaction_dt, transactions.operation_id) ALL NODES KSAFE 1
PARTITION BY transactions.transaction_dt::date;

CREATE TABLE IF NOT EXISTS STV2024012248__STAGING.currencies
(
    date_update timestamp(3) NOT NULL,
    currency_code numeric(3) NOT NULL,
    currency_code_with numeric(3) NOT NULL,
    currency_with_div numeric(5,2) NOT NULL
)
ORDER BY date_update
SEGMENTED BY hash(currencies.date_update, currencies.currency_code) ALL NODES KSAFE 1
PARTITION BY currencies.date_update::date;

CREATE TABLE IF NOT EXISTS STV2024012248__DWH.global_metrics
(
    date_update timestamp(3) NOT NULL,
    currency_from numeric(3) NOT NULL,
    amount_total numeric(15,2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(15,2) NOT NULL,
    cnt_accounts_make_transactions int NOT NULL
)
PARTITION BY global_metrics.date_update::date;