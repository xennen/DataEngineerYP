SELECT *
  FROM public.transactions
 WHERE transaction_dt::date = %(load_date)s;