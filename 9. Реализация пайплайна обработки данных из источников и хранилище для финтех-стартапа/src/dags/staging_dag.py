import logging
import pendulum

from scripts.vertica_connection import VerticaConnector
from scripts.pg_connection import PostgresConnector
from airflow.decorators import dag, task

vertica_conn = VerticaConnector()
postgres_conn = PostgresConnector()

log = logging.getLogger(__name__)


@dag(
    dag_id='upload_to_stg_dag_tst_13',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    schedule_interval='0 0 * * *',
    catchup=True,
    tags=['upload', 'stg']
)
def upload_to_stg_dag():
    @task(task_id='upload_currencies')
    def update_currencies(yesterday_ds = None):
        load_date = pendulum.from_format(yesterday_ds, 'YYYY-MM-DD').to_date_string()
        log.info(f'Currencies load date: {load_date}')
        currencies_log_df = postgres_conn.pg_query("query_currencies_to_log.sql", {'load_date': load_date})
        log.info(f'Currencies to load: {currencies_log_df["count"][0]}')
        currencies_df = postgres_conn.pg_query("get_currencies.sql", {'load_date': load_date})
        vertica_conn.execute_query("delete_currencies.sql", (load_date, ))
        vertica_conn.copy_csv("update_currencies.sql", currencies_df)

    @task(task_id='upload_transactions')
    def update_transactions(yesterday_ds = None):
        load_date = pendulum.from_format(yesterday_ds, 'YYYY-MM-DD').to_date_string()
        log.info(f'Transactions load date: {load_date}')
        transactions_log_df = postgres_conn.pg_query("query_transactions_to_log.sql", {'load_date': load_date})
        log.info(f'Transactions to load: {transactions_log_df["count"][0]}')
        transactions_df = postgres_conn.pg_query("get_transactions.sql", {'load_date': load_date})
        vertica_conn.execute_query("delete_transactions.sql", (load_date, ))
        vertica_conn.copy_csv("update_transactions.sql", transactions_df)

    update_currencies() >> update_transactions()
    


_ = upload_to_stg_dag()
