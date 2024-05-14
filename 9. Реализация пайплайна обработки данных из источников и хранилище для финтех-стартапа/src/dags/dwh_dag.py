import logging
import pendulum


from scripts.vertica_connection import VerticaConnector
from airflow.decorators import dag, task

vertica_conn = VerticaConnector()

log = logging.getLogger(__name__)

@dag(
    dag_id='datamart_update_tst_1',
    start_date=pendulum.datetime(2022, 10, 1, tz='UTC'),
    schedule_interval='0 1 * * *',
    catchup=True,
    tags=['upload', 'datamart', 'update']
)
def datamart_update_dag():
    @task(task_id='update_data')
    def update_datamart(yesterday_ds=None, ds=None):
        date_update = pendulum.from_format(ds, 'YYYY-MM-DD').to_date_string()
        load_date = pendulum.from_format(yesterday_ds, 'YYYY-MM-DD').to_date_string()
        vertica_conn.execute_query("update_datamart.sql", (date_update, load_date, ))

    update_datamart()


_ = datamart_update_dag()