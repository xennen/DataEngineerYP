import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.delivery_system_dag.pg_saver import PgSaver
from stg.delivery_system_dag.loader import Loader
from stg.delivery_system_dag.reader import Reader
from lib import ApiConnect, ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_delivery_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    @task()
    def load_couriers():
        pg_saver = PgSaver()

        api_connect = ApiConnect()

        collection_reader = Reader(api_connect, 'couriers')

        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    couriers_loader = load_couriers()
    
    @task()
    def load_deliveries():
        pg_saver = PgSaver()

        api_connect = ApiConnect(sort_field='date')

        collection_reader = Reader(api_connect, 'deliveries', 'order_id')

        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy_delivery()

    deliveries_loader = load_deliveries()

    deliveries_loader
    couriers_loader


stg_system_delivery = stg_delivery_system_dag()
