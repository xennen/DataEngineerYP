import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from dds.load_to_dds.loader import Loader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/60 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds', 'warehouse'],
    is_paused_upon_creation=True
)
def dds_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_users()

    users = load_users()

    @task(task_id="restaurants_load")
    def load_restaurants():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_restaurants()

    restaurants = load_restaurants()

    @task(task_id="timestamp_load")
    def load_timestamps():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_timestamps()

    timestamp = load_timestamps()

    @task(task_id="products_load")
    def load_products():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_products()

    product = load_products()

    @task(task_id="orders_load")
    def load_orders():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_orders()

    order = load_orders()
    
    @task(task_id="sales_load")
    def load_sales():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_sales()

    sale = load_sales()
    
    @task(task_id="couriers_load")
    def load_couriers():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_couriers()

    courier = load_couriers()
    
    @task(task_id="deliveries_load")
    def load_deliveries():
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_deliveries()

    delivery = load_deliveries()
    
    users >> restaurants >> timestamp >> product >> order >> sale >> courier >> delivery
    


dds_load = dds_load_dag()
