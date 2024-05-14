import logging

import pendulum
from airflow.decorators import dag, task
from stg.bonus_system_dag.loader import Loader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_bonus_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = Loader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    ranks = load_ranks()
    
    @task(task_id="users_load")
    def load_users():
        rest_loader = Loader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    users = load_users()

    @task(task_id="events_load")
    def load_events():
        rest_loader = Loader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()

    events = load_events()

    ranks >> users >> events


stg_bonus_system = stg_bonus_system_dag()
