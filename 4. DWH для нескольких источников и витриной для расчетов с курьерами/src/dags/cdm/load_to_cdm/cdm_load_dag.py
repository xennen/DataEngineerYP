import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from cdm.load_to_cdm.loader import Loader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/60 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['cdm', 'warehouse'],
    is_paused_upon_creation=True
)
def cdm_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="settlement_reports_load")
    def load_settlement_reports():
        settlement_reports_loader = Loader(dwh_pg_connect, log)
        settlement_reports_loader.load_settlements_reports()

    reports_s = load_settlement_reports()
    
    @task(task_id="courier_ledger_reports_load")
    def load_courier_ledger_reports():
        courier_ledger_reports_loader = Loader(dwh_pg_connect, log)
        courier_ledger_reports_loader.load_courier_ledger_reports()

    reports_c = load_courier_ledger_reports()



    reports_s >> reports_c
    


dds_load = cdm_load_dag()
