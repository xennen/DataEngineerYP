import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.order_system_dag.pg_saver import PgSaver
from stg.order_system_dag.loader import Loader
from stg.order_system_dag.reader import Reader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_order_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        pg_saver = PgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = Reader(mongo_connect, "restaurants")

        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    restaurant_loader = load_restaurants()

    @task()
    def load_users():
        pg_saver = PgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = Reader(mongo_connect, "users")

        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    user_loader = load_users()

    @task()
    def load_orders():
        pg_saver = PgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = Reader(mongo_connect, "orders")

        loader = Loader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    order_loader = load_orders()

    restaurant_loader >> user_loader >> order_loader


stg_system = stg_order_system_dag()
