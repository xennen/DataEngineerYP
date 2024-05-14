import pandas as pd
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresConnector:
    def __init__(self):
        self.postgres_hook = PostgresHook('postgres_default')
        self.engine = self.postgres_hook.get_sqlalchemy_engine()
        self.log = logging.getLogger(__name__)

    def pg_query(self, file, params=None) -> pd.DataFrame:
        try:
            with self.engine.connect() as pg_conn:
                with open(f"sql/{file}") as sql_file:
                    query = sql_file.read()
                    self.log.info(f'Executing query: {query}')
                    query_result = pd.read_sql_query(sql=query, con=pg_conn, params=params)
                
                return query_result
        except Exception as e:
            self.log.error('ERROR: %s', str(e))
            raise