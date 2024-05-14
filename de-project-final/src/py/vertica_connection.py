from airflow.models import Variable
import vertica_python
import pandas as pd

import logging


class VerticaConnector:
    def __init__(self):
        self.vertica_host = Variable.get('VERTICA_HOST')
        self.vertica_user = Variable.get('VERTICA_USER')
        self.vertica_password = Variable.get('VERTICA_PASSWORD')
        self.db = Variable.get('VERTICA_DB')
        self.vertica_conn_info = {'host': self.vertica_host,
                     'port': 5433,
                     'user': self.vertica_user,
                     'password': self.vertica_password,
                     'database': self.db,
                     'autocommit': True
                     }
        self.log = logging.getLogger(__name__)
    
    def execute_query(self, file, params=None):
        try:
            with vertica_python.connect(**self.vertica_conn_info) as conn:
                with conn.cursor() as cur:
                    with open(f"sql/{file}") as sql_file:
                        query = sql_file.read()
                        cur.execute(query, params)
        except Exception as e:
            self.log.error('ERROR: %s', str(e))
            raise
                
    def copy_csv(self, file, df: pd.DataFrame, sep: str = '|', index: bool = False, header: bool = False) -> None:
        try:
            with vertica_python.connect(**self.vertica_conn_info) as conn:
                with conn.cursor() as cur:
                    with open(f"sql/{file}") as sql_file:
                        query = sql_file.read()
                        cur.copy(query, df.to_csv(sep=sep, index=index, header=header))
        except Exception as e:
            self.log.error('ERROR: %s', str(e))
            raise
