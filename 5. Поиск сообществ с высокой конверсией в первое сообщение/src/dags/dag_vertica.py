from datetime import datetime

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.task_group import TaskGroup

AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
ENDPOINT_URL = Variable.get('ENDPOINT_URL')
S3_BUCKET = Variable.get('S3_BUCKET')

VERTICA_CONNECTION = "VERTICA_CONN"


def fetch_s3_file(bucket: str, key: str):

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=f'{bucket}',
        Key=f'{key}',
        Filename=f'/data/{key}'
    )

args = {
    "owner": 'me',
    'schedule_interval': None
}

with DAG(
        dag_id= 'dag_vault_vertica',
        default_args=args,
        start_date=datetime.now()
) as dag:

        start = EmptyOperator(task_id='start')
        end = EmptyOperator(task_id='end')

        with TaskGroup(group_id='get_csv') as tg_csv:
            groups_csv = PythonOperator(
            task_id='groups.csv_load',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': S3_BUCKET, 'key': 'groups.csv'},
            )
            dialogs_csv = PythonOperator(
            task_id='dialogs.csv_load',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': S3_BUCKET, 'key': 'dialogs.csv'},
            )
            users_csv = PythonOperator(
            task_id='users.csv_load',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': S3_BUCKET, 'key': 'users.csv'},
            )
            group_log = PythonOperator(
                task_id='group_log.csv_load',
                python_callable=fetch_s3_file,
                op_kwargs={'bucket': S3_BUCKET, 'key': 'group_log.csv'},
            )

        with TaskGroup(group_id='stg') as tg_stg:
            dialogs_stg = VerticaOperator(
                task_id='dialogs_stg',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/dialogs_stg.sql'
            )
            groups_stg = VerticaOperator(
                task_id='groups_stg',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/groups_stg.sql'
            )
            users_stg = VerticaOperator(
                task_id='users_stg',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/users_stg.sql'
            )
            group_log_stg = VerticaOperator(
                task_id='group_log_stg',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/group_log_stg.sql'
            )

        with TaskGroup(group_id='hubs') as tg_hubs:
            h_users = VerticaOperator(
                task_id='h_users_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/h_users_dwh.sql'
            )
            h_groups = VerticaOperator(
                task_id='h_groups_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/h_groups_dwh.sql'
            )
            h_dialogs = VerticaOperator(
                task_id='h_dialogs_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/h_dialogs_dwh.sql'
            )

        with TaskGroup(group_id='links') as tg_links:
            l_admins = VerticaOperator(
                task_id='l_admins_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/l_admins_dwh.sql'
            )
            l_groups_dialogs = VerticaOperator(
                task_id='l_groups_dialogs_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/l_groups_dialogs_dwh.sql'
            )
            l_user_message = VerticaOperator(
                task_id='l_user_message_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/l_user_message_dwh.sql'
            )
            l_user_group_activity_dwh = VerticaOperator(
                task_id='l_user_group_activity_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/l_user_group_activity_dwh.sql'
            )

        with TaskGroup(group_id='sats') as tg_sats:
            s_admins = VerticaOperator(
                task_id='s_admins_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_admins_dwh.sql'
            )
            s_dialog_info = VerticaOperator(
                task_id='s_dialog_info_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_dialog_info_dwh.sql'
            )
            s_group_name = VerticaOperator(
                task_id='s_group_name_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_group_name_dwh.sql'
            )
            s_group_private_status = VerticaOperator(
                task_id='s_group_private_status_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_group_private_status_dwh.sql'
            )
            s_user_chatinfo = VerticaOperator(
                task_id='s_user_chatinfo_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_user_chatinfo_dwh.sql'
            )
            s_user_socdem = VerticaOperator(
                task_id='s_user_socdem_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_user_socdem_dwh.sql'
            )
            s_auth_history = VerticaOperator(
                task_id='s_auth_history_dwh',
                vertica_conn_id=VERTICA_CONNECTION,
                sql='sql/s_auth_history_dwh.sql'
            )

(
start >> tg_csv >> tg_stg >> tg_hubs >> tg_links >> tg_sats >> end
)