from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import time
import json
import requests
import pandas as pd
import logging

task_logger = logging.getLogger("airflow.task")

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host
postgres_conn_id = 'postgresql_de'

nickname = Variable.get('nickname')
cohort = Variable.get('cohort')
email_student = Variable.get('email_student')

truncate_flag = False

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    task_logger.info("Making request generate_report")

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    task_logger.info("Making request get_report")
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError('report id is None')

    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f'Report_id={report_id}')


def get_increment(date, ti):
    task_logger.info("Making request get_increment")
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename, index_col=0).drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')

args = {
    "owner": nickname,
    'email': [email_student],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

business_dt = '{{ ds }}'


with DAG(
        'sales_mart_v',
        default_args=args,
        description='project dag sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    
    
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})
    
    tasks_mart_update = dict()    
    for i in ['d_city', 'd_item', 'd_customer', 'f_sales', 'f_c_r_create', 'f_c_r_insert', 'delete_f_sales', 'delete_uol', 'truncate_fcr']:
        tasks_mart_update[i] = PostgresOperator(
            task_id = f'load_{i}',
            postgres_conn_id = postgres_conn_id,
            sql = f'sql/mart.{i}.sql',
            dag = dag
        )
   
   
    (
       
    generate_report >> get_report >> get_increment >> tasks_mart_update["delete_uol"] >> upload_user_order_inc >> [tasks_mart_update["d_city"], tasks_mart_update["d_item"], tasks_mart_update["d_customer"]]
    >> tasks_mart_update["delete_f_sales"] >> tasks_mart_update["f_sales"]
    >> tasks_mart_update["f_c_r_create"] >> tasks_mart_update["truncate_fcr"] >> tasks_mart_update["f_c_r_insert"]
    
    )