import os
import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

INPUT_PATH = Variable.get('INPUT_PATH')
OUTPUT_PATH = Variable.get('OUTPUT_PATH')



default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'me'
}

dag = DAG(
    dag_id='dag_events',
    schedule_interval=None,
    default_args=default_args
)

dm_events = SparkSubmitOperator(
    task_id='events',
    dag=dag,
    application='/scripts/events.py',
    conn_id='yarn_spark',
    application_args=[INPUT_PATH, OUTPUT_PATH],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)


dm_events