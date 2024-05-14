# Необходимые импорты
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id='dag',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    )

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
        

    op_kwargs={
        'url': 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/data.csv',
        'tmp_file': '/tmp/file.csv'}
    )

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        'tmp_file': '/tmp/file.csv',
        'tmp_agg_file': '/tmp/file_agg.csv',
        'group': ['A', 'B', 'C'],
        'agreg': {"D": sum}},
    dag=dag
    )

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={
        'tmp_file': '/tmp/file_agg.csv',
        'table_name': 'table'},
    dag=dag
    )

# Создадим порядок выполнения задач
# В данном случае 2 задачи буудт последователньы и ещё 2 парараллельны
extract_data >> transform_data >> load_data 