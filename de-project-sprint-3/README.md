# Проектная работа по ETL и автоматизации подготовки данных

### Задачи проекта
1. Адаптировать пайплайн для текущей задачи: учесть в витрине нужные статусы и обновить пайплайн с учётом этих статусов.
2. На основе пайплайна наполнить витрину данными по «возвращаемости клиентов» в разрезе недель.
3. Перезапустить пайплайн и убедиться, что после перезапуска не появилось дубликатов в витринах.

### Используемые инструменты

- Python
- Apache Airflow
- S3
- PostgreSQL
- REST API

![Python](https://img.shields.io/badge/-Python-blue)
![Airflow](https://img.shields.io/badge/-Airflow-orange)
![S3](https://img.shields.io/badge/-S3-orange)
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-salad)
![REST-API](https://img.shields.io/badge/-REST_API-white)


### Структура репозитория

- /src/dags - DAG, с помощью которого реализован ETL.
- /src/dags/sql - SQL-скрипты.


