# Реализация пайплайна обработки данных из источников и хранилище для финтех-стартапа

## Описание задачи
Команда аналитиков попросила вас собрать данные по транзакционной активности пользователей и настроить обновление таблицы с курсом валют. 

Цель — понять, как выглядит динамика оборота всей компании и что приводит к его изменениям. 

## Задачи проекта
1. Реализовать хранилище на Vertica.
2. Пайплайн обработки данных из PostgreSQL в Vertica.
3. Формирование витрины с помощью Airflow и DAG.
4. Необходимо реализовать BI-аналитику из Vertica в Metabase.

## Используемые инструменты

- PostgreSQL
- Python
- SQL
- Apache Airflow
- Vetrica
- Metabase

![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-salad)
![Python](https://img.shields.io/badge/-Python-blue)
![SQL](https://img.shields.io/badge/-SQL-pink)
![Airflow](https://img.shields.io/badge/-Airflow-orange)
![Vertica](https://img.shields.io/badge/-Vertica-grey)
![Metabase](https://img.shields.io/badge/-Metabase-yellow)

## Структура репозитория

- /src/dags - DAG-и, которые поставляют данные из источника в хранилище и обновляют витрину данных.
- /src/py - скрипты с подключениями к БД и SQL-скрипты обновления таблиц.
- /src/sql - SQL-запрос формирования таблиц в STAGING и DWH-слоях
- /src/img - скриншоты реализованного над витриной дашборда.

## Архитектура решения

![Архитектура решения](img/Image.png)
