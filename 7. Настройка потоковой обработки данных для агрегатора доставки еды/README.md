# **Настройка потоковой обработки данных для агрегатора доставки еды**

![Kafka](https://img.shields.io/badge/-Kafka-orange)
![Spark Streaming](https://img.shields.io/badge/-Spark_Streaming-orange)
![PySpark](https://img.shields.io/badge/-PySpark-green)
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-salad)
![Python](https://img.shields.io/badge/-Python-blue)

## **Задачи проекта**
Написать сервис, который будет:

1. Читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени.
2. Получать список подписчиков из базы данных Postgres.
3. Джойнить данные из Kafka с данными из БД.
4. Сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka.
5. Отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане.
6. Вставлять записи в Postgres, чтобы получить фидбэк от пользователя.

## **Использованные технологии:**

- **`Kafka`**
- **`Spark Streaming`**
- **`PySpark`**
- **`PostgreSQL`**
- **`Python`**

## **Структура репозитория**

- `/src/scripts` - spark скрипт для чтения данных из топика kafka, отправки в PostgreSQL и другой топик.