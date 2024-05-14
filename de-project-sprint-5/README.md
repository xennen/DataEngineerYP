# DWH для нескольких источников и витриной. Витрина данных помогает при расчете платежей курьерам

## Задача проекта

Реализовать витрину для расчётов с курьерами. 
В ней необходимо рассчитать суммы оплаты каждому курьеру за предыдущий месяц.

## Описание задачи

Построить витрину, содержащую информацию о выплатах курьерам.

Состав витрины:

- **id** — идентификатор записи.
- **courier_id** — ID курьера, которому перечисляем.
- **courier_name** — Ф. И. О. курьера.
- **settlement_year** — год отчёта.
- **settlement_month** — месяц отчёта, где 1 — январь и 12 — декабрь.
- **orders_count** — количество заказов за период (месяц).
- **orders_total_sum** — общая стоимость заказов.
- **rate_avg** — средний рейтинг курьера по оценкам пользователей.
- **order_processing_fee** — сумма, удержанная компанией за обработку заказов, которая высчитывается как
  orders_total_sum * 0.25.
- **courier_order_sum** — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый
  доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- **courier_tips_sum** — сумма, которую пользователи оставили курьеру в качестве чаевых.
- **courier_reward_sum** — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum +
  courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном
месяце:

- r < 4 — 5% от заказа, но не менее 100 р.;
- 4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
- 4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
- 4.9 <= r — 10% от заказа, но не менее 200 р.

Отчёт собирается по дате заказа.
Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки. 
Иногда заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. 
Это важно, потому что такие случаи могут выпадать в том числе и на последний день месяца. 
Тогда начисление курьеру относите к дате заказа, а не доставки.


## Выполненная работа
1. Доработаны модели слоев DWH с учетом требований и нового источника.
2. Реализован дополнительный репозиторий для загрузки данных из API системы доставки.
3. Доработаны скрипты DAG Airflow для получения и обработки данных из системы доставки.
4. Реализован SQL скрипт сбора данных из DDS слоя в таблицу витрины слоя.


## Использованные технологии:

- Python
- PostgreSQL
- MongoDB
- Apache Airflow
- ETL