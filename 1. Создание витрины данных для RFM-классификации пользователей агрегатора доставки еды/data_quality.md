# 1.3. Качество данных

## Оцените, насколько качественные данные хранятся в источнике.

1. Посмотрел визуально на все данные в нужной таблице и убедиться что на виду нету null или дублей
2. Сделать простые запросы на поиск дублей по нужным нам полям 
3. Сделать простые запросы на поиск пустых значений(null или 0 например в поле оплаты заказа)

Вывод: В этой таблице хранятся только качественные данные

## Укажите, какие инструменты обеспечивают качество данных в источнике.


| Таблицы                   | Объект                            | Инструмент      | Для чего используется |
| -------------------       | ---------------------------       | --------------- | --------------------- |
| production.products       | id int not null PRIMARY KEY       | Первичный ключ  | Обеспечивает уникальность записей о продуктах |
| production.products       | price numeric(19,5) not null      |      CHECK      | Проверка положительного числ в поле по такой формуле ((price >= (0)::numeric)) |
| production.users          | id int not null PRIMARY KEY       | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.orderstatuslog | id int not null PRIMARY KEY       | Первичный ключ  | Обеспечивает уникальность записей о логах статусов |
| production.orderstatuslog | order_id int not null             | Вторичный ключ  | Обеспечивает уникальность записей id заказов связан с таблицей orders|
| production.orderstatuslog | status_id int not null            | Вторичный ключ  | Обеспечивает уникальность записей id статуса cвязан с таблицей orderstatuses |
| production.orderstatuses  | id int not null PRIMARY KEY       | Первичный ключ  | Обеспечивает уникальность записей о статусах |
| production.orders         | cost numeric(19,5) not null       | CHECK  | Формула подсчета данного поля ((cost = (payment + bonus_payment))) |
| production.orders         | order_id int not null PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей заказов |
| production.orderitems     | id int not null PRIMARY KEY       | Первичный ключ  | Обеспечивает уникальность записей продуктов в заказе |
| production.orderitems     | order_id int not null             | Вторичный ключ  | Обеспечивает уникальность записей id заказов связан с таблицей orders |
| production.orderitems     | product_id int not null           | Вторичный ключ  | Обеспечивает уникальность записей id продукта связан с таблицей products |
| production.orderitems     | discount numeric(19,5) not null   |      CHECK      | Формула ограничения данного поля (((discount >= (0)::numeric) AND (discount <= price))) |
| production.orderitems     | price numeric(19,5) not null      |      CHECK      | Формула ограничения данного поля ((price >= (0)::numeric)) |
| production.orderitems     | quantity numeric(19,5) not null   |      CHECK      | Формула ограничения данного поля ((quantity > 0)) |