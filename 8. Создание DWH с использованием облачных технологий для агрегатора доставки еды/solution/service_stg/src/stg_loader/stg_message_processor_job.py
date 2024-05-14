import json
from datetime import datetime
from logging import Logger
from typing import Dict, List

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from stg_loader.repository import (MenuProduct, Order, OutputMessage, Product,
                                   Restaurant, User)


class StgMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, producer: KafkaProducer, redis, stg_repository, batch_size, logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    def load_to_stg(self, message: Dict) -> None:
        self._stg_repository.order_events_insert(
            message['object_id'], message['object_type'], message['sent_dttm'], json.dumps(message['payload']))

    def get_user(self, id: str) -> User:
        user = self._redis.get(id)
        user['id'] = user['_id']
        return User.model_validate(user)
    
    def get_restaurant(self, id: str) -> Restaurant:
        restaurant = self._redis.get(id)
        restaurant['id'] = restaurant['_id']
        for product in restaurant['menu']:
            product['id'] = product['_id']
        return Restaurant.model_validate(restaurant)
        
    def get_products(self, order_items: List, res_menu: List[MenuProduct]) -> List[Product]:
        products = []
        for item in order_items:
            product = Product.model_validate(item)
            for p in res_menu:
                if product.id == p.id:
                    product.category = p.category
                    break
            products.append(product)
        return products
    
    def get_order(self, message: Dict, restaurant: Restaurant, user: User) -> Order:
        payload = message['payload']
        products = self.get_products(payload['order_items'], restaurant.menu)
        return Order(
            id = message['object_id'],
            date = payload['date'],
            cost = payload['cost'],
            payment = payload['payment'],
            status = payload['final_status'],
            restaurant = restaurant,
            user = user,
            products = products
        )
        
    def get_output_message(self, message: Dict, order: Order) -> OutputMessage:
        return OutputMessage(
            object_id = message['object_id'],
            object_type = message['object_type'],
            payload = order
        )
    
    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: SERVICE_STG START")

        for _ in range(self._batch_size):
            # Получаем сообщение из Kafka
            message = self._consumer.consume()
            if message is None:
                break
            if message["object_type"] != "order":
                continue
            
            # Передаем сообщение в хранилище
            self.load_to_stg(message)
            self._logger.info(f"{datetime.utcnow()}: Message received")
            # Получаем информацию о пользователе и ресторане
            user = self.get_user(message['payload']['user']['id'])
            restaurant = self.get_restaurant(message['payload']['restaurant']['id'])
            self._logger.info(f"{datetime.utcnow()}: User and restaurant received")
            
            # Форматируем заказ и список продуктов для отправки в Kafka
            order = self.get_order(message, restaurant, user)
            
            # Формируем сообщение для отправки в Kafka
            output_message = self.get_output_message(message, order)
            
            # Отправляем сообщение в Kafka
            self._producer.produce(output_message.model_dump_json())
            self._logger.info(f"{datetime.utcnow()}: Message sent to Kafka")
            
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: SERVICE_STG FINISH")
