import json

from datetime import datetime
from logging import Logger
from typing import List

from dds_loader.repository.dds_repository import DdsRepository
from dds_loader.repository.models import *
from dds_loader.repository.dds_builder import OrderDdsBuilder
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer


class DdsMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, producer: KafkaProducer, dds_repository: DdsRepository, batch_size: int, logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: SERVICE_DDS START")
        for _ in range(self._batch_size):
            # Получаем сообщение из Kafka
            message = self._consumer.consume()
            if message is None:
                break
            # Валидируем сообщение из Kafka.
            validate_message = InputMessage.model_validate_json(json.dumps(message))
            
            # Выделяем нужные данные из сообщения.
            order: Order = validate_message.payload
            dds_builder = OrderDdsBuilder(order)
            
            # Заполняем хабы.
            self._dds_repository.insert_h_order(dds_builder.h_order())
            self._dds_repository.insert_h_user(dds_builder.h_user())
            self._dds_repository.insert_h_restaurant(dds_builder.h_restaurant())
            self._dds_repository.insert_h_category(dds_builder.h_category())
            self._dds_repository.insert_h_product(dds_builder.h_product())     
              
            # Заполняем линки.
            self._dds_repository.insert_l_order_user(dds_builder.l_order_user())
            self._dds_repository.insert_l_order_product(dds_builder.l_order_product())
            self._dds_repository.insert_l_product_category(dds_builder.l_product_category())
            self._dds_repository.insert_l_product_restaurant(dds_builder.l_product_restaurant())
            
            # Заполняем сателлиты.
            self._dds_repository.insert_s_user_names(dds_builder.s_user_names())
            self._dds_repository.insert_s_restaurant_names(dds_builder.s_restaurant_names())
            self._dds_repository.insert_s_order_status(dds_builder.s_order_status())
            self._dds_repository.insert_s_order_cost(dds_builder.s_order_cost())
            self._dds_repository.insert_s_product_names(dds_builder.s_product_names())
                
            # Отправляем сообщение в Kafka.
            output_message = dds_builder.output_message()
            self._producer.produce(output_message.model_dump_json())
            
        
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: SERVICE_DDS FINISH")
