import json
import uuid
from datetime import datetime
from logging import Logger
from typing import List

from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.repository.models import *
from cdm_loader.repository.cdm_builder import CdmBuilder
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, cdm_repository: CdmRepository, batch_size: int, logger: Logger) -> None:

        self._consumer = consumer
        self._dds_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        
        for _ in range(self._batch_size):
            # Получаем сообщение из Kafka
            message = self._consumer.consume()
            # Валидируем сообщение из Kafka.
            validate_message = InputMessage.model_validate_json(json.dumps(message))
            cdm_builder = CdmBuilder(validate_message)
            
            # Заполняем витрины.
            self._dds_repository.user_product_counters_insert(cdm_builder.user_product_counters())
            self._dds_repository.user_category_counters_insert(cdm_builder.user_category_counters())


        self._logger.info(f"{datetime.utcnow()}: FINISH")
