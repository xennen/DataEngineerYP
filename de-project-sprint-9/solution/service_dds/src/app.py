import logging

from app_config import AppConfig
from apscheduler.schedulers.background import BackgroundScheduler
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository import DdsRepository
from flask import Flask

app = Flask(__name__)


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()
    repo = DdsRepository(config.pg_warehouse_db(), app.logger)

    # Инициализируем процессор сообщений.
    proc = DdsMessageProcessor(config.kafka_consumer(), config.kafka_producer(), repo, 30, app.logger)

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
