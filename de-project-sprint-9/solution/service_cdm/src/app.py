import logging

from app_config import AppConfig
from apscheduler.schedulers.background import BackgroundScheduler
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import CdmRepository
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
    repo = CdmRepository(config.pg_warehouse_db(), app.logger)
    
    # Инициализируем процессор сообщений.
    proc = CdmMessageProcessor(config.kafka_consumer(), repo, 30, app.logger)

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
