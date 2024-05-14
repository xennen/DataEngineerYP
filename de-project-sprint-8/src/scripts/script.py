import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

load_dotenv()

TOPIC_NAME_OUT = os.getenv('TOPIC_NAME_OUT')
TOPIC_NAME_IN = os.getenv('TOPIC_NAME_IN')
JDBC_URL_SUBSCRIBERS = os.getenv('JDBC_URL_SUBSCRIBERS')
PASSWORD_SUBSCRIBERS = os.getenv('PASSWORD_SUBSCRIBERS')
USER_SUBSCRIBERS = os.getenv('USER_SUBSCRIBERS')
JDBC_URL_LOCAL = os.getenv('JDBC_URL_LOCAL')
PASSWORD_LOCAL = os.getenv('PASSWORD_LOCAL')
USER_LOCAL = os.getenv('USER_LOCAL')
KAFKA_URL = os.getenv('KAFKA_URL')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_USER = os.getenv('KAFKA_USER')

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{KAFKA_USER}\" password=\"{KAFKA_PASSWORD}\";'
}

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

def spark_init(session_name) -> SparkSession:
    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = SparkSession.builder \
        .appName(session_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark  

def read_restaurants_stream(spark: SparkSession) -> DataFrame:
    # читаем из топика Kafka сообщения с акциями от ресторанов
    try:
        restaurant_read_stream_df = (spark.readStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', KAFKA_URL)
                                    .options(**kafka_security_options)
                                    .option('subscribe', TOPIC_NAME_IN)
                                    .load()
                                    )
    except Exception as e:
        logger.error(f"Error read from Kafka: {str(e)}")
    
    # определяем схему входного сообщения для json
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType()),
    ])
    # десериализуем из value сообщения json
    return (restaurant_read_stream_df
                .withColumn('value', f.col('value').cast(StringType()))
                .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
                .selectExpr('event.*')
    )

def filter_stream_data(restaurant_read_stream_df: DataFrame, trigger_datetime_created) -> DataFrame:
    # добавляем время создания события и фильтруем по времени старта и окончания акции.
    filtered_read_stream_df = (restaurant_read_stream_df
                                        .withColumn('trigger_datetime_created', trigger_datetime_created)
                                        .where(
                                                    (f.col("adv_campaign_datetime_start") < f.col("trigger_datetime_created")) & 
                                                    (f.col("adv_campaign_datetime_end") > f.col("trigger_datetime_created"))
                                                )
                                        .withColumn('timestamp', f.from_unixtime(f.col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
                                        .withWatermark('timestamp', '10 minutes')
                                        .dropDuplicates(['restaurant_id', 'adv_campaign_id'])
                                        .drop("timestamp")
                                        )
    return filtered_read_stream_df

def subscribers_restaurant(spark: SparkSession) -> DataFrame:
    # вычитываем всех пользователей с подпиской на рестораны
    try:
        subscribers_restaurant_df = (
            spark.read
            .format('jdbc')
            .option('url', JDBC_URL_SUBSCRIBERS)
            .option('driver', 'org.postgresql.Driver')
            .option('dbtable', 'subscribers_restaurants')
            .option('user', USER_SUBSCRIBERS)
            .option('password', PASSWORD_SUBSCRIBERS)
            .load()
        )
    except Exception as e:
        logger.error(f"Error read from PostgreSQL: {str(e)}")

    subscribers_restaurant_df = (
        subscribers_restaurant_df
        .dropDuplicates(["client_id", "restaurant_id"])
        )

    return subscribers_restaurant_df

def join_dfs(filtered_read_stream_df, subscribers_restaurant_df) -> DataFrame:
    # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
    result = (filtered_read_stream_df.join(subscribers_restaurant_df, "restaurant_id", "inner")
             .select("restaurant_id",
                     "adv_campaign_id",
                     "adv_campaign_content",
                     "adv_campaign_owner",
                     "adv_campaign_owner_contact",
                     "adv_campaign_datetime_start",
                     "adv_campaign_datetime_end",
                     "client_id",
                     "datetime_created",
                     "trigger_datetime_created",)
             )

    return result
    
# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def save_to_postgresql_and_kafka(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    
    # записываем df в PostgreSQL с полем feedback
    send_to_local_db(df)
    
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    send_to_kafka(df)
    
    # очищаем память от df
    df.unpersist()

def send_to_local_db(df):
    postgres_df = df.withColumn("feedback", f.lit(None).cast(StringType()))
    try:
        return (postgres_df.write
            .mode("append")
            .format("jdbc")
            .option("url", JDBC_URL_LOCAL)
            .option('driver', 'org.postgresql.Driver')
            .option("dbtable", "subscribers_feedback")
            .option("user", USER_LOCAL)
            .option("password", PASSWORD_LOCAL)
            .save()
        )
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")
    
def send_to_kafka(df):
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = (
        df.withColumn("value",
                      f.to_json(
                          f.struct(
                              f.col('restaurant_id'),
                              f.col('adv_campaign_id'),
                              f.col('adv_campaign_content'),
                              f.col('adv_campaign_owner'),
                              f.col('adv_campaign_owner_contact'),
                              f.col('adv_campaign_datetime_start'),
                              f.col('adv_campaign_datetime_end'),
                              f.col('client_id'),
                              f.col('datetime_created'),
                              f.col('trigger_datetime_created')
                          )
                      )
                      )
    )
    try:
        return (kafka_df.write
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option("topic", TOPIC_NAME_OUT)
            .option("checkpointLocation", "test_query")
            .save()
        )
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")
 

if __name__ == "__main__":
    spark = spark_init('RestaurantSubscribeStreamingService')
    restaurants_stream_df = read_restaurants_stream(spark)
    filter_stream_data_df = filter_stream_data(restaurants_stream_df, f.unix_timestamp(f.current_timestamp()))
    subscribers_restaurant_df = subscribers_restaurant(spark)
    result_df = join_dfs(filter_stream_data_df, subscribers_restaurant_df)
    
    # запускаем стриминг
    result_df.writeStream \
        .foreachBatch(save_to_postgresql_and_kafka) \
        .start() \
        .awaitTermination()