from kafka import KafkaConsumer
import datetime
import json
import pyspark
from pyspark.sql.types import *
import os

# Set the PYTHONIOENCODING environment variable to UTF-8
os.environ["PYTHONIOENCODING"] = "UTF-8"

def receive_callback(message):
    print('[kafka consumer received row]')

    # convert tu byte -> dict
    decoded_message = message.decode('utf-8') 
    data_dict = json.loads(decoded_message)

    # tao SparkSession
    spark = pyspark.sql.SparkSession.builder.appName("consumer").getOrCreate()
    #  ["id", "author", "content", "picture_count", "source", "title", "topic", "url", "crawled_at"]

    # chuyen thanh dataframe
    df = spark.createDataFrame([data_dict])

    now = datetime.datetime.now()
    path = 'hdfs://namenode:9000/user/pdt/raw_rows/{}_{}_{}_{}_{}_{}_{}.csv'.format(now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond)
    # news thanh raw_rows

    # viet vao file .csv
    df.write \
        .option("header", "true") \
        .option("delimiter", "|||") \
        .csv(path)
    print('[kafka consumer saved row]')

print('hello')

# Kafka consumer configuration
consumer_config = {
    'bootstrap_servers': 'kafka-1:9092',  # Replace with your Kafka broker address
    'group_id': 'my_consumer_group',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 100,
}

print("configed")

# Create Kafka consumer instance
consumer = KafkaConsumer(**consumer_config)

print("created")

# Subscribe to a Kafka topic
kafka_topic = 'TrafficAccident'  # Replace with your Kafka topic
consumer.subscribe([kafka_topic])

print("subscribed")

try:
    while True:
        records = consumer.poll(1000) # timeout in millis , here set to 1 min

        record_list = []
        for tp, consumer_records in records.items():
            for consumer_record in consumer_records:
                receive_callback(consumer_record.value)

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
