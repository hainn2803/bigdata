import json
import time
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import pprint
import pandas

# Kafka broker address
bootstrap_servers = 'localhost:29092'
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)


# Kafka topic to publish to
kafka_topic = 'TrafficAccident'
topic = NewTopic(name=kafka_topic, num_partitions=3, replication_factor=2)
try:
    admin_client.create_topics([topic])
except Exception as e:
    print(f"Bi loi nay: {e}")

# Read JSON data from the file
data_source_path = os.path.join(os.path.dirname(__file__), '2022-09-06.csv')
csv_data = pandas.read_csv(data_source_path)
csv_data = csv_data[['ID', 'Severity', 'Start_Time', "Weather_Condition"]]
csv_data = csv_data.to_dict('records')

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Function to publish an article to Kafka topic
def send_a_row(row):
    key = str(row["ID"]).encode('utf-8')  # Encode the key to bytes
    value = json.dumps(row).encode('utf-8')
    producer.send(kafka_topic, key=key, value=value)
    producer.flush()
    print('sent one row')
print(len(csv_data))
i = 0
# Publish an article every 5 seconds
for row in csv_data:
    send_a_row(row)
    i += 1
    time.sleep(0.5)
    # if i == 50:
    #     break

# Close the Kafka producer
producer.close()
