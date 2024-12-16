from kafka import KafkaAdminClient

# Ket noi den Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:29092",
    client_id='my-client'
)

# Ten topic
topic_name = "trafficaccident"

# Mo ta
try:
    topic_details = admin_client.describe_topics([topic_name])
    print(f"Details for topic '{topic_name}':")
    for detail in topic_details:
        print(detail)
except Exception as e:
    print(f"Loi '{topic_name}': {e}")

# Close the client
admin_client.close()
