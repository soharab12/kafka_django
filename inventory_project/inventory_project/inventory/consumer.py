from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'inventory_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka Consumer: Listening for inventory updates...")

for message in consumer:
    print(f"Received update: {message.value}")
