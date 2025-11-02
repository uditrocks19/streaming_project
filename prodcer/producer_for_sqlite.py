from confluent_kafka import Producer
import yaml
import json
import time
import uuid
from random import choice, randint
import time

# Load Kafka config
config_settings = yaml.safe_load(open('config/settings.yml'))

BOOTSTRAP_SERVERS = config_settings['bootstrap_server']
CLUSTER_API_KEY = config_settings['cluster_api_key']
CLUSTER_API_SECRET = config_settings['cluster_api_secret']

def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] | key={msg.key().decode()} | offset={msg.offset()}")

if __name__ == '__main__':
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'sasl.username': CLUSTER_API_KEY,
        'sasl.password': CLUSTER_API_SECRET,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'acks': 'all',
        'batch.size': 32000,
        'linger.ms': 20,
        'enable.idempotence': True,   # Prevent duplicates
    }

    producer = Producer(config)

    topic = "Orders"

    # Create sample data
    tick_data = [
        {"symbol": "AAPL", "price": 150.12},
        {"symbol": "GOOGL", "price": 2750.65},
        {"symbol": "MSFT", "price": 299.87},
        {"symbol": "AMZN", "price": 3400.50},
        {"symbol": "TSLA", "price": 720.30}
    ]


    # Each message has a unique key → symbol → determines partition
    for tick in tick_data:
        key = tick["symbol"].encode('utf-8')
        value = json.dumps({
            "symbol": tick["symbol"],
            "price": tick["price"],
        }).encode('utf-8')

        producer.produce(topic=topic, key=key, value=value, callback=delivery_callback)
        producer.poll(0)
        time.sleep(1)  # Simulate time delay between ticks
    
    producer.flush()