from confluent_kafka import Producer
import yaml
import json
import time
import uuid
from random import choice, randint

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
        'enable.idempotence': True,   # Prevent duplicates
    }

    producer = Producer(config)

    topic = "Orders"

    # Create sample data
    users = ['alice', 'bob', 'charlie', 'david', 'emma', 'frank']
    products = ['book', 'mouse', 'keyboard', 'laptop', 'pen', 'notebook']

    for i in range(50):
        order_id = str(uuid.uuid4())[:8]
        user = choice(users)
        product = choice(products)
        quantity = randint(1, 5)
        price = randint(10, 100) * quantity

        # Each message has a unique key → user name → determines partition
        key = user.encode('utf-8')
        value = json.dumps({
            "order_id": order_id,
            "user": user,
            "product": product,
            "quantity": quantity,
            "price": price,
            "timestamp": int(time.time())
        }).encode('utf-8')

        producer.produce(topic=topic, key=key, value=value, callback=delivery_callback)
        producer.poll(0)

        time.sleep(0.5)  # simulate stream flow

    # Ensure all messages are delivered before exiting
    producer.flush()
