#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaError
import yaml
import sys
import time

# Load Kafka cluster settings
config_settings = yaml.safe_load(open('config/settings.yml'))

BOOTSTRAP_SERVERS = config_settings['bootstrap_server']
CLUSTER_API_KEY = config_settings['cluster_api_key']
CLUSTER_API_SECRET = config_settings['cluster_api_secret']

if __name__ == '__main__':
    # Consumer ID (passed as command-line arg)
    # Example: python consumer.py 1
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else "1"

    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'sasl.username': CLUSTER_API_KEY,
        'sasl.password': CLUSTER_API_SECRET,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'acks': 'all',
        'group.id': 'orders-consumer-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False   # Disable auto commit
    }

    consumer = Consumer(config)
    topic = "Orders"
    consumer.subscribe([topic])

    print(f"Consumer {consumer_id} started and subscribed to topic: {topic}")
    print("-" * 80)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                time.sleep(0.5)
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer {consumer_id} error: {msg.error()}")
                continue

            # Decode message
            key = msg.key().decode('utf-8') if msg.key() else 'None'
            value = msg.value().decode('utf-8')
            partition = msg.partition()
            offset = msg.offset()

            # Print message details
            print(f"[Consumer {consumer_id}] "
                  f"Partition={partition} | Offset={offset} | Key={key} | Value={value}")

            # Manually commit offset in sync mode
            consumer.commit(message=msg, asynchronous=False)
            print(f"Committed offset {offset + 1} for partition {partition}")

    except KeyboardInterrupt:
        print(f"\n Consumer {consumer_id} stopped manually.")
    finally:
        consumer.close()
