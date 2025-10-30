from random import choice
from confluent_kafka import Producer
import yaml

# loading the kafka cluster settings from the yaml file
config_settings = yaml.safe_load(open('config/settings.yml'))

BOOTSTRAP_SERVERS = config_settings['bootstrap_server']
CLUSTER_API_KEY = config_settings['cluster_api_key']
CLUSTER_API_SECRET = config_settings['cluster_api_secret']

if __name__ == '__main__':

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'sasl.username':     CLUSTER_API_KEY,
        'sasl.password':     CLUSTER_API_SECRET,

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = "Orders"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0
    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()