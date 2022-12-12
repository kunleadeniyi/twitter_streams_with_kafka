from confluent_kafka import Producer
import socket
import time

from template import get_fake_user
from utils import json_serializer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'client.id': socket.gethostname()
}
bootstrap_server = ['localhost:9092']
topicName = 'python_topic'
myproducer = Producer(conf)

# modify this
def get_producer():
    return Producer(conf)

# producer = KafkaProducer()

# params = json.dump(producer.__dict__, )
# print(params)
# pprint.pprint(producer.__dict__, indent=4)

if __name__ == "__main__":
    while True:
        try:
            # push to broker    
            myproducer.produce(
                'python_topic',
                value=json_serializer(get_fake_user())
            )
            time.sleep(1)
        except Exception as error:
            print(str(error))


