from json import load
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import Consumer
# from confluent_kafka.schema_registry.json_schema import JSONSerializer
import sys

from init_database import get_mongo_conn, insert_stream_batch
from utils import json_deserializer

MIN_COMMIT_COUNT = 10

bootstrap_servers = ['localhost:9092']
mytopics = ['python_topic']

conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "python_consumer_group",
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    # 'value.serializer': JSONSerializer()
}

# modify this
def get_consumer():
    return Consumer(conf)

def consume_loop(consumer, topics):
    tweet_queue = []
    try:
        consumer.subscribe(topics)

        msg_count = 0
        with get_mongo_conn() as db_connection: # so it doesnt create a new connection per insert.
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # print ("%s:%d:%d: key=%s value=%s" % (msg.topic(), msg.partition(), msg.offset(), msg.key(),msg.value()))
                    # print(type(msg.value()))

                    # insert into database
                    # convert bytes to dict to insert into mongodb and add to tweet_queue
                    tweet = json_deserializer(msg.value())
                    tweet_queue.append(tweet)

                    # batching I think
                    msg_count += 1
                    if msg_count % MIN_COMMIT_COUNT == 0:
                        consumer.commit(asynchronous=True)
                        print("\n\n\ncommitted %d messages \n\n\n\n\n" % (msg_count) )

                        # insert each tweet in the array
                        # for index, elem in enumerate(tweet_queue):
                        #     print(elem)
                        #     insert_stream(db_connection, tweet=elem)
                        #     if index == (len(tweet_queue) -1):
                        #         print(f"inserted {len(tweet_queue)} tweets")
                        
                        insert_stream_batch(db_conn=db_connection, tweets=tweet_queue)
                        # empty the array
                        tweet_queue.clear()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# try:
#     for message in consumer:
#         print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
# except KeyboardInterrupt:
#     sys.exit()
#consumer.subscribe([topicName])

# move this to new script where you'll insert to database

if __name__ == '__main__':
    myconsumer = get_consumer()
    consume_loop(consumer=myconsumer, topics=mytopics)
