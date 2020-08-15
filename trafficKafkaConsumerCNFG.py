from confluent_kafka import Consumer
from confluent_kafka import TrafficPartition
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
import sys

class TrafficOConsumer:

    """
    Initialise consumer object with group_id, server_id (e.g. localhost:9092) and
    a list of topics
    """
    def __init__(self, group_id, server_id, topics):
        conf = {'bootstrap.servers': "%s" % server_id, 
            'group.id': "%s" % group_id,
            'auto.offset.reset': 'smallest'}
        self.topics = topics
        self.consumer = Consumer(conf)
        self.consumer.subscribe(topics)


    """
    Assign a new custom topic, partition or offset to consumer object
    offset argument is optional, if left blank will continue to read from
    last known offset
    """
    def customPartition(self, topic, partition, offset=False):
        if not offset:
            consumer_settings = TrafficPartition(topic, partition)
            self.consumer.assign([consumer_settings])
        else:
            consumer_settings = TrafficPartition(topic, partition, offset)
            self.consumer.assign([consumer_settings])


    """
    Start a consume loop, from the assigned parition and offset
    """
    def consume_loop(self):
        try:
            msg_count = 0
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    print(msg.value())
                    msg_count += 1
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    """
    Set a defined number of data to be consumed
    """
    def consume_messages(self, number):
        try:
            for i in range(number):
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    print(msg.value())
                    print("Message number" + str(i))
                    
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

sampleConsumer = TrafficOConsumer(["traffic_status"], "foo", "localhost:9092")
sampleConsumer.customPartition("traffic_status",0, 20)
sampleConsumer.consume_messages(20)
sampleConsumer.customPartition("traffic_status",0, 10)
sampleConsumer.consume_messages(20)
