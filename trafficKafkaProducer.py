from confluent_kafka import Producer
import socket

import requests
import json

#set up connection to Kafka
conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)



#get data from VicRoadsAPI
headers = {
    # Request headers
    'Ocp-Apim-Subscription-Key': '9832fb6b4ce74451b9dccece4a37f52d',
}

data = '{body}'

try:
    response = requests.get('https://data-exchange-api.vicroads.vic.gov.au/bluetooth_data/links', headers=headers, data=data)
    data = response.text
    data_dict = json.loads(data)
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

#send data to kafka topic
topic = "traffic_status"

#callback method for asynchronous writing
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

#iterate through data received and add to topic
for i in data_dict:
    producer.produce(topic, key=i['name'], value=str(i), callback=acked)
    producer.poll(1)
