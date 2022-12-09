#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import time

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()
    

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            # print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            #     topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
        
            m = msg.key().decode('utf-8')
            i = m.index("_")
            num = m[i+1:]
            if (int(num) % 100 == 0):
                print('outputed {c}'.format(c=m[i+1:]))

    # Produce data by selecting random values from these lists.
    topic = "t2"
    user_ids = ['eabaraeabaraeabaraeabaraeabaraeabaraeabaraeabaraeabaraeabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteriesbook alarm clock t-shirts gift card batteries']

    count = 0
    for i in range(1000000):
    # for i in range(10):

        user_id = choice(user_ids) + "_" + str(i)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1
        # print("Produced event: key = {key:12}".format(key=user_id))
        producer.poll(0)
        time.sleep(0.1)


        

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
