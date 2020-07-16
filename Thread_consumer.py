
from threading import Thread
from kafka import KafkaConsumer
import threading
import json
import time
import logging

class Consumer(threading.Thread):
    def __init__(self, site):
        threading.Thread.__init__(self)
        self.site = site
        bootstrap_servers = ["203.255.92.48:9092"]
        self.CONSUMER = KafkaConsumer(
            site,
            bootstrap_servers=bootstrap_servers,
            group_id='test',
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def run(self):
        while True:
            time.sleep(1)
            logging.warning('consumer')
            for msg in self.CONSUMER:
                print(msg)
