import pandas as pd
import time
import socket
import random
from kafka import KafkaConsumer, KafkaProducer
import threading

class Producer(threading.Thread):
    daemon = True
    crm_weekly = pd.read_csv("~/wedohadoop/data/msc_weekly.csv", delimiter=";")

    def __init__(self, sleep_time):
        self.sleep_time = sleep_time


    def run(self, host="localhost", port=9092):
        producer = KafkaProducer(bootstrap_servers=host+":"+str(port))

        while True:
            record = random.randint(0, self.crm_weekly.shape[0] - 1)
            producer.send('data', bytes(";".join(str(x) for x in self.crm_weekly.ix[record])+"\n", "UTF-8"))
            time.sleep(self.sleep_time)

x = Producer(1)
x.run()
