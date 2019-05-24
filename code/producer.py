from __future__ import print_function
from time import sleep
from json import dumps
from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

with open("dataset/ratings2.csv") as f:
    # reader = csv.DictReader(file,delimiter=",")
    # file = f.readlines()
    next(f)
    for row in f:
        row=row.rstrip("\n")
        producer.send('movies', value=row)
        print(row)
        sleep(0.001)

