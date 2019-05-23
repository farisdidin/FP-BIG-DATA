from time import sleep
from json import dumps
from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

with open("../dataset/ratings.csv") as file:
    next(file)
    # reader = csv.DictReader(file,delimiter=",")
    for i,row in enumerate(file):
        producer.send('movies', value=row)
        print(row)
        if i > 100:
            break
        sleep(1)

# with open("../dataset/ratings.csv",'r') as file:
#     count = 1
#     next(file)
#     for i,line in enumerate(file):
#         print(line)
#         if i > 10:
#             break