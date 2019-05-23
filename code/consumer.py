from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'movies',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

with open('output.csv','a') as output:
    output.write("userId,movieId,rating,timestamp\n")
    for i,message in enumerate(consumer):
        message = message.value
        output.write(message)
        # collection.insert_one(message)
        # print('{} added to {}'.format(message, collection))\
        print(message)
        if i > 100:
            break