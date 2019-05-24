from kafka import KafkaConsumer
from shutil import copyfile
# from pymongo import MongoClient
from json import loads

BATCH_SIZE = 300000
BATCH_NUM = 1
OUTPUT_FILE = "output/output-"

consumer = KafkaConsumer(
    'movies',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

# consumer=[]
# for i in range(50):
#     consumer.append(i)

FILE_NAME = OUTPUT_FILE+str(BATCH_NUM)

with open(FILE_NAME+".csv",'a+') as output:
    output.write("userId,movieId,rating,timestamp\n")

count = 0 
for message in consumer:
    message = message.value
    message=str(message)
    if count >= BATCH_SIZE:
        output_file.close()
        BATCH_NUM +=1
        count = 0
        awal= FILE_NAME+".csv"
        akhir = OUTPUT_FILE+str(BATCH_NUM)+".csv"
        print(awal+" adalah awal")
        print(akhir+" adalah akhir")
        copyfile(awal, akhir)
        FILE_NAME= OUTPUT_FILE+str(BATCH_NUM)
    if count == 0 :
        output_file = open(FILE_NAME+".csv",'a+') 
    output_file.write(message+"\n")
    count+=1
    print(message)
   
    # collection.insert_one(message)
    # print('{} added to {}'.format(message, collection))\
