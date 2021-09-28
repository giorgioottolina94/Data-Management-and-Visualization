#!/usr/bin/env python
# coding: utf-8

# # Script producer da file csv

# # Meteo

import sys
anno = str(sys.argv[1])



file_path = '/data/my-data/meteo/{}.csv'.format(anno)
# qui serve scaricare il file csv e metterlo nella cartella data

print('Download Data from file: ',file_path)


from kafka import KafkaProducer
import json
import time
import csv
# We create a KafraProducer and we pass a lambda function as value serializer.
# This means the message we pass as value of the send method will be converted to JSON
# using the lambda function.

producer = KafkaProducer(
  bootstrap_servers=["kafka:9092"],
  value_serializer=lambda v: json.dumps(v).encode("utf-8"))


print('\nKafka Connected')


# metodo per leggere grossi file csv linea per linea 
data = csv.DictReader(open(file_path))
# streammiamo le righe su kafka, topico meteo
for row in data:
    producer.send(topic = 'meteo',value=row)





