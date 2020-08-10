#!/usr/bin/env/ python3
''' 
Simple Python Kafka Consumer with SSL, AVRO, and Schema Registry query
Export CSV File

Python 3.xxx
!!! Min. Confluent Kafka Client 1.4.2 

Autor: Thomas Cigolla, 2020-08-17
Version: 0.001
'''
import datetime
import csv

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

topic = "TOPIC NAME"
out_file = open("OUT-FILE-NAME.csv", 'at', newline='', encoding='utf=8')

  
def date_iso():
    return datetime.datetime.now().isoformat()        

# Schema Registry Configuration
schema_conf = {'url': "SCHEMA URL",
               'ssl.ca.location' : "PATH/CA-FILE",
               'ssl.key.location' : "PATH/KEY-FILE",
               'ssl.certificate.location' : "PATH/CERT-FILE"
            }

schema_registry_client = SchemaRegistryClient(schema_conf)
schema_str = schema_registry_client.get_latest_version("SCHEMA-NAME")
schema_obj = schema_registry_client.get_schema(schema_str.schema_id)


avro_deserializer = AvroDeserializer(schema_obj.schema_str,
                                    schema_registry_client,
                                    dict_to_user)
string_deserializer = StringDeserializer('utf_8')

consumer_conf = {'bootstrap.servers': "BOOTSTRAP-SERVER-NAME",
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': "GROUP-ID",
                     'auto.offset.reset': "earliest",
                     'security.protocol' : 'ssl',
                     'ssl.ca.location' : "PATH/CA-FILE",
                     'ssl.key.location' : "PATH/KEY-FILE",
                     'ssl.certificate.location' : "PATH/CERT-FILE"
                    }

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([topic])

# Start
i = 0 # Counter for messages
j = 0 # Counter for empty messages
h = 0 # CSV Header 

print ('Start reading Kafka Topic:', topic, 'Time:', date_iso())

while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1.0)
        if msg is None:
            j += 1
            continue

        data = msg.value()
        if data is not None:
            if h == 0
                fildnames = data.keys()
                writer = csv.DictWriter(out_file, fieldnames=fieldnames)
                writer.writeheader()
                h += 1
                    
            writer.writerow(data)   
            i += 1

    except KeyboardInterrupt:
        break
    
    print ('Message:', i, 'NONE:',  j, end='\r', flush=True)

out_file.close()
consumer.close()













