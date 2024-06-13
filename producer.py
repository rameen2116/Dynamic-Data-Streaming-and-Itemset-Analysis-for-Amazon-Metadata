#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaProducer
import json
import time

def produce_data(producer, input_file, topic):
    with open(input_file, 'r') as file:
        for line in file:
            data = json.loads(line.strip())
            producer.send(topic, json.dumps(data).encode('utf-8'))
            time.sleep(0.1)  # Simulate streaming delay
            print("Produced:", data)

if __name__ == "__main__":
    input_file = 'preprocessed_amazon_metadata.json'
    topic = 'topic6'
   
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Produce data
    produce_data(producer, input_file, topic)

