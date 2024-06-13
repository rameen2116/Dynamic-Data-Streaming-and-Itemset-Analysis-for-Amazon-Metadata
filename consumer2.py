#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['association_rules_db']  # Create or connect to the database
frequent_items_collection = db['frequent_items']
frequent_pairs_collection = db['frequent_pairs']
association_rules_collection = db['association_rules']

def update_counts(item, hash_table):
    # Update counts for individual items
    hash_table[item] = hash_table.get(item, 0) + 1

def update_pair_counts(pair, hash_table):
    # Update counts for pairs of items
    hash_table[pair] = hash_table.get(pair, 0) + 1

def calculate_association_rules(frequent_pairs, frequent_items, min_confidence):
    for pair, support in frequent_pairs.items():
        if support < min_support:
            continue
        item1, item2 = pair
        confidence = support / frequent_items[item1]
        if confidence >= min_confidence:
            association_rule = {"item1": item1, "item2": item2, "confidence": confidence}
            association_rules_collection.insert_one(association_rule)

def process_data(data, frequent_items, frequent_pairs, min_support, min_confidence):
    # Extract relevant data fields
    title = data['title']
    also_buy = data['also_buy']
   
    # Update counts for individual items
    for item in also_buy:
        update_counts(item, frequent_items)

    # Update counts for pairs of items
    for i in range(len(also_buy)):
        for j in range(i + 1, len(also_buy)):
            pair = (also_buy[i], also_buy[j])
            update_pair_counts(pair, frequent_pairs)

    # Print insights or associations based on frequent items and pairs
    frequent_items_list = [item for item, count in frequent_items.items() if count >= min_support]
    frequent_pairs_list = [pair for pair, count in frequent_pairs.items() if count >= min_support]

    if frequent_items_list:
        print("Frequent items:", frequent_items_list)
        frequent_items_collection.insert_one({"frequent_items": frequent_items_list})
    if frequent_pairs_list:
        print("Frequent pairs:", frequent_pairs_list)
        frequent_pairs_collection.insert_one({"frequent_pairs": frequent_pairs_list})
        calculate_association_rules(frequent_pairs, frequent_items, min_confidence)

if __name__ == "__main__":
    # Initialize Kafka consumer
    consumer = KafkaConsumer('topic0', bootstrap_servers=['localhost:9092'], group_id='group1')

    # Initialize hash tables for counting frequent items and pairs
    frequent_items = {}
    frequent_pairs = {}

    # Define minimum support and minimum confidence thresholds
    min_support = 2  # Adjust this threshold as needed
    min_confidence = 0.5  # Adjust this threshold as needed

    # Start consuming messages
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        process_data(data, frequent_items, frequent_pairs, min_support, min_confidence)

