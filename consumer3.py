#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
import json
from collections import defaultdict
from pymongo import MongoClient

class TreeNode:
    def __init__(self, name, count, parent):
        self.name = name
        self.count = count
        self.parent = parent
        self.children = {}
        self.node_link = None

    def increment(self, count):
        self.count += count

def construct_tree(transactions, min_support):
    header_table = defaultdict(int)
    
    for transaction in transactions:
        for item in transaction:
            header_table[item] += 1
    
    header_table = {k: v for k, v in header_table.items() if v >= min_support}
    
    if len(header_table) == 0:
        return None, None
    
    for item in header_table:
        header_table[item] = [header_table[item], None]
    
    root = TreeNode("Null", 1, None)
    
    for transaction, count in transactions.items():
        transaction = [item for item in transaction if item in header_table]
        transaction.sort(key=lambda item: header_table[item][0], reverse=True)
        if len(transaction) > 0:
            update_tree(transaction, root, header_table, count)
    
    return root, header_table

def update_tree(transaction, node, header_table, count):
    if transaction[0] in node.children:
        node.children[transaction[0]].increment(count)
    else:
        node.children[transaction[0]] = TreeNode(transaction[0], count, node)
        if header_table[transaction[0]][1] is None:
            header_table[transaction[0]][1] = node.children[transaction[0]]
        else:
            update_header(header_table[transaction[0]][1], node.children[transaction[0]])
    
    if len(transaction) > 1:
        update_tree(transaction[1:], node.children[transaction[0]], header_table, count)

def update_header(node_to_test, target_node):
    while node_to_test.node_link is not None:
        node_to_test = node_to_test.node_link
    node_to_test.node_link = target_node

def ascend_tree(node, prefix_path):
    if node.parent is not None:
        prefix_path.append(node.name)
        ascend_tree(node.parent, prefix_path)

def find_prefix_path(base_path, node):
    conditional_patterns = {}
    while node is not None:
        prefix_path = []
        ascend_tree(node, prefix_path)
        if len(prefix_path) > 1:
            conditional_patterns[tuple(prefix_path[1:])] = node.count
        node = node.node_link
    return conditional_patterns

def mine_frequent_itemsets(header_table, min_support):
    frequent_itemsets = []
    for item, item_info in header_table.items():
        base_pattern = [item]
        frequent_itemsets.append(([item], item_info[0]))
        conditional_patterns = find_prefix_path(base_pattern, item_info[1])
        conditional_tree, conditional_header = construct_tree(conditional_patterns, min_support)
        if conditional_header is not None:
            conditional_itemsets = mine_frequent_itemsets(conditional_header, min_support)
            for conditional_itemset in conditional_itemsets:
                frequent_itemsets.append(([item] + list(conditional_itemset[0]), conditional_itemset[1]))
    return frequent_itemsets

def consume_data(consumer, min_support):
    transactions = defaultdict(int)
    
    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))
            transaction = [data['asin'], data['brand']]
            if 'also_buy' in data:
                transaction.extend(data['also_buy'])
            if 'also_view' in data:
                transaction.extend(data['also_view'])
            transactions[tuple(transaction)] += 1
        except Exception as e:
            print(f"Error processing message: {e}")
    
    root, header_table = construct_tree(transactions, min_support)
    
    frequent_itemsets = mine_frequent_itemsets(header_table, min_support)
    
    # Store frequent itemsets in MongoDB
    store_frequent_itemsets(frequent_itemsets)
    
    return frequent_itemsets

def store_frequent_itemsets(frequent_itemsets):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['frequent_itemsets_db']
    collection = db['frequent_itemsets']
    
    for itemset, support in frequent_itemsets:
        itemset_dict = {'itemset': itemset, 'support': support}
        collection.insert_one(itemset_dict)

topic = 'topic6'
min_support = 5

# Initialize Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

# Mine frequent itemsets
frequent_itemsets = consume_data(consumer, min_support)

consumer.close()


