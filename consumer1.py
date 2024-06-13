from kafka import KafkaConsumer
from collections import defaultdict
import json
import itertools

# Function to generate candidate itemsets
def generate_candidates(itemset, length):
    candidates = set()
    for item1 in itemset:
        for item2 in itemset:
            union_set = item1.union(item2)
            if len(union_set) == length:
                candidates.add(frozenset(union_set))
    return candidates

# Function to prune candidate itemsets
def prune_candidates(candidates, prev_frequent_set):
    pruned_candidates = set()
    for candidate in candidates:
        subsets = itertools.combinations(candidate, len(candidate) - 1)
        if all(frozenset(subset) in prev_frequent_set for subset in subsets):
            pruned_candidates.add(candidate)
    return pruned_candidates

# Function to count itemset occurrences in transactions
def count_occurrences(transactions, itemsets):
    counts = defaultdict(int)
    for transaction in transactions:
        for itemset in itemsets:
            if itemset.issubset(transaction):
                counts[itemset] += 1
    return counts

# Function to print frequent itemsets
def print_frequent_itemsets(frequent_itemsets):
    for itemset, support in frequent_itemsets.items():
        print("Frequent Itemset:", itemset, "Support:", support)

# Apriori algorithm implementation
def apriori(transactions, min_support):
    # Count individual item occurrences
    item_counts = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            item_counts[item] += 1
    
    # Get frequent 1-itemsets
    frequent_1_itemsets = {frozenset([item]): support for item, support in item_counts.items() if support >= min_support}
    
    frequent_itemsets = frequent_1_itemsets
    k = 2
    
    while frequent_itemsets:
        candidate_itemsets = generate_candidates(frequent_itemsets.keys(), k)
        pruned_candidates = prune_candidates(candidate_itemsets, frequent_1_itemsets.keys())
        candidate_counts = count_occurrences(transactions, pruned_candidates)
        frequent_itemsets = {itemset: support for itemset, support in candidate_counts.items() if support >= min_support}
        
        if frequent_itemsets:
            print("\nFrequent", k, "-itemsets:")
            print_frequent_itemsets(frequent_itemsets)
        
        k += 1

# Kafka consumer setup
consumer = KafkaConsumer('topic6', bootstrap_servers=['localhost:9092'], group_id='my_group')

# Consume and process data
transactions = []
for message in consumer:
    data = json.loads(message.value)
    transaction = set(data['also_buy'] + data['also_view'])  # Assuming 'also_buy' and 'also_view' are lists of items
    transactions.append(transaction)
    print("Received Transaction:", transaction)
    
    # Run Apriori algorithm on received transactions
    min_support = 2
    apriori(transactions, min_support)

