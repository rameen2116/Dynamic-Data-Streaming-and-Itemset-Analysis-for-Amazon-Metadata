#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json

def preprocess_data(input_file, output_file):
    with open(input_file, 'r') as f_input, open(output_file, 'w') as f_output:
        for line in f_input:
            item = json.loads(line)
            preprocessed_item = {
                'title': item.get('title', 'Unknown Title'),
                'brand': item.get('brand', 'Unknown Brand'),
                'also_buy': item.get('also_buy', []),
                'also_view': item.get('also_view', []),
                'asin': item.get('asin', 'Unknown ASIN')
               }

            json.dump(preprocessed_item, f_output)
            f_output.write('\n')

input_file = 'Sampled_Amazon_Meta.json'
output_file = 'preprocessed_amazon_metadata.json'

preprocess_data(input_file, output_file)

