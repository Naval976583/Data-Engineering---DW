import json
import pandas as pd

# Read the JSON data from the file
with open("sample.json", 'r') as file:
    data = json.load(file)

# Flatten the JSON data using json_normalize, adding a prefix to metadata columns
flattened_data = pd.json_normalize(data, record_path=['qualify'],
                                   meta=['user_id', 'rec_id', 'uut'])
rules_data = pd.json_normalize(data, record_path=['qualify', 'rules'])

flattened_data = pd.concat([flattened_data, rules_data], axis=1)

flattened_data.drop('rules', axis=1, inplace=True)

parquet_output_path = "updated_table.parquet"

flattened_data.to_parquet('updated_table.parquet', index=False)

print(flattened_data)
