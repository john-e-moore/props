import duckdb
import os
import json
from fuzzywuzzy import process
import pandas as pd
from utils.utils import load_config

################################################################################
# Configuration
################################################################################
environment = os.getenv('PROPS_ENVIRONMENT')
environment_config = load_config(environment)
base_config = load_config('base')

# Web requests
requests_config = base_config['draftkings']['requests']
url_template = requests_config['url_template']
api = requests_config['api']
sleep_secs_min = requests_config['sleep_secs_min']
sleep_secs_max = requests_config['sleep_secs_max']
retries_max = requests_config['retries_max']
headers = requests_config['headers']
# NOTE: uncomment when introducing proxy rotation.
#proxies = requests_config['proxies']
proxies = None

# S3
s3_bucket = environment_config['aws']['s3_bucket']
s3_base_key = environment_config['aws']['s3_key']
file_extension_raw = environment_config['aws']['file_extension_raw']
file_extension_processed = environment_config['aws']['file_extension_processed']

# DuckDB
db_path = environment_config['duckdb']['db_path']
db_name = environment_config['duckdb']['db_name']



dk_data_dir = '/home/john/props/data/draftkings/player_name_matching'

# Connect to your DuckDB database
con = duckdb.connect(f'{db_path}/{db_name}')
table1 = 'fact_dk_offers'
column_table1 = 'participant_name'
table2 = 'players'
column_table2 = 'display_name'

# Query distinct names from both tables
query_table1 = f"SELECT DISTINCT {column_table1} FROM {table1};"
query_table2 = f"SELECT DISTINCT {column_table2} FROM {table2};"

table1_names = pd.DataFrame(con.execute(query_table1).fetchall(), columns=[column_table1])
table2_names = pd.DataFrame(con.execute(query_table2).fetchall(), columns=[column_table2])

# Convert to lists for easier comparison
names1 = table1_names[column_table1].tolist()
names2 = table2_names[column_table2].tolist()

# Find exact matches
exact_matches = sorted(list(set(names1) & set(names2)))
no_match_table1 = sorted(list(set(names1) - set(names2)))

# Save exact matches to a file
with open(f'{dk_data_dir}/exact_matches.txt', 'w') as f:
    for name in exact_matches:
        f.write(name + "\n")

# Save no-match names from table1 to a file
with open(f'{dk_data_dir}/no_match_table1.txt', 'w') as f:
    for name in no_match_table1:
        f.write(name + "\n")

# Find approximate matches for names in table1 that have no exact match in table2
approximate_matches = []
no_approximate_matches = []
for name in no_match_table1:
    match = process.extractOne(name, names2, score_cutoff=80)  # 80 is the score threshold for a good match
    if match:
        approximate_matches.append((name, match[0]))
    else:
        no_approximate_matches.append(name)

# Sort approximate matches
approximate_matches.sort(key=lambda x: x[0])
no_approximate_matches.sort()

# Save approximate matches to a text file
with open(f'{dk_data_dir}/approximate_matches.txt', 'w') as f:
    for name, match in approximate_matches:
        f.write(f"{name} -> {match}\n")

# Save approximate matches to a JSON file
approximate_matches_dict = {name: match for name, match in approximate_matches}
with open(f'{dk_data_dir}/dk_to_nfldatapy.json', 'w') as f:
    json.dump(approximate_matches_dict, f, indent=4)

# Save no approximate match names to a file
with open(f'{dk_data_dir}/no_approximate_matches.txt', 'w') as f:
    for name in no_approximate_matches:
        f.write(name + "\n")

# Print results to terminal
print("Exact Matches:")
print("\n".join(exact_matches))

print("\nNames in table1 with No Match in table2:")
print("\n".join(no_match_table1))

print("\nApproximate Matches:")
for name, match in approximate_matches:
    print(f"{name} -> {match}")

print("\nNames with no approximate matches:")
print("\n".join(no_approximate_matches))
