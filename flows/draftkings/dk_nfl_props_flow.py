# Standard
import os
import sys
import json
# External
import duckdb
import pandas as pd
from prefect import flow, task
# Internal
from handlers.s3_handler import S3Handler
from handlers.request_handler import RequestHandler
from utils.utils import load_config, get_event_group_by_name, generate_timestamp

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
#proxies = requests_config['proxies']

# S3
s3_bucket = environment_config['aws']['s3_bucket']
s3_base_key = environment_config['aws']['s3_key']
file_extension_raw = environment_config['aws']['file_extension_raw']
file_extension_processed = environment_config['aws']['file_extension_processed']

# DuckDB
db_path = environment_config['duckdb']['db_path']
db_name = environment_config['duckdb']['db_name']

# Prefect flow
retries = environment_config['prefect']['retries']
retry_delay_seconds = environment_config['prefect']['retry_delay_seconds']
log_prints = environment_config['prefect']['log_prints']

################################################################################
# Tasks
################################################################################
@task
def web_request():
    pass

@task
def upload_raw_data_s3():
    pass

@task
def parse_and_flatten_raw_data():
    pass

@task
def upload_parsed_data_s3():
    pass

@task
def load_parsed_data_duckdb():
    pass

@task
def group_data_by_player():
    pass

@task
def compute_fpts_columns():
    pass

@task
def upload_final_dataset_s3():
    pass

################################################################################
# Flow
################################################################################



if __name__ == "__main__":
    # Handlers
    request_handler = RequestHandler(
        headers=headers,
        sleep_secs_min=sleep_secs_min,
        sleep_secs_max=sleep_secs_max,
        retries_max=retries_max,
        proxies=None
    )
    s3_handler = S3Handler(s3_bucket)

    # Main scraping loop.
    print("Scraping DraftKings for NFL season-long player props.")
    nfl_seasonlong_eventgroup = get_event_group_by_name(api, 'nfl-seasonlong')
    # Find the player-stats category.
    for category in nfl_seasonlong_eventgroup['categories']:
        if category['name'] == 'player-stats': 
            print(f"Category: {category['name']}")
            # Check if subcategories exist
            if 'subcategories' in category and category['subcategories']:
                for subcategory in category['subcategories']:
                    print(f"Subcategory: {subcategory['name']}")

                    # Construct URL
                    url_params = {
                        'eventgroup_id': nfl_seasonlong_eventgroup['eventgroup_id'],
                        'category_id': category['category_id'],
                        'subcategory_id': subcategory['subcategory_id']
                    }
                    print(url_template)
                    print(url_params)
                    url = request_handler.construct_url(url_template, **url_params)

                    # Fetch data
                    print(f"Fetching data from {url}")
                    response = request_handler.get(url)
                    json_data = response.json()
                    json_string_data = json.dumps(json_data)

                    # Write to S3
                    s3_handler.upload_object(
                        obj=json_string_data, 
                        object_name=subcategory['name'],
                        base_key=f'{s3_base_key}/draftkings/raw',
                        file_extension=file_extension_raw
                    )

            else:
                print("No subcategories available.")
    print("Done.")
