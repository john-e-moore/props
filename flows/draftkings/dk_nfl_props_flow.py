# Standard
import os
import sys
import json
import requests
# External
import duckdb
import pandas as pd
from typing import Dict, Any, List, Optional
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

# Prefect flow
retries = environment_config['prefect']['retries']
retry_delay_seconds = environment_config['prefect']['retry_delay_seconds']
log_prints = environment_config['prefect']['log_prints']

################################################################################
# Tasks
################################################################################
@task
def issue_requests():
    """
    Request props data from each endpoint.

    :return: Dictionary containing subcategory names and HTTP responses.
    """
    # Main scraping loop.
    print("Scraping DraftKings for NFL season-long player props.")
    responses = dict()
    nfl_seasonlong_eventgroup = get_event_group_by_name(api, 'nfl-seasonlong')
    # Find the player-stats category.
    for category in nfl_seasonlong_eventgroup['categories']:
        if category['name'] == 'player-stats': 
            print(f"Category: {category['name']}")
            # Check if subcategories exist
            if 'subcategories' in category and category['subcategories']:
                for subcategory in category['subcategories']:
                    subcategory_name = subcategory['name']
                    print(f"Subcategory: {subcategory_name}")

                    # Construct URL
                    url_params = {
                        'eventgroup_id': nfl_seasonlong_eventgroup['eventgroup_id'],
                        'category_id': category['category_id'],
                        'subcategory_id': subcategory['subcategory_id']
                    }
                    url = request_handler.construct_url(url_template, **url_params)

                    # Fetch data
                    print(f"Fetching data from {url}")
                    response = request_handler.get(url, headers)
                    responses[subcategory_name] = response
            else:
                print("No subcategories available.")
    print("Done.")
    return responses
                    

@task
def upload_raw_data_s3(responses: Dict[str, requests.Response]):
    """
    Converts objects to JSON strings and uploads to S3.

    :param responses: Dictionary containing subcategory names and HTTP responses.
    """
    for subcategory_name, obj in responses:
        json_obj = obj.json()
        json_string_obj = json.dumps(json_obj)
        s3_handler.upload_object(
            obj=json_string_obj, 
            object_name=subcategory_name,
            base_key=f'{s3_base_key}/draftkings/raw',
            file_extension=file_extension_raw
        )
    

@task
def parse_and_flatten_raw_data(responses: Dict[str, requests.Response]):
    """
    Parses and flattens data from each endpoint, then concatenates the data
    into a Pandas DataFrame.

    :param responses: Dictionary containing subcategory names and HTTP responses.
    :return: Pandas DataFrame containing player prop data.
    """
    pass

@task
def upload_parsed_data_s3():
    pass

@task
def load_parsed_data_duckdb():
    pass
#######################################
# Develop the last two transformations after the first 5 tasks are deployed;
# no reason not to start filling the database
@task
def group_data_by_player():
    # can do this in sql: transformations/sql
    pass

@task
def compute_averages_from_vegas_odds():
    # this should use stuff from transformations/python
    pass

@task
def compute_fpts_columns():
    # transformations/sql
    pass

@task
def upload_final_dataset_s3():
    pass

################################################################################
# Flow
################################################################################
@flow
def dk_nfl_props(log_prints=log_prints, retries=retries, retry_delay_seconds=retry_delay_seconds):
    """
    ***Extract / Load***
    1. Request props data from each endpoint.
    2. Upload raw response JSON to S3.
    3. Parse response JSON.
    4a. Upload parsed CSV to S3.
    4b. Load parsed CSV to DuckDB.
    -----------------------------------
    ***Transformations***
    5. Group data by player.
    6. Compute averages from Vegas odds using scikitlearn.
    7. Create DuckDB view computing fantasy points for every scoring system.
    8. Read the finished view and upload CSV to S3. 
    """
    print(f"duckdb location: {db_path}/{db_name}")

    responses = issue_requests()

    upload_raw_data_s3(responses)

    df = parse_and_flatten_raw_data(responses)


if __name__ == "__main__":
    # Instantiate handlers
    request_handler = RequestHandler(
        sleep_secs_min=sleep_secs_min,
        sleep_secs_max=sleep_secs_max,
        retries_max=retries_max,
        proxies=proxies
    )

    s3_handler = S3Handler(s3_bucket)

    dk_nfl_props.serve(
        name='dk_nfl_props',
        cron='* * * * *',
        tags=['dk', 'props'],
        # default description is flow's docstring
        version='0.1',
    )       
