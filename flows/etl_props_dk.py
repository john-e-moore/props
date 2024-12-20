# Standard
import os
import sys
import json
import requests
from io import StringIO
# External
import duckdb
import pandas as pd
from typing import Dict, Any, List, Optional
from prefect import flow, task, get_run_logger
# Internal
from handlers.s3_handler import S3Handler
from handlers.request_handler import RequestHandler
from handlers.duckdb_handler import DuckDBHandler
from utils.utils import load_config, get_event_group_by_name, generate_timestamp, parse_dk_offers

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
def issue_request(request_handler: RequestHandler, url: str, logger):
    """
    Request props data from each endpoint.

    :return: Dictionary containing subcategory names and HTTP responses.
    """
    logger.info(f"Fetching data from {url}")
    return request_handler.get(url, headers)
                    

@task
def upload_raw_data_s3(s3_handler: S3Handler, response: requests.Response, subcategory_name: str, timestamp: str, logger) -> None:
    """
    Converts objects to JSON strings and uploads to S3.

    :param response: requests.Response HTTP response (json).
    """
    try:
        json_obj = response.json()
    except AttributeError as e:
        logger.info(f"No JSON object found.\n{e}")
        return None
    json_string_obj = json.dumps(json_obj)
    s3_handler.upload_object(
        obj=json_string_obj, 
        object_name=subcategory_name,
        base_key=f'{s3_base_key}/draftkings/raw',
        file_extension=file_extension_raw,
        timestamp=timestamp
    )
    

@task
def parse_raw_data(response: requests.Response, timestamp: str, logger) -> list:
    """
    Parses and flattens data from each endpoint, then concatenates the data
    into a Pandas DataFrame.

    :param response: requests.Response HTTP response (json).
    :param timestamp: 14-character timestamp string.

    :return: Flattened JSON containing parsed player prop data.
    """
    try:
        json_obj = response.json()
    except AttributeError as e:
        logger.info(f"No JSON object found.\n{e}")
        return None
    
    return parse_dk_offers(json_obj, timestamp)
    
@task
def upload_parsed_data_s3(s3_handler: S3Handler, combined_offers: str, subcategory_name: str, timestamp: str, logger) -> None:
    """
    Converts objects to JSON strings and uploads to S3.

    :param combined_offers: JSON string of combined offers
    :param response: requests.Response HTTP response (json).
    """
    s3_handler.upload_object(
        obj=combined_offers, 
        object_name=subcategory_name,
        base_key=f'{s3_base_key}/draftkings/parsed',
        file_extension=file_extension_processed,
        timestamp=timestamp
    )

@task
def load_parsed_data_duckdb(parsed_json: dict, db_path_full: str, logger) -> None:
    table_name = "fact_dk_offers"
    duckdb_handler = DuckDBHandler(db_path_full)

    create_table_statement = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        subcategory_subcategoryId VARCHAR,
        subcategory_name VARCHAR,
        offer_label VARCHAR,
        offer_providerOfferId VARCHAR,
        offer_eventId VARCHAR,
        offer_eventGroupId VARCHAR,
        offer_playerNameIdentifier VARCHAR,
        outcome_label VARCHAR,
        outcome_oddsAmerican VARCHAR,
        outcome_oddsDecimal DOUBLE,
        outcome_line DOUBLE,
        participant_id VARCHAR,
        participant_name VARCHAR,
        participant_type VARCHAR,
        timestamp TIMESTAMP
    );
    """
    duckdb_handler.execute(create_table_statement)

    combined_offers_df = pd.DataFrame(parsed_json)
    combined_offers_df['timestamp'] = pd.to_datetime(combined_offers_df['timestamp'], format='%Y%m%d%H%M%S')
    duckdb_handler.insert_data(table_name, combined_offers_df)

################################################################################
# Flow
################################################################################
@flow
def etl_props_dk(log_prints=log_prints, retries=retries, retry_delay_seconds=retry_delay_seconds):
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
    logger = get_run_logger()
    logger.info(f"duckdb location: {db_path}/{db_name}")

    s3_handler = S3Handler(s3_bucket)

    request_handler = RequestHandler(
        sleep_secs_min=sleep_secs_min,
        sleep_secs_max=sleep_secs_max,
        retries_max=retries_max,
        proxies=proxies
    )
    # Extract, load, and parse
    logger.info("Scraping DraftKings odds.")
    nfl_seasonlong_eventgroup = get_event_group_by_name(api, 'nfl')
    parsed_offers_list = []
    for category in nfl_seasonlong_eventgroup['categories']:
        if category['name'] != 'player-stats': # Don't get full season stuff
            logger.info(f"Category: {category['name']}")
            # Check if subcategories exist
            if 'subcategories' in category and category['subcategories']:
                for subcategory in category['subcategories']:
                    subcategory_name = subcategory['name']
                    logger.info(f"Subcategory: {subcategory_name}")

                    # Construct URL
                    url_params = {
                        'eventgroup_id': nfl_seasonlong_eventgroup['eventgroup_id'],
                        'category_id': category['category_id'],
                        'subcategory_id': subcategory['subcategory_id']
                    }
                    url = request_handler.construct_url(url_template, **url_params)
                    response = issue_request(request_handler, url, logger)
                    # Use the timestamp of the response for all future operations
                    timestamp = generate_timestamp()
                    upload_raw_data_s3(s3_handler, response, subcategory_name, timestamp, logger)
                    parsed_offers = parse_raw_data(response, timestamp, logger)
                    parsed_offers_list.append(parsed_offers)
                    if parsed_offers_list:
                        logger.info("Parsed offer list is not empty after updating for this category.")
                    else:
                        logger.info("Parsed offer list is empty after updating for this category.")
    
    # Combine parsed offers and upload to S3
    combined_offers = []
    for sublist in parsed_offers_list:
        if not sublist:
            logger.info("Sublist is empty.")
            continue
        else:
            logger.info("Sublist is not empty.")
        for item in sublist:
            combined_offers.append(item)
    combined_offers_json_str = json.dumps(combined_offers, indent=4)
    logger.info("Uploading parsed data.")
    upload_parsed_data_s3(s3_handler, combined_offers_json_str, 'parsed_props', timestamp, logger)

    # Insert into duckdb
    db_path_full = f"{db_path}/{db_name}"
    combined_offers_json_obj = json.loads(combined_offers_json_str)
    load_parsed_data_duckdb(combined_offers_json_obj, db_path_full, logger)

    logger.info("Done.")
    logger.info("Parsed offer list empty; database not updated.")


if __name__ == "__main__":
    etl_props_dk.serve(
        name='etl_props_dk',
        #cron='17 * * * *', # 17 just to be friendly to their server
        tags=['etl', 'dk', 'props'],
        # default description is flow's docstring
        version='0.1',
    )       
