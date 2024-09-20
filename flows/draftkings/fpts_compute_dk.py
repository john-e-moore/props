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
def fetch_props_data():
    pass

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