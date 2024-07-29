import requests
from utils.s3_utils import S3Utility
from utils.config_utils import load_config

def get_dk_mlb_props():
    base_config = load_config('base')
    env_config = load_config('dev')
    # API / requests
    base_url = base_config['draftkings']['url_template']
    sleep_min = base_config['draftkings']['sleep_min']
    sleep_max = base_config['draftkings']['sleep_max']
    request_headers = base_config['requests']['headers']
    # AWS
    region = base_config['aws']['region']
    s3_bucket = base_config['aws']['s3_bucket']
    s3_key = env_config['aws']['s3_key']

    # TODO: fill here; below is old code for getting DK props


import yaml
import requests
import time
import random
from datetime import datetime

def load_yml(filepath: str) -> dict:
    """Load config from yml file."""
    try:
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as exc:
        print(f"Error in configuration file:\n{exc}")

def main():
    # Load config
    config_path = "common/config.yml"
    config = load_yml(config_path)
    aws_config = config['aws'][0]
    webscraping_config = config['webscraping']
    bucket = aws_config['bucket']
    key = aws_config['bronze_draftkings_s3_key']
    url_template = webscraping_config[1]['draftkings']['url_template']
    sleep_min = webscraping_config[1]['draftkings']['sleep_min']
    sleep_max = webscraping_config[1]['draftkings']['sleep_max']

    # Load API schema
    api_path = "layers/bronze/draftkings/api.yml"
    api_mlb = load_yml(api_path)['eventgroups'][0]
    eventgroup_mlb = api_mlb['id']

    # Request data for each subcategory
    #data_dir = webscraping_config[3]['data-directory']
    headers = webscraping_config[0]['headers']
    for category in api_mlb['categories']:
        category_id = category['id']
        print(f"Category name: {category['name']}")
        for subcategory in category['subcategories']:
            subcategory_id = subcategory['id']
            subcategory_name = subcategory['name']
            print(f"Subcategory name: {subcategory_name}")
            # Construct URL
            url = url_template.format(
                eventgroup_id=eventgroup_mlb, 
                category_id=category_id, 
                subcategory_id=subcategory_id)
            print(f"Request URL: {url}")
            print(f"Request headers: {headers}")
            # Fetch data
            print("Fetching data...")
            try:
                response = requests.get(
                    url=url,
                    headers=headers
                )
                print(f"Status code: {response.status_code}")
            except Exception as e:
                print(f"Status code: {response.status_code}")
                print(e)
            # Write data
            if response.status_code == 200:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                S3Utility.upload_obj_s3(
                    bucket=bucket,
                    key=f'{key}/dk-{subcategory_name}_{timestamp}.json',
                    obj=response.text
                )
                """
                with open(f'{data_dir}/dk-{subcategory_name}_{timestamp}.json', 'w') as f:
                    json.dump(response.text, f)
                print("Successfully wrote JSON file.")
                """
            else:
                print("Error fetching data.")
            # Sleep
            sleep_secs = random.randint(sleep_min, sleep_max)
            print(f"Sleeping for {sleep_secs} seconds...")
            time.sleep(sleep_secs)

if __name__ == '__main__':
    main()