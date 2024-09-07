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
from handlers.dk_response_parser import DKResponseParser
from utils.utils import load_config, get_event_group_by_name, generate_timestamp
from utils.stats_utils import gamma_mean_from_market, poisson_mean_from_market, find_normal_mean, evaluate_normal_distribution

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

def calculate_weighted_score(df: pd.DataFrame, scoring_dict: dict) -> pd.DataFrame:
    """
    Adds a new column to the DataFrame that contains weighted scores.
    The score for each row is computed by multiplying a value from a dictionary
    based on the 'subcategory' key by the value in 'normal_mean_025'.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    scoring_dict (dict): A dictionary with keys as subcategories and values as scores.

    Returns:
    pd.DataFrame: The original DataFrame with an additional column 'weighted_score'.
    """
    # Map the 'subcategory' column to the scores using the dictionary
    df['score'] = df['subcategory'].map(scoring_dict)
    
    # Calculate the weighted score by multiplying the mapped score with 'normal_mean_025'
    df['weighted_score'] = df['score'] * df['mean']
    
    # Drop the intermediate 'score' column as it's not needed
    df.drop(columns='score', inplace=True)

    return df


def aggregate_fantasy_points(df: pd.DataFrame, scoring_keys: list) -> pd.DataFrame:
    """
    Enhances the DataFrame with additional boolean columns indicating the presence of data
    for each subcategory. Also calculates the sum of 'weighted_score' for each 'name'.

    Parameters:
    df (pd.DataFrame): The DataFrame containing 'name', 'subcategory', and 'weighted_score'.
    scoring_keys (list): List of keys from the scoring dictionary.

    Returns:
    pd.DataFrame: Enhanced DataFrame with boolean columns for each subcategory, 'weighted_score', and 'fpts'.
    """
    # Calculate the sum of 'weighted_score' grouped by 'name' and assign it to 'fpts'
    df['fpts'] = df.groupby('name')['weighted_score'].transform('sum')

    df.to_csv('./data/draftkings/temp_df.csv')

    # Create boolean columns for each subcategory based on presence of data
    for key in scoring_keys:
        has_key_data = df['subcategory'] == key
        df[f'has_{key}_data'] = has_key_data
        if has_key_data.any():
            df[f'{key}_mean'] = df['mean'][df['subcategory'] == key]
        else:
            df[f'{key}_mean'] = None
        #df[f'has_{key}_data'] = df.groupby('name')[key].transform('sum')


    # Group by 'name' and aggregate the data
    aggregation_functions = {f'has_{key}_data': 'any' for key in scoring_keys}
    aggregation_functions.update({f'{key}_mean': 'any' for key in scoring_keys})
    aggregation_functions.update({
        'weighted_score': 'first',  # Assuming the weighted score to aggregate as first or sum
        'fpts': 'first'  # Assuming fpts as first since it's already the aggregated sum
    })

    # Perform the aggregation
    df = df.groupby('name').agg(aggregation_functions).reset_index()

    df = df.round(1)

    return df


def issue_requests():
    """
    Request props data from each endpoint.

    :return: Dictionary containing subcategory names and HTTP responses.
    """
    # Main scraping loop.
    print("Scraping DraftKings for NFL season-long player props.")
    responses = dict()
    nfl_seasonlong_eventgroup = get_event_group_by_name(api, 'nfl')
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

if __name__ == "__main__":
    request_handler = RequestHandler(
        sleep_secs_min=sleep_secs_min,
        sleep_secs_max=sleep_secs_max,
        retries_max=retries_max,
        proxies=proxies
    )

    s3_handler = S3Handler(s3_bucket)

    responses = issue_requests()

    all_props_df = pd.DataFrame()
    for subcategory_name, response in responses.items():
        json_obj = response.json()
        parser = DKResponseParser(json_obj)

        events = parser.find_nested_value(d=json_obj, key='events')
        flattened_events = parser.flatten_item(item=events, exclude={'tags'})
        events_df = parser.flattened_events_to_dataframe(flattened_events)
        events_cols_to_keep = [
            'eventId',
            'name',
            'team1.name',
            'team2.name'
        ]
        events_df = events_df[events_cols_to_keep]

        offers = parser.find_nested_value(d=json_obj, key='offers')
        flattened_offers = parser.flatten_item(item=offers, exclude={'tags'})
        offers_df = parser.flattened_props_offers_to_dataframe(flattened_offers)

        
        offers_cols_to_keep = [
            'eventId',
            'label',
            'outcomes[0]_label',
            'outcomes[0]_oddsAmerican',
            'outcomes[0]_oddsDecimal',
            'outcomes[1]_label',
            'outcomes[1]_oddsAmerican',
            'outcomes[1]_oddsDecimal'
        ]
        offers_df = offers_df[offers_cols_to_keep]
        

        props_df = pd.merge(left=events_df, right=offers_df, how='inner', on='eventId')

        props_df.insert(1, 'subcategory', f'{subcategory_name}')

        print(f"props_df:\n{props_df.dtypes}")
        print(props_df.shape)
        print(f"all_props_df:\n{all_props_df.dtypes}")
        print(all_props_df.shape)


        if all_props_df.empty:
            all_props_df = props_df
        else:
            #all_props_df = pd.merge(all_props_df, props_df, how='outer', on='name_x', suffixes=(f'_{subcategory_name}_x', f'_{subcategory_name}_y')) # name from events_df i.e. Jordan Love 2024/25
            all_props_df = pd.concat([all_props_df, props_df], axis=0)

    # Split columns
    all_props_df[['over', 'over_number']] = all_props_df['outcomes[0]_label'].str.split(' ', expand=True)
    all_props_df[['under', 'under_number']] = all_props_df['outcomes[1]_label'].str.split(' ', expand=True)
    all_props_df['over_number'] = pd.to_numeric(all_props_df['over_number'])
    all_props_df['under_number'] = pd.to_numeric(all_props_df['under_number'])
    all_props_df['outcomes[0]_oddsAmerican'] = pd.to_numeric(all_props_df['outcomes[0]_oddsAmerican'])
    all_props_df['outcomes[1]_oddsAmerican'] = pd.to_numeric(all_props_df['outcomes[1]_oddsAmerican'])
    all_props_df.rename({
        'outcomes[0]_oddsAmerican': 'over_odds',
        'outcomes[1]_oddsAmerican': 'under_odds'
    }, axis=1, inplace=True
    )

    mean_methods = {
        'passing-yards': 'normal',
        'passing-tds': 'poisson',
        'rushing-yards': 'normal',
        'rushing-tds': 'poisson',
        'receiving-yards': 'normal',
        'receiving-tds': 'poisson',
        'receptions': 'normal',
        'qb-ints': 'poisson'
    }

    normal_std = 0.25

    means = []
    for i,row in all_props_df.iterrows():
        method = mean_methods[row['subcategory']]
        if method == 'normal':
            mean = find_normal_mean(n=row['over_number'], over_odds=row['over_odds'], under_odds=row['under_odds'], sigma=row['over_number']*normal_std)
        elif method == 'poisson':
            mean = poisson_mean_from_market(X=row['over_number'], over_american=row['over_odds'], under_american=row['under_odds'])
        else:
            mean = 0
        means.append(mean)

    all_props_df['mean'] = means

    all_props_df = all_props_df[['name', 'subcategory', 'mean']]

    all_props_df.to_csv('./data/draftkings/all_props.csv', index=False)

    # Pivot the DataFrame to have subcategories as columns
    pivot_df = all_props_df.pivot(index='name', columns='subcategory', values='mean').reset_index()

    # Save the pivoted DataFrame to a CSV file
    pivot_df.to_csv('./data/draftkings/pivoted_all_props.csv', index=False)

    ppr_scoring = {
        'passing-yards': 0.04,
        'passing-tds': 4,
        'rushing-yards': 0.1,
        'rushing-tds': 6,
        'receiving-yards': 0.1,
        'receiving-tds': 6,
        'receptions': 1,
        'qb-ints': -1
    }

    half_ppr_scoring = {
        'passing-yards': 0.04,
        'passing-tds': 4,
        'rushing-yards': 0.1,
        'rushing-tds': 6,
        'receiving-yards': 0.1,
        'receiving-tds': 6,
        'receptions': 0.5,
        'qb-ints': -1
    }

    standard_scoring = {
        'passing-yards': 0.04,
        'passing-tds': 4,
        'rushing-yards': 0.1,
        'rushing-tds': 6,
        'receiving-yards': 0.1,
        'receiving-tds': 6,
        'receptions': 0,
        'qb-ints': -1
    }

    ppr_scoring_df = calculate_weighted_score(all_props_df, ppr_scoring)
    ppr_scoring_df.to_csv('./data/draftkings/ppr_scoring.csv', index=False)
    ppr_aggregated = aggregate_fantasy_points(ppr_scoring_df, ppr_scoring)
    ppr_aggregated.to_csv('./data/draftkings/ppr_aggregated.csv', index=False)

    half_ppr_scoring_df = calculate_weighted_score(all_props_df, half_ppr_scoring)
    half_ppr_scoring_df.to_csv('./data/draftkings/half_ppr_scoring.csv', index=False)
    half_ppr_aggregated = aggregate_fantasy_points(ppr_scoring_df, half_ppr_scoring)
    half_ppr_aggregated.to_csv('./data/draftkings/half_ppr_aggregated.csv', index=False)

