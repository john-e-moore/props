# Standard
import os
# External
import yaml
import duckdb
import pandas as pd
from prefect import flow, task
# Internal
from utils.s3_utils import S3Utility

################################################################################
# Configuration
################################################################################
def load_config(env):
    with open(f'configs/{env}_config.yaml', 'r') as file:
        return yaml.safe_load(file)

environment = os.getenv('PROPS_ENVIRONMENT')
#base_config = load_config('base')
environment_config = load_config(environment)

# S3
s3_bucket = environment_config['aws']['s3_bucket']
s3_key = environment_config['aws']['s3_key']

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
def extract_data():
    # Example data extraction logic
    data = {
        "id": [1, 2, 3],
        "value": [100, 200, 300]
    }
    df = pd.DataFrame(data)
    return df

@task
def upload_raw_to_s3(df):
    print(f"Raw data preview:\n{df.head()}")
    s3 = S3Utility()
    s3.upload_obj_s3(
        bucket=s3_bucket,
        key=f'{s3_key}/raw/example_data.json',
        obj=df.to_json()
    )
    return None

@task
def process_raw_data(df):
    # Example data transformation logic
    df['value'] = df['value'] * 1.1  # Adjust values by some factor
    return df    

@task
def upload_processed_to_s3(df):
    s3 = S3Utility()
    s3.upload_obj_s3(
        bucket=s3_bucket,
        key=f'{s3_key}/processed/example_data.json',
        obj=df.to_json()
    )
    return None

@task
def load_processed_to_duckdb(df):
    # Example data load logic
    with duckdb.connect(database=f'{db_path}/{db_name}') as con:
        con.execute("CREATE TABLE IF NOT EXISTS fact_example (id INTEGER, value FLOAT)")
        con.register('example_df', df)
        con.execute("INSERT INTO fact_example SELECT * FROM example_df")
    return None

@task
def query_sum(table, column_name):
    # NOTE: fetch methods: fetchdf(), fetchall(), fetchone(), fetchnumpy(), fetch_df_chunk()
    with duckdb.connect(database=f'{db_path}/{db_name}') as con:
        con = duckdb.connect(database=f'{db_path}/{db_name}')
        sum = con.execute(f"SELECT SUM({column_name}) AS sum_values FROM {table};").fetchone()
    return sum

################################################################################
# Flow
################################################################################
@flow
def example_flow(log_prints=log_prints, retries=retries, retry_delay_seconds=retry_delay_seconds):
    """
    Generates example data, processes it, and loads it to duckdb. Stages intermediate results in S3.
    """
    print(f"duckdb location: {db_path}/{db_name}")

    # "Extract" raw data
    df_raw = extract_data()

    # Stage raw in S3
    upload_raw_to_s3(df_raw)

    # Process data
    df_processed = process_raw_data(df_raw)

    # Stage processed in S3
    upload_processed_to_s3(df_processed)

    # Load processed to duckdb
    load_processed_to_duckdb(df_processed)

    # Calculate and print sum of new values
    sum = query_sum('fact_example', 'value')
    print(f"Sum of newly calculated values: {sum}")

# Running the flow
if __name__ == "__main__":
    example_flow.serve(
        name='example-flow',
        cron='* * * * *',
        tags=['example'],
        # default description is flow's docstring
        version='0.1',
    )
