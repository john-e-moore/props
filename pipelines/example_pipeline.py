from prefect import Flow, task
import duckdb
import pandas as pd
from utils.s3_utils import S3Utility
from jobs.extract.draftkings.get_dk_mlb_props import get_dk_mlb_props

# Configuration
import yaml

def load_config(env):
    with open(f'configs/{env}_config.yaml', 'r') as file:
        return yaml.safe_load(file)

base_config = load_config('base')
env_config = load_config('dev')

# Tasks
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
def upload_raw_to_s3():
    pass

@task
def process_raw_data(df):
    # Example data transformation logic
    df['value'] = df['value'] * 1.1  # Adjust values by some factor
    return df    

@task
def upload_processed_to_s3():
    pass

@task
def load_processed_to_duckdb(df):
    # Example data load logic
    conn = duckdb.connect(env_config['duckdb']['db_path'])
    conn.execute("CREATE TABLE IF NOT EXISTS fantasy_output (id INTEGER, value FLOAT)")
    conn.execute("INSERT INTO fantasy_output SELECT * FROM df")

# Flow definition
with Flow("Example Pipeline") as flow:
    data = extract_data()
    transformed_data = process_raw_data(data)
    load_processed_to_duckdb(transformed_data)

# Running the flow
if __name__ == "__main__":
    flow.run()
