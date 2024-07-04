from prefect import Flow, task
import duckdb
import pandas as pd

# Configuration
import yaml

def load_config(env):
    with open(f'configs/{env}_config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config('dev')  # Change to 'prod' in production

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
def transform_data(df):
    # Example data transformation logic
    df['value'] = df['value'] * 1.1  # Adjust values by some factor
    return df

@task
def load_data(df):
    # Example data load logic
    conn = duckdb.connect(config['duckdb']['db_path'])
    conn.execute("CREATE TABLE IF NOT EXISTS fantasy_output (id INTEGER, value FLOAT)")
    conn.execute("INSERT INTO fantasy_output SELECT * FROM df")

# Flow definition
with Flow("Fantasy Output Pipeline") as flow:
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)

# Running the flow
if __name__ == "__main__":
    flow.run()
