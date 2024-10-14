import duckdb
import pandas as pd
import yaml

def ingest_player_weekly_to_duckdb(db_path, csv_file_path):
    # Connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)
    
    # Read the player_weekly CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Write the DataFrame to the DuckDB database in a table called 'fact_player_weekly'
    conn.execute("CREATE TABLE IF NOT EXISTS fact_player_weekly AS SELECT * FROM df")
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    # Load the configuration file
    with open('configs/dev_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    # Extract the db_path from the configuration
    db_path = config['duckdb']['db_path']
    db_name = config['duckdb']['db_name']
    db_path_full = f"{db_path}/{db_name}"
    csv_file_path = 'data/nfldatapy/player_weekly_2013-2023.csv'
    ingest_player_weekly_to_duckdb(db_path_full, csv_file_path)
