import duckdb
import pandas as pd
import yaml
import nfl_data_py as nfl

def ingest_table_to_duckdb(db_path, table_name, csv_file_path):
    # Connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)
    
    # Read the player_weekly CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Write the DataFrame to the DuckDB database in a table called 'fact_player_weekly'
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    # Load the configuration file
    with open('configs/dev_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    table_name = 'player_ids'

    df = nfl.import_ids()
    print(df.head())
    df.to_csv(f'data/nfldatapy/{table_name}.csv', index=False)
    
    # Extract the db_path from the configuration
    db_path = config['duckdb']['db_path']
    db_name = config['duckdb']['db_name']
    db_path_full = f"{db_path}/{db_name}"
    csv_file_path = f'data/nfldatapy/{table_name}.csv'
    ingest_table_to_duckdb(db_path_full, table_name, csv_file_path)
