import duckdb
import pandas as pd
from utils.stats_utils import calculate_vig_free_odds_and_vig, poisson_mean_from_market, gamma_mean_from_market

def execute_query_and_calculate_props(db_path, sql_file_path):
    # Connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)
    
    # Read the SQL query from the file
    with open(sql_file_path, 'r') as file:
        query = file.read()
    
    # Execute the query and fetch the results into a DataFrame
    df = conn.execute(query).fetchdf()
    
    df['p_over_vig_free'] = df.apply(lambda row: calculate_vig_free_odds_and_vig(row['over_odds'], 'over', row['under_odds'])[0], axis=1)
    df['p_under_vig_free'] = df.apply(lambda row: calculate_vig_free_odds_and_vig(row['under_odds'], 'over', row['over_odds'])[0], axis=1)

    poisson_categories = ['Receptions O/U', 'TD Scorer', 'Interceptions Thrown O/U']
    gamma_categories = ['Rushing Yards O/U', 'Passing Yards O/U', 'Rushing TDs O/U', 'Passing TDs O/U', 'Rush + Rec Yards O/U']
    df['subcategory_type'] = df['subcategory_name'].apply(lambda x: 'poisson' if x in poisson_categories else 'gamma')
    def calculate_mean_outcome(row):
        if row['subcategory_type'] == 'poisson':
            return poisson_mean_from_market(row['outcome_line'], row['over_odds'], row['under_odds'])
        elif row['subcategory_type'] == 'gamma':
            # TODO: Number series should be series of outcomes from similar players (receivers, or short adot receivers, etc.)
            # Could also use linear regression to predict gamma distribution based on player history
            #return gamma_mean_from_market(row['outcome_line'], row['over_odds'], row['under_odds'], row['numeric_series'])
            return gamma_mean_from_market(row['outcome_line'], row['over_odds'], row['under_odds'], [1,2])
        else:
            return None

    df['mean_outcome'] = df.apply(calculate_mean_outcome, axis=1)

    fpts_per = {
        'TD Scorer': 6,
        'Receptions O/U': 1,
        'Rush Yards O/U': 0.1,
        'Rec Yards O/U': 0.1,
        'Rush + Rec Yards O/U': 0,
        'Pass Yards O/U': 0.025,
        'Pass TDs O/U': 4,
        'Interceptions O/U': -2,
        'PAT Made': 1,
        'FG Made': 3
    }
    
    df['fpts_per'] = df['subcategory_name'].apply(lambda x: fpts_per[x])

    # Close the connection
    conn.close()
    
    return df

if __name__ == "__main__":
    db_path = '/mnt/c/Users/John/Documents/Personal/props/dev_warehouse.duckdb'
    sql_file_path = 'transformations/sql/select_raw_props.sql'
    result_df = execute_query_and_calculate_props(db_path, sql_file_path)[[
        'participant_name', 'subcategory_name', 'outcome_line',
        'over_odds', 'under_odds', 'subcategory_type', 'mean_outcome', 'fpts_per']]
    result_df.to_csv('props_output.csv', index=False)
    print(result_df[result_df['subcategory_type'] == 'poisson'].head(20))