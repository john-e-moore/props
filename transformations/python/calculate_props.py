import duckdb
import json
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from collections import defaultdict
from scipy.stats import poisson, norm, expon, lognorm, gamma
from utils.stats_utils import calculate_vig_free_odds_and_vig, poisson_mean_from_market, gamma_mean_from_market, calculate_gamma_scale
from utils.stats_utils import gamma_over_100_prob, evaluate_normal_distribution, fit_normal_to_qb_data

def get_positions(
        conn,
        df: pd.DataFrame,
        player_name_map_path: str,
        players_table_name: str,
) -> dict:
    """
    Transform the names according to player name map json. Then look up list of positions
    from the players database, left join, and return a dict of player and positions.

    :params:
    :conn: Database connection.
    :df: DataFrame containing props offers.
    :player_name_map_path: Path to json file containing map from Draftkings player
    name to nfldatapy player name.
    :players_table_name: Name of the nfldatapy players duckdb table.

    :returns: Dict of player: position

    Usage: df[position] = get_positions(db_path, player_name_map, players_table_name)
    """
    # Load the player name map from the JSON file
    with open(player_name_map_path, 'r') as f:
        player_name_map = json.load(f)

    # Transform participant names in the DataFrame using the player name map
    df['transformed_name'] = df['participant_name'].map(player_name_map).fillna(df['participant_name'])

    # Query the players table to get display_name and position
    query = f"SELECT display_name, position FROM {players_table_name}"
    players_df = conn.execute(query).fetchdf()

    # Merge the DataFrame with the players DataFrame on the transformed name
    # NOTE: need to do this with ID's long-term, but for now I am going to try dropping
    # defensive positions and see if my merged result matches the original df length.
    merged_df = df.merge(players_df, left_on='transformed_name', right_on='display_name', how='left')
    # Filter merged_df to only keep rows with positions QB, WR, RB, TE
    valid_positions = ['QB', 'WR', 'RB', 'TE']
    merged_df = merged_df[merged_df['position'].isin(valid_positions)]

    # Return a dictionary of distinct participant names and their positions
    return merged_df.drop_duplicates(subset='participant_name').set_index('participant_name')['position'].to_dict()

def query_weekly_scores(db_path, table_name, position, stat_category) -> list:
    """
    :params:
        db_path: Path to database.
        table_name: Name of table to query.
        position: Player position to query.
        stat_category: Stat category to query.
    :returns:
        A list of weekly scores for the given position.
    """
    # Connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Execute the query and fetch the results into a DataFrame
    # TODO: may want to filter this further for a better scale.
    if (position == 'QB') and (stat_category == 'passing_yards'):
        query = f"""
        SELECT {stat_category} 
        FROM {table_name} 
        WHERE 
            position = '{position}' 
            AND {stat_category} > 0
            AND attempts > 10
    """
    else:
        query = f"""
            SELECT {stat_category} 
            FROM {table_name} 
            WHERE 
                position = '{position}' 
                AND {stat_category} > 0
        """
    df = conn.execute(query).fetchdf()
    weekly_scores = df[f'{stat_category}'].tolist()
    
    # Close the connection
    conn.close()

    return weekly_scores

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

    # Match players to their position
    position_dict = get_positions(
        conn=conn,
        df=df, 
        player_name_map_path='data/draftkings/player_name_matching/dk_to_nfldatapy.json',
        players_table_name='players'
    )
    # Create a new 'position' column for df based on the values in position_dict
    df['position'] = df['participant_name'].map(position_dict)
    print(df[['participant_name', 'position']].head())

    poisson_categories = [
        'Receptions', 'TD Scorer', 'Interceptions O/U', 
        'Rushing TDs O/U', 'Pass TDs O/U'
    ]
    gamma_categories = ['Rush Yards O/U', 'Pass Yards O/U', 'Rec Yards O/U']
    normal_categories = ['Pass Yards O/U']
    df = df[df['subcategory_name'].isin(poisson_categories + gamma_categories + normal_categories)]
    df['subcategory_type'] = df['subcategory_name'].apply(
        lambda x: 'poisson' if x in poisson_categories else ('normal' if x in normal_categories else 'gamma')
    )
    category_map = {
        'Rush Yards O/U': 'rushing_yards',
        'Rec Yards O/U': 'receiving_yards',
        'Pass Yards O/U': 'passing_yards'
    }
    # Store gamma scales for each position + stat category combination
    positions = ['QB', 'RB', 'WR', 'TE']
    gamma_scales = defaultdict(lambda: defaultdict(float))
    for position in positions:
        for category in gamma_categories:
            player_weekly_category = category_map[category]
            weekly_scores = query_weekly_scores(db_path, 'fact_player_weekly', position, player_weekly_category)
            shape, loc, scale = gamma.fit(weekly_scores)
            gamma_scales[position][player_weekly_category] = scale
        if position == 'QB':
            player_weekly_category = category_map['Pass Yards O/U']
            weekly_scores = query_weekly_scores(db_path, 'fact_player_weekly', position, player_weekly_category)
            mu, sigma = fit_normal_to_qb_data(weekly_scores)

    # TODO: save these to json
    def calculate_mean_outcome_and_bonus_prob(row):
        try:
            if row['subcategory_type'] == 'poisson':
                result = (poisson_mean_from_market(row['outcome_line'], row['over_odds'], row['under_odds']), 0)
            elif row['subcategory_type'] == 'gamma':
                position = row['position']
                stat_category = category_map[row['subcategory_name']]
                result = gamma_mean_from_market(row['outcome_line'], row['over_odds'], row['under_odds'], gamma_scales[position][stat_category])
            elif row['subcategory_type'] == 'normal':
                result = evaluate_normal_distribution(mu, sigma, row['outcome_line'], row['over_odds'], row['under_odds'])
            else:
                result = (0, 0)
            return result
        except Exception as e:
            print(f"Error processing row: {row.to_dict()}, Error: {e}")
            return (None, None)

    df[['mean_outcome', 'prob_bonus']] = df.apply(calculate_mean_outcome_and_bonus_prob, axis=1, result_type='expand')

    # Account for 20-25% juice on DK Anytime TD market (via Chris)
    #df.loc[df['subcategory_name'] == 'TD Scorer', 'mean_outcome'] *= (1 - 0.225)
    # New: inspecting a game, looks like closer to 11%
    # My RB stuff will be even higher than ETR but I think ETR is low.
    # Sabersim projections are more in line with mine in aggregate for RB
    df.loc[df['subcategory_name'] == 'TD Scorer', 'mean_outcome'] *= (1 - 0.11)

    # 
    fpts_per = {
        'TD Scorer': 6,
        'Receptions': 1,
        'Rush Yards O/U': 0.1,
        'Rec Yards O/U': 0.1,
        'Rush + Rec Yards O/U': 0,
        'Pass Yards O/U': 0.04,
        'Pass TDs O/U': 4,
        'Interceptions O/U': -1,
        'PAT Made': 1,
        'FG Made': 3,
    }
    
    df['fpts_per'] = df['subcategory_name'].apply(lambda x: fpts_per[x])
    df['fpts'] = (df['mean_outcome'] * df['fpts_per']) + (df['prob_bonus'] * 3)
    df['fpts'] = df['fpts'].round(1)

    # Close the connection
    conn.close()
    
    return df

if __name__ == "__main__":
    db_path = '/mnt/c/Users/John/Documents/Personal/props/dev_warehouse.duckdb'
    sql_file_path = 'transformations/sql/select_raw_props.sql'
    
    df = execute_query_and_calculate_props(db_path, sql_file_path)[[
        'participant_name', 'subcategory_name', 'outcome_line',
        'over_odds', 'under_odds', 'subcategory_type', 'mean_outcome', 
        'fpts_per', 'fpts', 'prob_bonus', 'position'
        ]]

    # Get the current timestamp in the format YYYYMMDDHHMMSS
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Save the raw results with a timestamp
    df.to_csv(f'data/draftkings/player_projections/props_output_{timestamp}.csv', index=False)
    print("Saved raw results.")

    # Pivot the dataframe to include both mean_outcome and prob_bonus
    pivot_df = df.pivot_table(
        index=['participant_name', 'position'],
        columns='subcategory_name',
        values=['mean_outcome', 'prob_bonus'],
        aggfunc='sum'
    ).reset_index()

    # Flatten the MultiIndex columns
    pivot_df.columns = ['_'.join(col).strip() if col[1] else col[0] for col in pivot_df.columns.values]

    # Append fpts
    fpts = df.groupby(['participant_name', 'position'])['fpts'].sum().reset_index()
    pivot_df = pivot_df.merge(fpts, on=['participant_name', 'position'], how='left')
    
    # Reorder columns to place 'fpts' as the third column
    cols = pivot_df.columns.tolist()
    fpts_index = cols.index('fpts')
    cols.insert(2, cols.pop(fpts_index))
    pivot_df = pivot_df[cols]

    # Append a column with all 'subcategory_name' values for each player
    subcategory_names = df.groupby(['participant_name', 'position'])['subcategory_name'].apply(lambda x: ', '.join(x.unique())).reset_index(name='subcategory_names')

    # Merge the subcategory names back into the pivoted dataframe
    pivot_df = pivot_df.merge(subcategory_names, on=['participant_name', 'position'])

    # Rounding
    pivot_df[[col for col in pivot_df.columns if 'mean_outcome' in col]] = pivot_df[[col for col in pivot_df.columns if 'mean_outcome' in col]].round(1)
    pivot_df[[col for col in pivot_df.columns if 'prob_bonus' in col]] = pivot_df[[col for col in pivot_df.columns if 'prob_bonus' in col]].round(2)

    # Remove ' O/U' and ' Scorer' from column names
    pivot_df.columns = [col.replace(' O/U', '').replace(' Scorer', '') for col in pivot_df.columns]

    # Save the pivoted dataframe to a new CSV with a timestamp
    pivot_df.to_csv(f'data/draftkings/player_projections/pivoted_props_output_{timestamp}.csv', index=False)
    print("Saved pivoted table.")

    print(pivot_df.dtypes)

    # Sabersim upload
    # Add is_complete column based on position
    def check_is_complete(row):
        if row['position'] == 'QB':
            required_columns = ['mean_outcome_Rush Yards', 'mean_outcome_Interceptions', 'mean_outcome_Pass TDs', 'mean_outcome_Pass Yards', 'mean_outcome_TD']
        elif row['position'] == 'RB':
            required_columns = ['mean_outcome_Receptions', 'mean_outcome_Rec Yards', 'mean_outcome_Rush Yards', 'mean_outcome_TD']
        elif row['position'] in ['WR', 'TE']:
            required_columns = ['mean_outcome_Receptions', 'mean_outcome_Rec Yards', 'mean_outcome_TD']
        else:
            return False
        return all(pd.notnull(row[col]) for col in required_columns)

    pivot_df['is_complete'] = pivot_df.apply(check_is_complete, axis=1)

    print(pivot_df.head(20))

    sabersim_df = pivot_df[['participant_name', 'fpts']][pivot_df['is_complete'] == True]
    sabersim_df.columns = ['Name', 'Projection']
    sabersim_df.to_csv(f'data/draftkings/player_projections/sabersim_upload_{timestamp}.csv', index=False)
    
    