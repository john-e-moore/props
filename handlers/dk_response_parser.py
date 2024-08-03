import pandas as pd
import json
from typing import Dict, Any, List, Set, Optional

class DKResponseParser:
    def __init__(self, json_obj: Dict[str, Any]):
        """
        Initializes the DKResponseParser which handles responses from DraftKings.
        
        :param response: Response returned from DraftKings API.
        """
        self.json_obj = json_obj

    def parse_and_clean_game_lines():
        pass

    def parse_and_clean_player_props():
        pass
    
    @staticmethod
    def flatten_item(item: Any, exclude: Optional[Set[str]] = None) -> Dict[str, Any]:
        """
        Recursively flattens the specified item. Nested structures may include
        dictionaries, lists, or a list of dictionaries.

        :param item: Dictionary or list containing dictionaries to flatten.
        :param exclude: Fields to leave intact. Meant to be used for fields that can
        contain a list of arbitrary length, which would break normalization.
        :return: Flattened (single layer) dictionary.
        """
        if exclude is None:
            exclude = set()
        
        def flatten(current_item: Any, key_prefix: str = '') -> Dict[str, Any]:
            flat_dict = {}
            if isinstance(current_item, dict):
                for k, v in current_item.items():
                    full_key = f"{key_prefix}.{k}" if key_prefix else k
                    if k in exclude:
                        flat_dict[full_key] = v
                    else:
                        flat_dict.update(flatten(v, full_key))
            elif isinstance(current_item, list):
                for index, elem in enumerate(current_item):
                    full_key = f"{key_prefix}[{index}]"
                    flat_dict.update(flatten(elem, full_key))
            else:
                flat_dict[key_prefix] = current_item
            return flat_dict
        
        return flatten(item)
    
    @staticmethod
    def find_nested_value(d: Dict[Any, Any], key: str, found=None) -> Any:
        """
        Recursively searches through nested dictionary until locating the specified key,
        then extracts the value of that key. If the key appears multiple times at any level,
        an error is thrown.

        Note: must pass 'd' instead of using self.response because of recursion.

        :params d: The dictionary to search.
        :params key: The key to locate.
        :return: The value corresponding to the specified key, in this case a list of 
        offers or events.
        :raises ValueError: If the key is found more than once in the dictionary structure.
        """

        if found is None:
            found = []

        if key in d:
            if key in found:
                raise ValueError(f"Key '{key}' found more than once.")
            found.append(key)
            return d[key]

        for value in d.values():
            if isinstance(value, dict):
                result = DKResponseParser.find_nested_value(value, key, found)
                if result is not None:
                    return result
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        result = DKResponseParser.find_nested_value(item, key, found)
                        if result is not None:
                            return result
        if not found:
            return None
    
    @staticmethod
    def flattened_events_to_dataframe(d: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts a dictionary to a Pandas Dataframe.

        :param d: dictionary to convert
        :return: DataFrame
        """
        rows = {}

        for key, value in d.items():
            # Extract index and field name from the key
            index = int(key.split('].')[0][1:])
            field_name = key.split('].')[1]
            
            # Initialize the row if it hasn't been started yet
            if index not in rows:
                rows[index] = {}
            
            # Add data to the correct dictionary based on the index
            rows[index][field_name] = value

        rows_list = list(rows.values())

        return pd.DataFrame(rows_list)
    
    @staticmethod
    def flattened_offers_to_dataframe(d: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts a dictionary to a Pandas Dataframe.

        :param d: dictionary to convert
        :return: DataFrame
        """
        # Initialize a dictionary to hold the processed rows
        rows = {}

        # Parse the flattened data
        for key, value in d.items():
            # Correct parsing of the key to extract indices and property names
            # Example key format: "[0][0].outcomes[0].label"
            parts = key.replace(']', '').split('[')
            main_index = int(parts[1])  # First index for DataFrame row
            # Initialize the row if it hasn't been started yet
            if main_index not in rows:
                rows[main_index] = {}

            # Iterate over parts to handle multiple nested levels
            for part in parts[1:]:
                # Handle outcome indices and properties, assuming format 'outcomes[0].label'
                if 'outcomes' in part:
                    try:
                        outcome_idx = part.split('[')[1].split(']')[0]
                    except IndexError:
                        outcome_idx = 0
                    property_name = part.split('.')[-1]
                    column_name = f"outcomes[{outcome_idx}].{property_name}"
                    rows[main_index][column_name] = value
                else:
                    # Direct properties without further nesting
                    property_name = part
                    if '.' in property_name:
                        property_name = property_name.split('.')[1]  # Strip off any leading indices if still attached
                    rows[main_index][property_name] = value

        # Convert the dictionary of rows into a list of dictionaries for DataFrame creation
        rows_list = [rows[key] for key in sorted(rows.keys())]

        for i,row in enumerate(rows_list):
            print(f"({i}) {row}")

        # Create a DataFrame from the list of dictionaries
        return pd.DataFrame(rows_list)

  
    @staticmethod
    def flattened_props_offers_to_dataframe(d: Dict[str, Any]) -> pd.DataFrame:
        # Initialize a dictionary to hold the processed rows
        rows = {}

        """
        [5][0].isSubcategoryFeatured": false,
        "[5][0].betOfferTypeId": 0,
        "[5][0].providerCriterionId": "5207",
        "[5][0].outcomes[0].providerOutcomeId": "0QA195641493#1789967200_13L88808Q11619917Q2-1",
        "[5][0].outcomes[0].providerId": 2,
        "[5][0].outcomes[0].providerOfferId": "195641493",
        "[5][0].outcomes[0].label": "Over 3600.5",

        ["[5", "[0", ".outcomes[0", ".label"]
        """

        # Parse the flattened data
        for key, value in d.items():
            # Split the key at '].' to handle nested properties correctly
            parts = key.split(']')
            main_index = int(parts[0][1:])  # Extract the main index, assuming format '[0]'
            
            # Initialize the row if it hasn't been started yet
            if main_index not in rows:
                rows[main_index] = {}

            if 'outcomes' in key:
                outcome_idx = parts[2][-1]
                property_name = parts[-1][1:]
                column_name = f'outcomes[{outcome_idx}]_{property_name}'
            else:
                column_name = parts[-1][1:]
            
            rows[main_index][column_name] = value

        # Convert the dictionary of rows into a list of dictionaries for DataFrame creation
        rows_list = [rows[key] for key in sorted(rows.keys())]

        # Create a DataFrame from the list of dictionaries
        return pd.DataFrame(rows_list)


    @staticmethod
    def split_series(series: pd.Series, delimiter: str, new_colnames: List[str]) -> pd.DataFrame:
        """
        Splits a series (column) by delimiter. 

        :param series: Series to split.
        :param delimiter: Delimiter to split by.
        :param colnames: List of new column names.
        :return: DataFrame containing the new series'.
        """
        try:
            split_data = series.str.split(delimiter, expand=True)
            split_data.columns = new_colnames
            return split_data
        except Exception as e:
            raise ValueError(f"Error processing series with provided parameters: {e}")




"""
1a. Extract events list
1b. Extract offers list
2. Extract dictionaries and flatten from each list
3. Join on eventId
"""

"""
################################################################################
# DK Games
################################################################################
# Load JSON data from file
with open('/mnt/data/dk_sample_nfl_full_20240730173759.json', 'r') as file:
    data = json.load(file)['eventGroup']

# Extract 'offerCategories' and 'events'
offer_categories = data['offerCategories']
events = data['events']

# Refine and flatten the data as per the detailed instructions and run the script

# Flatten offers
offers_list = []
for category in offer_categories:
    if 'offerSubcategoryDescriptors' in category:
        for subcategory in category['offerSubcategoryDescriptors']:
            if 'offerSubcategory' in subcategory and 'offers' in subcategory['offerSubcategory']:
                for offer in subcategory['offerSubcategory']['offers']:
                    for detail in offer:
                        for outcome in detail.pop('outcomes', []):
                            outcome_data = {**detail, **{f'outcome_{k}': v for k, v in outcome.items()}}
                            outcome_data['offerSubcategoryName'] = subcategory['offerSubcategory']['name']
                            outcome_data['offerCategoryId'] = category['offerCategoryId']
                            offers_list.append(outcome_data)

# Flatten events
events_list = []
for event in events:
    team1_data = {f'team1_{k}': v for k, v in event.pop('team1', {}).items()}
    team2_data = {f'team2_{k}': v for k, v in event.pop('team2', {}).items()}
    event_status_data = {f'eventStatus_{k}': v for k, v in event.pop('eventStatus', {}).items()}
    event_data = {**event, **team1_data, **team2_data, **event_status_data}
    events_list.append(event_data)

# Convert to DataFrame
offers_df = pd.DataFrame(offers_list)
events_df = pd.DataFrame(events_list)

# Merge offers and events on eventId
merged_df = pd.merge(offers_df, events_df, on='eventId', how='inner')

# Save to CSV
output_file = '/mnt/data/merged_offers_events_refined.csv'
merged_df.to_csv(output_file, index=False)
output_file


################################################################################
# Player props (will still need to hit each subcategory URL)
################################################################################
import json
import pandas as pd

# Load the JSON data from the file
with open("/mnt/data/dk_sample_nfl_player_stats_20240730181739.json", 'r') as file:
    data = json.load(file)['eventGroup']

# Initialize lists for offers and events
offers = []
events = []

# Extract offers and flatten outcomes
for category in data['offerCategories']:
    if 'offerSubcategoryDescriptors' in category:
        for subcategory in category['offerSubcategoryDescriptors']:
            if 'offerSubcategory' in subcategory and 'offers' in subcategory['offerSubcategory']:
                for offer_batch in subcategory['offerSubcategory']['offers']:
                    for offer in offer_batch:
                        for outcome in offer['outcomes']:
                            flattened_outcome = {
                                **{k: v for k, v in offer.items() if k != 'outcomes'},
                                **{f'outcome_{key}': value for key, value in outcome.items()},
                                'offerCategoryId': category['offerCategoryId'],
                                'subcategoryName': subcategory['name']
                            }
                            offers.append(flattened_outcome)

# Flatten event data with additional checks
for event in data['events']:
    event_flat = {
        'eventId': event['eventId'],
        'eventName': event['name'],
        'eventStartDate': event['startDate'],
        'team1_id': event['team1']['teamId'] if 'team1' in event and 'teamId' in event['team1'] else None,
        'team1_name': event['team1']['name'] if 'team1' in event and 'name' in event['team1'] else None,
        'team2_id': event['team2']['teamId'] if 'team2' in event and 'teamId' in event['team2'] else None,
        'team2_name': event['team2']['name'] if 'team2' in event and 'name' in event['team2'] else None,
        'eventStatus_state': event['eventStatus']['state'] if 'eventStatus' in event else None,
        'eventStatus_minute': event['eventStatus']['minute'] if 'eventStatus' in event else None,
        'eventStatus_second': event['eventStatus']['second'] if 'eventStatus' in event else None,
        'eventStatus_isClockRunning': event['eventStatus']['isClockRunning'] if 'eventStatus' in event else None
    }
    events.append(event_flat)

# Convert lists to DataFrames
df_offers = pd.DataFrame(offers)
df_events = pd.DataFrame(events)

# Merge on eventId
df_merged = pd.merge(df_offers, df_events, on='eventId', how='left')

# Save to CSV
output_csv_path = "/mnt/data/merged_nfl_player_stats_outcomes_flattened.csv"
df_merged.to_csv(output_csv_path, index=False)
"""
