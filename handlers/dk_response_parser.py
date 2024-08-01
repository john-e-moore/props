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
    
    def flatten_all_items(flattened_items: List[dict], key: str = None) -> pd.DataFrame:
        """
        
        """
        pass
    
    def flatten_dict(self, exclude: Optional[Set[str]] = set()) -> Dict[str, Any]:
        """
        Recursively flattens the specified JSON object. Nested structures may include
        dictionaries, lists, or a list of dictionaries.

        :param json_obj: Series to flatten. 
        :param exclude: Fields to leave intact. Meant to be used for fields that can
        contain a list of arbitrary length, which would break normalization.
        :return: Flattened dictionary.
        """
        items = {}
        for key, value in self.json_obj.items():
            if key in exclude:
                items[key] = value
            elif isinstance(value, dict):
                child_items = DKResponseParser.flatten_dict(value, exclude)
                for child_key, child_value in child_items.items():
                    new_key = f"{key}.{child_key}"
                    items[new_key] = child_value
            elif isinstance(value, list) and all(isinstance(i, dict) for i in value):
                for index, item in enumerate(value):
                    child_items = DKResponseParser.flatten_dict(item, exclude)
                    for child_key, child_value in child_items.items():
                        new_key = f"{key}[{index}].{child_key}"
                        items[new_key] = child_value
            else:
                items[key] = value
        return items
    
    @staticmethod
    def find_nested_value(d: Dict[Any, Any], key: str, found=None) -> Any:
        """
        Recursively searches through nested dictionary until locating the specified key,
        then extracts the value of that key. If the key appears multiple times at any level,
        an error is thrown.

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
    def dict_to_dataframe(d: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts a dictionary to a Pandas Dataframe.

        :param d: dictionary to convert
        :return: DataFrame
        """
        try:
            df = pd.DataFrame(d.items())
        except Exception as e:
            raise ValueError(f"Error converting dictionary to DataFrame: {e}")
        return df

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
