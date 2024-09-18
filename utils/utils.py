import yaml
import hashlib
import re
from datetime import datetime
from typing import Dict, Any, List, Optional

################################################################################
# Configuration
################################################################################
def load_config(env):
    """
    Loads configuration parameters for the specified environment.
    """
    with open(f'configs/{env}_config.yaml', 'r') as file:
        return yaml.safe_load(file)
    
def get_event_group_by_name(event_groups: List[dict], name: str) -> dict:
    """
    Parses a list of url parameters to get only the eventgroup (sport) we need.

    :return: A dictionary containing IDs and names of categories and subcategories
    within an eventgroup.
    """
    return next((event_group for event_group in event_groups if event_group['name'] == name), None)

    
################################################################################
# Cryptography
################################################################################
def compute_md5_hash(obj: bytes) -> str:
    """
    Computes the MD5 hash of an in-memory object.
    """
    md5 = hashlib.md5()
    md5.update(obj)
    return md5.hexdigest()

################################################################################
# Files
################################################################################
def generate_timestamp() -> str:
    """
    Generates the current timestamp in YYYYMMDDHHMMSS format.
    """
    return datetime.now().strftime("%Y%m%d%H%M%S")

def extract_timestamp_from_filename(filename: str) -> str:
    """
    Extracts a 14-digit number from the given input string.

    Args:
        filename (str): The string to extract the number from.

    Returns:
        str: The extracted 14-digit number, or an empty string if no valid number is found.
    """
    # Use regex to find a sequence of 14 digits
    match = re.search(r'\d{14}', filename)
    
    if match:
        return match.group(0)  # Return the 14-digit number
    else:
        return ""  # Return an empty string if no match

################################################################################
# DraftKings response parsing
################################################################################
def parse_dk_offers(data: Dict[str, Any], timestamp: str) -> List[Dict[str, Any]]:
    """
    Parses the given data dictionary to extract and flatten information about offers and outcomes.

    Convert to dataframe with "offers_df = pd.DataFrame(offers_data)"

    Args:
        data (Dict[str, Any]): The input dictionary containing offers and outcomes data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries where each item corresponds to one outcome, 
                              containing fields related to the offer, outcome, subcategory ID, and name.
    """
    offers_data = []
    for category in data.get("eventGroup", {}).get("offerCategories", []):
        if "offerSubcategoryDescriptors" in category:
            for subcategory in category["offerSubcategoryDescriptors"]:
                if "offerSubcategory" in subcategory and "offers" in subcategory["offerSubcategory"]:
                    for offer_list in subcategory["offerSubcategory"]["offers"]:  # offers is a list of lists
                        for offer in offer_list:
                            for outcome in offer["outcomes"]:
                                participants = outcome.get("participants", [])
                                for participant in participants:
                                    # Flatten and capture the relevant fields for each outcome, including subcategory info
                                    offer_outcome = {
                                        "subcategory_subcategoryId": subcategory.get("subcategoryId", None),
                                        "subcategory_name": subcategory.get("name", ""),
                                        "offer_label": offer.get("label", ""),
                                        "offer_providerOfferId": offer.get("providerOfferId", ""),
                                        "offer_eventId": offer.get("eventId", ""),
                                        "offer_eventGroupId": offer.get("eventGroupId", ""),
                                        "offer_playerNameIdentifier": offer.get("playerNameIdentifier", ""),
                                        "outcome_label": outcome.get("label", ""),
                                        "outcome_oddsAmerican": outcome.get("oddsAmerican", ""),
                                        "outcome_oddsDecimal": outcome.get("oddsDecimal", None),
                                        "outcome_line": outcome.get("line", None),  # Handle missing 'line'
                                        "participant_id": participant.get("id", None),
                                        "participant_name": participant.get("name", ""),
                                        "participant_type": participant.get("type", ""),
                                        "timestamp": timestamp
                                    }
                                    offers_data.append(offer_outcome)
    return offers_data