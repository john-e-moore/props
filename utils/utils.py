import yaml
import hashlib
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