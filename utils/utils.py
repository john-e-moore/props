import yaml
import hashlib
from datetime import datetime

################################################################################
# Configuration
################################################################################
def load_config(env):
    """
    Loads configuration parameters for the specified environment.
    """
    with open(f'configs/{env}_config.yaml', 'r') as file:
        return yaml.safe_load(file)
    
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