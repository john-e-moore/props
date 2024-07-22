import yaml

def load_config(env):
    with open(f'configs/{env}_config.yaml', 'r') as file:
        return yaml.safe_load(file)