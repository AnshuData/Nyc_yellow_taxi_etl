import yaml
import os

def load_config(config_file):
    """
    Load a YAML configuration file and return it as a dictionary.

    Parameters:
    - config_file (str): The path to the YAML configuration file.

    Returns:
    - config_dict (dict): The configuration settings loaded from the YAML file.

    Raises:
    - FileNotFoundError: If the specified configuration file is not found.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file '{config_file}' not found")

    with open(config_file, 'r') as file:
        config_dict = yaml.safe_load(file)
    return config_dict

