# ============CONSTANTS===========

PATH = "channel_tags.yml"

# ============CODE================
import yaml
from typing import List
from dagster import asset

@asset(kinds={'python'})
def load_channels() -> List:
    """
    Loads all channel names from channel.yml
    """
    with open(PATH, "r") as file:
        data = yaml.safe_load(file)
    
    return data['channels']