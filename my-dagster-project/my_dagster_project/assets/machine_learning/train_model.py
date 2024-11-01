# ============CONSTANTS===========



# ============CODE================
from typing import List
from dagster import asset, AssetIn


@asset(
        ins={"images": AssetIn("load_images")},
        kinds={'python', 'tensorflow'}
)
def train_model(images):
    '''
    Extract latest YOLO model and train images on it
    '''
    return 0