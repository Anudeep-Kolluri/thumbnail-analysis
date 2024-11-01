# ============CONSTANTS===========



# ============CODE================
from typing import List
from dagster import asset, AssetIn


@asset(
        ins={"weights": AssetIn("train_model")},
        kinds={'python'}
)
def save_model(weights):
    '''
    save model weights
    '''
    return 0