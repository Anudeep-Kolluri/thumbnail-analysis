# ============CONSTANTS===========



# ============CODE================
import re
from datetime import datetime
from typing import List
from dagster import asset, AssetIn


@asset(
        ins={"metadata": AssetIn("scrape_metadata")},
        kinds={'python'}
)
def clean_metadata(metadata: List) -> List:
    '''
    Converting text into numbers
    '''

    columns = ['subscriber_count', 'video_count', 'view_count']

    for record in metadata:

        for column in columns:
            value = record[column].split(" ")[0].replace(',', '')
            if 'K' in value:
                value = int(float(value.replace('K', '')) * 1_000)
            elif 'M' in value:
                value = int(float(value.replace('M', '')) * 1_000_000)
            
            record[column] = value
        
        record['channel_tag'] = record['channel_tag'].lower()
        record['joined_date'] = datetime.strptime(record['joined_date'].replace('Joined ', ''), '%b %d, %Y').strftime('%Y-%m-%d')

    print(metadata)

    return metadata
