# ============CONSTANTS===========

LOGS_PATH = "my_dagster_project/logs/log.txt"

# ============CODE================
import requests, datetime

from dagster import asset, AssetIn

@asset(
    ins={"channel_tags": AssetIn("load_channels")},
    kinds={'python'}
)
def check_channels(channel_tags):
    '''
    Check if all the channels from yaml are working or not
    '''

    valid_channels = []

    for channel_tag in channel_tags:
        URL = f"https://www.youtube.com/@{channel_tag}/videos"
        response = requests.get(URL)
        status_code = response.status_code


        
        print(f'{channel_tag} : {response.status_code}')

        if status_code != 200:
            with open(LOGS_PATH, 'a') as f:
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                text = f'{current_time} || {channel_tag} : {status_code} \n'
                f.write(text)
        else:
            valid_channels.append(channel_tag)

    # print(valid_channels)
    
    return valid_channels


