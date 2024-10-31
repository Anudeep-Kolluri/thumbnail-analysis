# ============CONSTANTS===========

WAIT_TIME = 2

# ============CODE================
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options
import time, csv
from bs4 import BeautifulSoup
from typing import List
from dagster import asset, AssetIn


@asset(
        ins={"channel_tags" : AssetIn("check_channels")},
        kinds={'python'}
)
def scrape_thumbnails(channel_tags) -> List:
    """
    Scrape title, views, time, link of thumbnail
    """

    # Set up the options for Firefox
    options = Options()
    options.binary_location = "/usr/bin/firefox"
    options.add_argument("--headless")  # Run in headless mode
    options.log.level = "trace"  # Enable verbose logging

    # Set up the Firefox WebDriver with the Service class
    service = Service(GeckoDriverManager().install())
    print("Starting Firefox...")
    driver = webdriver.Firefox(service=service, options=options)


    aria_labels = []


    # Navigate to the YouTube videos page
    for channel_tag in channel_tags:
        URL = f"https://www.youtube.com/@{channel_tag}/videos"

        print(f"Entering website {channel_tag}")
        driver.get(URL)
        time.sleep(WAIT_TIME)

        # Scroll down until the bottom of the page is reached
        print("Scrolling")
        last_height = driver.execute_script("return document.documentElement.scrollHeight")
        while True:
            # Scroll to the bottom
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            
            # Wait for new content to load
            time.sleep(WAIT_TIME)
            
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                # If the height hasn't changed, we've reached the bottom
                print("Reached the end")
                break
            last_height = new_height

        # Collect the page's HTML
        page_source = driver.page_source

        soup = BeautifulSoup(page_source, 'html.parser')
        contents_div = soup.find('div', id='contents')
        thumbnails = soup.find_all('div', id='thumbnail')

        timers = set(['year', 'month', 'week', 'day', 'minute', 'hour', 'second', 'years', 'months', 'weeks', 'days', 'hours', 'minutes', 'seconds', 'minutes,', 'minute,', 'hour', 'hour,'])

        for tag, href in zip(contents_div.find_all('a', attrs={'aria-label': True}), thumbnails):

            data = [channel_tag, None, None, None, None]
            labels = tag['aria-label'].split(" ")

            checks = set(labels[-7:])
            counts = len(checks.intersection(timers))
            # print(labels[-7:], checks.intersection(timers))

            if counts == 3:
                data[1] = ' '.join(labels[:-9])
                data[2] = labels[-9].replace(",", "")
                data[3] = ' '.join(labels[-7:])
            else:
                data[1] = ' '.join(labels[:-7])
                data[2] = labels[-7].replace(",", "")
                data[3] = ' '.join(labels[-5:])
            
            uid = href.find('a', id='thumbnail')['href'].split('=')[-1]
            data[4] = f"https://i.ytimg.com/vi/{uid}/hqdefault.jpg"

            # print(data)
            aria_labels.append(data)
        print(len(aria_labels), len(thumbnails))
            
    # Close the driver
    driver.quit()

    return aria_labels   #aria_labels is thumbnail_data