# ============CONSTANTS===========

WAIT_TIME = 1

# ============CODE================
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options
import time
from bs4 import BeautifulSoup
from typing import List

from dagster import asset, AssetIn

@asset(
        ins={"channel_tags" : AssetIn("check_channels")},
        kinds={'python'}
)
def scrape_metadata(channel_tags: List) -> List:
    '''
    Scrapes channel_tag, channel_name, verified status, subscribers and more
    '''

    options = Options()
    options.binary_location = "/usr/bin/firefox"
    options.add_argument("--headless")  # Run in headless mode
    options.log.level = "trace"  # Enable verbose logging


    service = Service(GeckoDriverManager().install())

    print("Starting Firefox...")
    driver = webdriver.Firefox(service=service, options=options)

    metadata = []

    for channel_tag in channel_tags:
        URL = f"https://www.youtube.com/@{channel_tag}/videos"

        print(f"Entered website {channel_tag}")
        driver.get(URL)
        time.sleep(WAIT_TIME)


        print("Scraping Website")
        html = driver.page_source


        data = {}

        soup = BeautifulSoup(html, 'html.parser')
        element = soup.find(class_='dynamic-text-view-model-wiz__h1')

        if element:
            channel_name = element.get_text(strip=True)
            is_verified = "verified" in element.get("aria-label", "").lower()  # Check if 'verified' is in aria-label

            data['channel_tag'] = channel_tag.lower()
            data['channel_name'] = channel_name
            data['is_verified'] = is_verified

            # Click the "more" button to load additional channel info
            try:
                # Click the button using CSS selector
                more_button = driver.find_element("class name", "truncated-text-wiz__absolute-button")
                # print(more_button)
                more_button.click()

                # print("Clicked the '...more' button.")

                # Wait for additional info to load (adjust as necessary)
                time.sleep(WAIT_TIME)

                # Scrape the updated page content
                updated_html = driver.page_source
                updated_soup = BeautifulSoup(updated_html, 'html.parser')

                # You can extract additional information here if needed
                # For example, extracting the description after clicking

                td_elements = updated_soup.find_all('table')[1].find_all('td')
                
                data_mapping = {
                    '@': 'youtube_link',
                    'subscribers': 'subscriber_count',
                    'videos': 'video_count',
                    'views': 'view_count',
                    'Joined': 'joined_date'
                }

                for td in td_elements:
                    if te := td.get_text(strip=True):
                        for keyword, key in data_mapping.items():
                            if keyword in te:
                                data[key] = te
                                break

                metadata.append(data)

            except Exception as e:
                print(f"{channel_tag} || Error clicking the button: {e}")
            


        else:
            print("Element not found")

    print("Closing firefox")
    driver.quit()

    return metadata