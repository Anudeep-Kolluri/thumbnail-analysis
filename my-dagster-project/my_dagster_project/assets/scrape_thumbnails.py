# ============CONSTANTS===========


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
    ins={"channel_tags": AssetIn("check_channels")},
    kinds={'python'}
)
def scrape_thumbnails(channel_tags):
    '''
    Scrape data from youtube
    '''