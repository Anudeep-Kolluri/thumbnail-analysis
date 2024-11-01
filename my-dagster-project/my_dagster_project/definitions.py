from .assets.web_scraping import \
    load_channels, \
    check_channels, \
    scrape_thumbnails, \
    scrape_metadata, \
    save_metadata, \
    clean_metadata, \
    save_thumbnails

from .assets.machine_learning import \
    load_images, \
    save_model, \
    train_model

from .resources import duckdb_resource

from dagster import Definitions, load_assets_from_modules


all_assets = load_assets_from_modules([load_channels, \
                                        check_channels, \
                                        scrape_thumbnails, \
                                        scrape_metadata, \
                                        save_metadata, \
                                        clean_metadata, \
                                        save_thumbnails, \
                                        load_images, \
                                        save_model, \
                                        train_model  ])

defs = Definitions(
    assets=all_assets,
    resources={
        "ddb" : duckdb_resource
    }
)
