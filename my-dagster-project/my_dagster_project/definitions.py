from .assets import \
    load_channels, \
    check_channels, \
    scrape_thumbnails, \
    scrape_metadata, \
    save_metadata, \
    clean_metadata, \
    save_thumbnails

from .resources import duckdb_resource

from dagster import Definitions, load_assets_from_modules


all_assets = load_assets_from_modules([load_channels, \
                                        check_channels, \
                                        scrape_thumbnails, \
                                        scrape_metadata, \
                                        save_metadata, \
                                        clean_metadata, \
                                        save_thumbnails  ])

defs = Definitions(
    assets=all_assets,
    resources={
        "ddb" : duckdb_resource
    }
)
