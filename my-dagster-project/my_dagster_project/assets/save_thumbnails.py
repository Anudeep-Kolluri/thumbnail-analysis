# ============CONSTANTS===========



# ============CODE================
from typing import List
from dagster import asset, AssetIn
from dagster_duckdb import DuckDBResource

@asset(
        ins={"metadata": AssetIn("scrape_thumbnails")},
        kinds={'python', 'duckdb'}
)
def save_thumbnails(ddb:DuckDBResource, metadata: List) -> None:
    '''
    Save to duckdb database
    '''

    with ddb.get_connection() as conn:
        query = """ INSERT INTO metadata 
                    (channel_tag, channel_name, is_verified, youtube_link, subscriber_count, video_count, view_count, joined_date) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

        for record in metadata:
            conn.execute(query, (
                record['channel_tag'],
                record['channel_name'],
                record['is_verified'],
                record['youtube_link'],
                record['subscriber_count'],
                record['video_count'],
                record['view_count'],
                record['joined_date']
            ))
