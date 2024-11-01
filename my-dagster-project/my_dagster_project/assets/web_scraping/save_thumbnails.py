# ============CONSTANTS===========



# ============CODE================
from typing import List
from dagster import asset, AssetIn
from dagster_duckdb import DuckDBResource

@asset(
        ins={"thumbnail_data": AssetIn("scrape_thumbnails")},
        kinds={'python', 'duckdb'}
)
def save_thumbnails(ddb:DuckDBResource, thumbnail_data: List) -> None:
    '''
    Save to duckdb database
    '''

    with ddb.get_connection() as conn:
        query = """ INSERT INTO thumbnails 
                    (channel_tag, video_title, view_count, time_updated, thumbnail_link) 
                    VALUES (?, ?, ?, ?, ?)"""

        for record in thumbnail_data:
            conn.execute(query, (
                record[0],       # Assuming this is the unique identifier for the video/channel
                record[1],       # Map this to the appropriate field from your data
                record[2],        # Number of views for the video
                record[3],      # Timestamp for when the data was updated
                record[4]     # Link to the thumbnail image
            ))