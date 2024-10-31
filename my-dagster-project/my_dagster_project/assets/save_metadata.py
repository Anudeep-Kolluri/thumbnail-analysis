# ============CONSTANTS===========



# ============CODE================
from typing import List
from dagster import asset, AssetIn
from dagster_duckdb import DuckDBResource

@asset(
        ins={"thumbnail_data": AssetIn("clean_metadata")},
        kinds={'python', 'duckdb'}
)
def save_metadata(ddb:DuckDBResource, thumbnail_data: List) -> None:
    '''
    Save to duckdb database
    '''

    with ddb.get_connection() as conn:
        query = """ INSERT INTO thumbnails 
                    (channel_tag, video_title, view_count, time_updated, thumbnail_link) 
                    VALUES (?, ?, ?, ?, ?)"""

        for record in thumbnail_data:
            conn.execute(query, (
                record['channel_tag'],       # Assuming this is the unique identifier for the video/channel
                record['video_title'],       # Map this to the appropriate field from your data
                record['view_count'],        # Number of views for the video
                record['time_updated'],      # Timestamp for when the data was updated
                record['thumbnail_link']     # Link to the thumbnail image
            ))