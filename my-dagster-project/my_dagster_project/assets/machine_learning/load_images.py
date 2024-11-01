from dagster import asset


@asset(
        kinds={'python', 'duckdb'}
)
def load_images():
    '''
    Loads thumbnail links from duckdb and then downloads image from internet
    and corresponding target values
    '''
    return 0