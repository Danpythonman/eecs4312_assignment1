from google_play_scraper import app, reviews_all
import pickle
import os
from pathlib import Path


# Path of this file
UTILS_PATH = Path(__file__)

# Path of the directory that this file is in
UTILS_DIRECTORY_PATH = UTILS_PATH.parent

# Path of notebook directory
NOTEBOOK_DIRECTORY_PATH = UTILS_DIRECTORY_PATH.parent

# Path of data directory
DATA_DIRECTORY_PATH = NOTEBOOK_DIRECTORY_PATH.parent / 'data'

# Ensure data path exists
DATA_DIRECTORY_PATH.mkdir(exist_ok=True)


def CACHE_FILE(app_id):
    """
    Get the path of the pickle file for the reviews of the given `app_id`.
    """
    return DATA_DIRECTORY_PATH / f'data_cache-{app_id}.pkl'


def get_app_reviews(app_id):
    """
    Fetch reviews for an app.
    """
    return reviews_all(app_id, lang='en', country='us', sleep_milliseconds=0)


def get_google_play_data(app_id, flush=False):
    """
    Fetches reviews from the Google Play Store for the given `app_id`, caching
    them once retrieved.

    If the reviews for the given `app_id` are already cached, they will be
    retrieved from the cache instead of fetching them from the Google Play
    Store.

    If `flush` is set to `True`, then the reviews will be fetched from the
    Google play store and cached, regardless of whether or not it was previously
    in the cache. (If it was previously in the cache, it will be replaced.)
    """
    if not flush:
        if os.path.exists(CACHE_FILE(app_id)):
            # If the cache is not being flushed and the cache file exists, then
            # get the reviews from the cache
            with open(CACHE_FILE(app_id), 'rb') as f:
                return pickle.load(f)

    print('CACHE MISS, FETCHING FROM GOOGLE PLAY')

    # If the cache is being flushed, or the cache file does not exist, fetch the
    # reviews from the Google Play Store
    data = get_app_reviews(app_id)

    # Save reviews to the cache file
    with open(CACHE_FILE(app_id), 'wb') as f:
        pickle.dump(data, f)
        print('Data fetched from the web and cached.')

    return data
