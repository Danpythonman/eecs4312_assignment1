from google_play_scraper import app, reviews_all
import pickle
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import List


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


def CACHE_FILE(app_id: str) -> Path:
    """
    Get the path of the pickle file for the reviews of the given `app_id`.
    """
    return DATA_DIRECTORY_PATH / f'data_cache-{app_id}.pkl'


def get_app_reviews(app_id: str) -> list:
    """
    Fetch reviews for an app.
    """
    return reviews_all(app_id, lang='en', country='us', sleep_milliseconds=0)


def get_google_play_data(app_id: str, flush: bool=False) -> list:
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


def find_reviews_by_keyword_list(df: pd.DataFrame, keyword_list: List[str]) -> None:
    """
    Prints all reviews in `df` (a pandas DataFrame) whose `'content'` attribute
    contains any of the keywords in `keyword_list` as a substring.

    Also prints the number of reviews that were found, including a breakdown of
    the number of reviews from each topic.
    """
    total = 0
    topic_total_map = {}

    for i in range(df.shape[0]):
        review = df.iloc[i]
        if any(keyword in review['content'].lower() for keyword in keyword_list):
            total += 1

            topic = review['topic']
            if topic in topic_total_map:
                topic_total_map[topic] += 1
            else:
                topic_total_map[topic] = 1

            if isinstance(review['at'], str):
                date = datetime.strptime(review['at'], '%Y-%m-%d %H:%M:%S')
            else:
                date = review['at']

            print(f"TOPIC {topic}, ROW {i} [{date.strftime('%Y-%m-%d')}]: {review['content']}")

    print('===== Totals from each topic =====')

    for topic in sorted(topic_total_map.keys()):
        print(f'Topic {topic}: {topic_total_map[topic]}', end=', ')

    print(f'Total: {total}, all else 0')


def find_reviews_by_keyword(df: pd.DateOffset, keyword: str) -> None:
    """
    Prints all reviews in `df` (a pandas DataFrame) whose `'content'` attribute
    contains `keyword` as a substring.

    Also prints the number of reviews that were found, including a breakdown of
    the number of reviews from each topic.
    """
    find_reviews_by_keyword_list(df, [keyword])
