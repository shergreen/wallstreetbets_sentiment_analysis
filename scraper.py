import math
import json
import requests
import itertools
import numpy as np

import time
from datetime import datetime, timedelta

# based on:
# https://medium.com/@pasdan/how-to-scrap-reddit-using-pushshift-io-via-python-a3ebcc9b83f4

def make_request(uri, max_retries = 5):
    def fire_away(uri):
        response = requests.get(uri)
        assert response.status_code == 200
        return json.loads(response.content)
    current_tries = 1
    while current_tries < max_retries:
        try:
            time.sleep(1)
            response = fire_away(uri)
            return response
        except:
            time.sleep(1)
            current_tries += 1
    return fire_away(uri)

def get_selftext(post):
    try:
        text = post['selftext']
    except KeyError:
        text = ''
    return text

def pull_posts_for(subreddit, start_at, end_at):
    
    def map_posts(posts):
        return list(map(lambda post: {
            'id': post['id'],
            'created_utc': post['created_utc'],
            'prefix': 't4_',
            'title': post['title'],
            'text': get_selftext(post)
        }, posts))
    
    SIZE = 100
    URI_TEMPLATE = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}'

    post_collections = map_posts( \
        make_request( \
            URI_TEMPLATE.format( \
                subreddit, start_at, end_at, SIZE))['data'])
    n = len(post_collections)
    while n == SIZE:
        last = post_collections[-1]
        new_start_at = last['created_utc'] - (10)
        
        more_posts = map_posts( \
            make_request( \
                URI_TEMPLATE.format( \
                    subreddit, new_start_at, end_at, SIZE))['data'])
        
        n = len(more_posts)
        post_collections.extend(more_posts)
    return post_collections


subreddit = 'wallstreetbets'
end_at = math.ceil(datetime.utcnow().timestamp())
start_at = math.floor(datetime(2020,8,9).timestamp())
posts = pull_posts_for(subreddit, start_at, end_at)
## ~ 4314
print(len(posts))
## ~ 4306
print(len(np.unique([ post['id'] for post in posts ])))


for i in range(0,10):
    print(posts[i])

# Sample API request to get keys:
#https://api.pushshift.io/reddit/search/submission?subreddit=wallstreetbets&after=1565546826&before=1597082827&size=200
