import requests
import json
import re
import time
import math
from datetime import datetime
import pandas as pd
from os import path

# slightly edited version of code presented here:
# https://www.osrsbox.com/blog/2019/03/18/watercooler-scraping-an-entire-subreddit-2007scape/

PUSHSHIFT_REDDIT_URL = "https://api.pushshift.io/reddit"

def fetchObjects(**kwargs):
    # Default paramaters for API query
    params = {
        "sort_type":"created_utc",
        "sort":"asc",
        "size":100
        }

    # Add additional paramters based on function arguments
    for key,value in kwargs.items():
        params[key] = value

    # Print API query paramaters
    print(params)

    # Set the type variable based on function input
    # The type can be "comment" or "submission", default is "comment"
    type = "comment"
    if 'type' in kwargs and kwargs['type'].lower() == "submission":
        type = "submission"
    
    # Perform an API request
    r = requests.get(PUSHSHIFT_REDDIT_URL + "/search/" + type, params=params)
    print(r)

    # Check the status code, if successful, process the data
    if r.status_code == 200:
        response = json.loads(r.text)
        data = response['data']
        sorted_data_by_id = sorted(data, key=lambda x: int(x['id'],36))
        return sorted_data_by_id

def extract_reddit_data(**kwargs):
    # Specify the start timestamp
    max_created_utc = math.floor(datetime(2010,1,1).timestamp())
    max_id = 0

    # Open a file for JSON output
    fn = kwargs['subreddit'] + "_" + kwargs['type'] + ".json"
    if(path.exists(fn)):
        existing_data = pd.read_json(fn, lines=True)
        most_recent = existing_data['created_utc'].idxmax()
        recent_created_utc = existing_data['created_utc'][most_recent]
        recent_id = int(existing_data['id'][most_recent],36)
        if(recent_created_utc > max_created_utc):
            max_created_utc = recent_created_utc - 1
            max_id = recent_id
    file = open(fn,"a")


    # While loop for recursive function
    while 1:
        nothing_processed = True
        # Call the recursive function
        objects = fetchObjects(**kwargs,after=max_created_utc)
        
        # Loop the returned data, ordered by date
        for object in objects:
            id = int(object['id'],36)
            if id > max_id:
                nothing_processed = False
                created_utc = object['created_utc']
                max_id = id
                if created_utc > max_created_utc: max_created_utc = created_utc
                # Output JSON data to the opened file
                print(json.dumps(object,sort_keys=True,ensure_ascii=True),file=file)
        
        # Exit if nothing happened
        if nothing_processed: return
        max_created_utc -= 1

        # Sleep a little before the next recursive function call
        time.sleep(1)

# Start program by calling function with:
# 1) Subreddit specified
# 2) The type of data required (comment or submission)
extract_reddit_data(subreddit="wallstreetbets",type="submission")
