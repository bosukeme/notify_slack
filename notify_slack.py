import twint
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from pymongo import MongoClient
from decouple import config as env_config
import json
import sys
import requests

MONGO_URL = env_config("MONGO_URL")

client = MongoClient(MONGO_URL)

db = client.notify_slack
collection = db.tweets


pd.options.mode.chained_assignment = None

today = datetime.now()
start_date = today + timedelta(0)
end_date = today + timedelta(1)


start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")



focus_handles = ["JoeBiden", "BorisJohnson", "rickygervais", "paulg"]

def get_record_details(search_dict, collection, find_one=True):
    try:
        query = collection.find_one(search_dict) if find_one else collection.find(search_dict)
        return query
    except Exception as e:
        print(e)
        return None


def insert_records(collection, record):
    try:
        collection.insert_one(record)
    except Exception as e:
        print(e)
        
    return None

def save_to_mongo_db(data):
    insert_records(collection, data)
    cur = collection.count_documents({})
    print(f"we have {cur} entries")
    
    return None



def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]

def get_latest_tweets_from_handle(username, num_tweets,start_date,  end_date):

    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = start_date
    c.Until = end_date
    c.Hide_output = True
    twint.run.Search(c)
    try:
        tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags',
                                    'username', 'name', 'link', 'urls', 'photos', 'video',
                                    'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])
    except Exception as e:
        print(e)
        tweet_df = pd.DataFrame()
        
    return tweet_df


def notify_slack(data, username):

    url = env_config("SLACK_WEBHOOK_URL")
    
    message = (f'{data}')
    title = (f"New Incoming Message : {username} :zap:")
    
    slack_data = {
        "username": "Twitter_Extract",
        "attachments": [
            {
                "color":  "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
        
    return None

def run_notifications_for_slack():
    
    try:
        for username in focus_handles:
            print(username)
            tweet_df = get_latest_tweets_from_handle(username, 500, start_date_str,  end_date_str)
            tweet_df_dict = tweet_df.to_dict("records")

            for item in tweet_df_dict:
                search_dict = {'id': item['id']}

                query = get_record_details(search_dict, collection, find_one=True)
                if query == None:

                    notify_slack(item, username)
                    save_to_mongo_db(item)
    
    except Exception as e:
        print(e)


    return None


run_notifications_for_slack()
