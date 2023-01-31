import os
from datetime import datetime, timezone, timedelta
import pandas as pd
import tweepy

def fetch_recent_tweets_for_user(client, user_id):
    response = client.get_users_tweets(user_id, tweet_fields=["created_at"])
    # print(type(response.data[0]))
    rows = []
    if response.data is not None:
        for tweet in response.data:
            # print(tweet.created_at)
            created_at = pd.Timestamp(tweet.created_at).strftime("%Y-%m-%d %H:%M:%S")
            values = {"tweet_id": tweet.id, "author_id": user_id, 
                "created_at": created_at, "text": tweet.text}
            rows.append(values)
    return rows

def entry_point(request): # request is a Flask request
    # we can ignore request (Cloud Scheduler won't be putting anything in it)
    user_id = 602989093
    bearer_token = os.environ.get("BEARER_TOKEN")
    # print(bearer_token)
    client = tweepy.Client(bearer_token=bearer_token)
    tweet_rows = fetch_recent_tweets_for_user(client, user_id)
    print(tweet_rows)
    # TODO: insert tweet_rows into a BigQuery table
    # we need to restructure our code for Cloud Functions
    return "Success", 200

if __name__ == "__main__":
    entry_point(None)
