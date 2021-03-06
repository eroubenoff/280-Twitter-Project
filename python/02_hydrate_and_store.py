"""
Hydrate tweet ids and store directly into mongoDB.

The file does nothing when executed by default, to prevent unintentional
operations on the database. Use this as a utilities file, modify the __main__
block, and run when necessary.

This script assumes:
 - tweet id files are stored in ./tweets_split/
 - MongoDB is listening on localhost/27017
 - The database of interest is called "twitter"

This script will:
 - Create (if DNE) two collections, "tweets" and "read_ids"

Script modified from Twarc and Tushar Chandra.
"""

import os
import pymongo
import logging
import requests
import csv
import time
from requests_oauthlib import OAuth1Session
from credentials import auth
from error_email import email_dec


def rate_limit(f):
    """
    A decorator to handle rate limiting from the Twitter API. If
    a rate limit error is encountered we will sleep until we can
    issue the API call again.

    Modified from Twarc.
    """
    def new_f(*args, **kwargs):
        while True:
            resp = f(*args, **kwargs)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                reset = int(resp.headers['x-rate-limit-reset'])
                now = time.time()
                seconds = reset - now + 10
                if seconds < 1:
                    seconds = 10
                logging.info("rate limit exceeded: sleeping %s secs", seconds)
                print("Rate limit, sleeping.  Check log.")
                time.sleep(seconds)
            elif resp.status_code == 503:
                seconds = 60
                logging.info("503 from Twitter API, sleeping %s", seconds)
                print("Rate limit, sleeping.  Check log.")
                time.sleep(seconds)
            elif resp.status_code == 500:
                seconds = 600
                logging.info("500 from Twitter API, sleeping {0}".format(
                    seconds))
                time.sleep(seconds)
            else:
                resp.raise_for_status()
    return new_f


@rate_limit
def post(*args, **kwargs):
    """
    A function that makes the relevant API call.  Takes an OAuthLib object
    and should return the output of the API call.

    Modified from Twarc.
    """
    try:
        return twitter_client.post(*args, **kwargs)
    except requests.exceptions.ConnectionError as e:
        logging.info("caught connection error %s", e)
        _connect()
        return post(*args, **kwargs)


def _connect():
    """
    Creates OAuthLib object that is used for queries.  Assumes that the correct
    credentials are included.

    Modified from Twarc.
    """
    logging.info("creating http session")
    return OAuth1Session(
        client_key=auth["client_key"],
        client_secret=auth["client_secret"],
        resource_owner_key=auth["resource_owner_key"],
        resource_owner_secret=auth["resource_owner_secret"])


@email_dec
def hydrate(iterator, db_client):
    """
    Pass in an iterator of tweet ids and mongodb database.
    Check to see if the tweet has already beenand if need be, query in
    batches of 100.  Checks both the tweets collection and read_ids collection.

    Modified from Twarc.
    """

    ids = []
    url = "https://api.twitter.com/1.1/statuses/lookup.json"

    tweets_collection = db_client["twitter"]["tweets"]
    read_ids = db_client["twitter"]["read_ids"]
    count = 0
    for tweet_id in iterator:
        tweet_id = tweet_id[0]

        # If tweet is already in the "read_ids" collection, skip. Else, add it.

        if read_ids.find({'id': int(tweet_id)}).count() >= 1:
            continue
        else:
            read_ids.insert_one({'id': int(tweet_id)})

        # If the tweet is already in database, skip
        if tweets_collection.find({'id': int(tweet_id)}).count() >= 1:
            continue

        ids.append(tweet_id)
        if len(ids) == 100:
            # Call post method which will return a post object with json field
            resp = post(url, data={"id": ','.join(ids)})
            tweets = resp.json()
            for tweet in tweets:
                tweets_collection.insert_one(tweet)
            count += len(tweets)

            ids = []

        if count % 100000 == 0:
            logging.info("successfully wrote %s tweets into database",
                         count)
            count = 0

    # if the iterator finishes and there are still ids in the list
    if len(ids) > 0:
        resp = post(url, data={"id": ','.join(ids)})
        tweets = resp.json()
        for tweet in tweets:
            tweets_collection.insert_one(tweet)
        logging.info("successfully wrote %s tweets into database", len(tweets))


def store_tweets(db_client):
    """Read all JSON files and store all tweets to the database.

    db_client: pymongo.MongoClient to connect to
    """

    # Create collections needed
    # On your own time, it is best to convert this to zlib compression
    if "tweets" not in db_client["twitter"].collection_names():
        db_client["twitter"].crate_collection("tweets")
    # Create a collection of read tweet ids (to avoid querying multiple times)
    if "read_ids" not in db_client["twitter"].collection_names():
        db_client["twitter"].create_collection("read_ids")

    # Read stored tweets and store into database.
    tweet_dir = "./tweets_split/"

    file_index = 0

    # For each file in the tweets directory
    for tweet_file in sorted(os.listdir(tweet_dir)):
        # skip the first n files
        if file_index < 675:
            logging.info("skipping file %s", str(file_index))
            file_index += 1
            continue

        fpath = os.path.join(tweet_dir, tweet_file)

        with open(fpath) as f:
            csvfile = csv.reader(f)
            logging.info("hydrating file %s", str(tweet_file))
            hydrate(csvfile, db_client)
            logging.info("file %s fully hydrated", str(tweet_file))

        file_index += 1


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        filename="hydration_log.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    # Connect to database and create OAuth objects.  These are globals.
    db_client = pymongo.MongoClient("localhost", 27017)
    twitter_client = _connect()

    # UNCOMMENT THE BELOW LINE TO RUN THE SCRIPT.
    store_tweets(db_client)
