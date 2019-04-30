"""
Creates a collection in Mongo that contains tweets that
fit the following criteria:

    - In english
    - Have text

And save only the following fields:

    - Tweet id (hashed as _id), unique
    - User id (hashed)
    - Full text
    - Geo coordinates if available
    - Number of likes and RTs

Also create a collection of users who meet the above criteria
that saves:

    - User id (hashed), unique

Returns null.  Will not duplicate entries.

This script assumes that MongoDB is listening on the
default port and that the collection "tweets" (unfiltered) already
exists.

Note that this script _does not_ delete tweets from the unfiltered
"tweets" collection.

"""

import pymongo
import logging


def filter_tweets(client, limit):

    data = client["twitter"]["tweets"].find({}).limit(limit)

    tweets_count = users_count = tweets_failure = users_failure = 0

    for i, tweet in enumerate(data):

        # If the tweet has no text or is not in english, skip it
        if not hasattr(tweet, "full_text") or not tweet["lang"] == "en":
            try:
                data.next()
            except Exception:
                break
        filtered_tweet = {}
        user = {}
        filtered_tweet["_id"] = tweet["id"]
        filtered_tweet["user"] = tweet["user"]["id"]
        filtered_tweet["full_text"] = tweet["full_text"]
        filtered_tweet["hashtags"] = []
        if tweet["coordinates"]:
            filtered_tweet["coordinates"] = tweet["coordinates"]
        if tweet["place"]:
            filtered_tweet["place"] = tweet["place"]

        user["_id"] = tweet["user"]["id"]

        for j, entity in enumerate(tweet["entities"]["hashtags"]):
            filtered_tweet["hashtags"].append(entity["text"].lower())

        try:
            client["twitter"]["tweets_filtered"].update(
                {"_id": filtered_tweet["_id"]},
                filtered_tweet,
                upsert=True)
            tweets_count += 1
        except Exception as e:
            tweets_failure += 1
            out_str(e)

        try:
            client["twitter"]["users"].update(
                {"_id": user["_id"]},
                user,
                upsert=True)
            users_count += 1
        except Exception as e:
            users_failure += 1
            out_str(e)

        if i % 5000 == 0:
            out_str("Filter Tweets: Processed {0} tweets of {1} ".format(
                i, limit))

    out_str("-----Insertion Complete-----")
    out_str("{0} tweets attempted.  Found {1} valid tweets and"
            " {2} users updated. {3} tweets failed"
            " and {4} users failed.".format(limit, tweets_count, users_count,
                                            tweets_failure, users_failure))


def out_str(s):
    logging.info(s)
    print(s)


if __name__ == "__main__":

    # Configure logging
    logging.basicConfig(
        filename="filter_log.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    out_str("---------------------------------------")
    out_str("Initializing update of tweets_filtered and users")
    out_str("\n")

    client = pymongo.MongoClient("localhost", 27017)

    if "tweets" not in client["twitter"].collection_names():
        out_str("Collection \"tweets\" not present in database."
                " Aborting.")

    if "tweets_filtered" not in client["twitter"].collection_names():
        client["twitter"].create_collection("tweets_filtered")
    if "users" not in client["twitter"].collection_names():
        client["twitter"].create_collection("users")

    # Optional limit for testing purposes
    limit = 0

    if limit == 0:
        limit = client["twitter"]["tweets"].count()

    out_str("Filtering {0} tweets".format(limit))
    filter_tweets(client, limit)
