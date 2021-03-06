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
from error_email import email_dec


class Tweet:
    def __init__(self, tweet):
        self._id = tweet["id"]
        self.user = tweet["user"]["id"]
        self.full_text = tweet["full_text"] if "full_text" in \
            tweet.keys() else tweet["text"]
        self.hashtags = []
        self.coordinates = tweet["coordinates"]
        self.place = tweet["place"]

        for j, entity in enumerate(tweet["entities"]["hashtags"]):
            self.hashtags.append(entity["text"].lower())


@email_dec
def filter_tweets(client, limit):

    data = client["twitter"]["tweets"].find({}).limit(limit)

    tweets_success = tweets_failure = tweets_already = 0

    for i, tweet in enumerate(data):

        # If the tweet is not in english, skip it
        if tweet["lang"] != "en":
            tweets_failure += 1
            continue

        try:
            filtered_tweet = Tweet(tweet)
        except Exception:
            tweets_failure += 1
            continue

        try:
            client["twitter"]["tweets_filtered"].insert_one(
                filtered_tweet.__dict__)
            tweets_success += 1
        except pymongo.errors.DuplicateKeyError:
            tweets_already += 1

        if i % 100000 == 0:
            out_str("{0} tweets attempted.  Successfully inserted {1} valid tweets and"
                    " {2} tweets found invalid. {3} tweets were already present".format(
                        i, tweets_success, tweets_failure, tweets_already))

    # Clean up any remaining ones
    out_str("-----Insertion Complete-----")
    out_str("{0} tweets attempted.  Found {1} valid tweets and"
            " {2} tweets failed. {3} tweets were already present".format(
                limit, tweets_success, tweets_failure, tweets_already))


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

    # Note that these should be uncompressed collections
    if "tweets_filtered" not in client["twitter"].collection_names():
        client["twitter"].create_collection("tweets_filtered")
        client["twitter"]["tweets_filtered"].create_index("user")

    # Optional limit for testing purposes
    limit =0
    if limit == 0:
        limit = client["twitter"]["tweets"].count()

    out_str("Filtering {0} tweets".format(limit))
    filter_tweets(client, limit)
