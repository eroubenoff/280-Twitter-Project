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

    tweets_count = tweets_failure = 0

    for i, tweet in enumerate(data):

        # If the tweet has no text or is not in english, skip it
        if "full_text" not in tweet.keys() or not tweet["lang"] == "en":
            try:
                tweets_failure += 1
                data.next()
            except Exception:
                break
        try:
            filtered_tweet = {
                "_id": tweet["id"],
                "user": tweet["user"]["id"],
                "full_text": tweet["full_text"],
                "hashtags": [],
                "coordinates": tweet["coordinates"] if tweet[
                    "coordinates"] else None,
                "place": tweet["place"] if tweet["place"] else None,
            }
            """
            user = {
                "_id": tweet["user"]["id"]
            }
            """
            for j, entity in enumerate(tweet["entities"]["hashtags"]):
                filtered_tweet["hashtags"].append(entity["text"].lower())
        except Exception as e:
            out_str(repr(e))
            tweets_failure += 1
            data.next()

        try:
            client["twitter"]["tweets_filtered"].insert_one(
                filtered_tweet)
            tweets_count += 1
        except pymongo.errors.DuplicateKeyError:
            pass

        """
        try:
            client["twitter"]["users"].insert_one(
                user)
            users_count += 1
        except pymongo.errors.DuplicateKeyError:
            pass
        """

        if i % 5000 == 0:
            out_str("Filter Tweets: Processed {0} tweets of {1} ".format(
                i, limit))

    out_str("-----Insertion Complete-----")
    out_str("{0} tweets attempted.  Found {1} valid tweets and"
            " {2} tweets failed".format(limit, tweets_count,
                                        tweets_failure))


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
    """
    if "users" not in client["twitter"].collection_names():
        client["twitter"].create_collection("users")
    """

    # Optional limit for testing purposes
    limit = 0

    if limit == 0:
        limit = client["twitter"]["tweets"].count()

    out_str("Filtering {0} tweets".format(limit))
    filter_tweets(client, limit)
