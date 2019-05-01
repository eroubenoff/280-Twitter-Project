"""
test
Write list of users who tweeted to a CSV, user_list.csv.
Also write a list of hashtags and their frequency,
hashtag_list.csv

Also export numpy objects for faster processing.

The CSV has rows as (user id/hashtag, number of times tweeted).

This script assumes:
 - MongoDB is listening on localhost/27017
 - The database is called "twitter" with collection "tweets"
"""

import pymongo
import time
import logging
import scipy as sp
from scipy import sparse
import pickle


def generate_matrix(data):
    """
    Generates a sparse matrix of users by hashtag usage.  Does so by querying
    tweets from Mongo and looping through them.  Maintain a set of users and
    hashtags.

    Input: a cursor to mongo

    Returns:
        - tf_matrix: a sparse (dok) matrix of (#users x #hashtags)
        - hashtags: a hashmap of user id to row index in tf_matrix
        - hashtags: a hashmap of hashtag to column index in tf_matrix
    """

    out_str("Querying {0} tweets from Mongo".format(limit))
    count = data.count()

    hashtags = {}
    users = {}

    for i, tweet in enumerate(data):
        # Add each user to the set of users (will not duplicate)
        try:
            users[tweet["user"]] += 1
        except Exception:
            users[tweet["user"]] = 1

        # Add each hashtag to set of hashtags (will not duplicate)
        for entity in tweet["hashtags"]:
            try:
                hashtags[entity.lower()] += 1
            except Exception:
                hashtags[entity.lower()] = 1

        if i % 50000 == 0:
            out_str("Generate Matrix: Processed {0}/{1} tweets; have {2} "
                    "users and {3} hashtags".format(i, count,
                                                    len(users), len(hashtags)))

    # Remove users that only had one tweet and hashtags that were only used
    # once
    for x in list(users.keys()):
        if users[x] == 1:
            del users[x]
    for x in list(hashtags.keys()):
        if hashtags[x] == 1:
            del hashtags[x]

    tf_matrix = sparse.dok_matrix((len(users), len(hashtags)), dtype='u4')
    out_str("Generated {0} users and {1} hashtags".format(len(users),
                                                          len(hashtags)))

    # Enumerate the list to a dict of (key, index)
    users = {k: v for v, k in enumerate(users.keys())}
    hashtags = {k: v for v, k in enumerate(hashtags.keys())}

    return(tf_matrix, users, hashtags)


def fill_matrix(data, tf_matrix, users, hashtags):
    """
    Loops through tweets and records number of times each user uses each
    hashtag.  Requires tf_matrix, a tuple of tf_matrix, user/row hashmap,
    and hashtag/column hashmap.

    Assumes data is a global.

    Returns:
        - Sparse matrix, where (user, hashtag) = frequency

    """

    out_str("Filling matrix")

    count = 0
    num_updated = 0
    failure = 0

    for x, tweet in enumerate(data):
        for entity in tweet["hashtags"]:
            try:
                i = users[tweet["user"]]
                j = hashtags[entity.lower()]
                tf_matrix[i, j] += 1
                num_updated += 1
            except Exception:
                failure += 1

        count += 1

        if count % 50000 == 0:
            out_str("Fill Matrix: Processed {0} tweets; updated {1} "
                    "hashtags".format(count, num_updated))

    out_str("Updated {0} tweets and {1} hashtags. "
            "{2} failed.".format(count, num_updated, failure))


    return(tf_matrix)


def out_str(s):
    logging.info(s)
    print(s)


if __name__ == "__main__":
    # Initalize logging
    logging.basicConfig(
        filename="matrix_log.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    out_str("---------------------------------------")
    out_str("Initializing matrix creation")
    limit = 0

    # Initialize pymongo client
    client = pymongo.MongoClient("localhost", 27017)
    data = client["twitter"]["tweets_filtered"].find(
        {},
        {"_id": 0, "user": 1, "hashtags": 1},
        no_cursor_timeout=True).limit(limit)

    # start timing
    start = time.time()

    # Generate empty matrix and assign to tf_matrix
    tf_matrix, users, hashtags = generate_matrix(data)
    end_1 = time.time()
    out_str("generate_matrix took {0}".format(end_1 - start))

    # Fill matrix
    data = data.rewind()
    tf_matrix = fill_matrix(data, tf_matrix, users, hashtags)
    end_2 = time.time()
    out_str("fill_matrix took {0} and matrix size "
            "is {1} MB".format(end_2-end_1, 2*tf_matrix.size/(1024**2)))

    # Write out matrix
    out_str("saving matrix")
    tf_matrix = sparse.csr_matrix(tf_matrix)
    sparse.save_npz('tf_sparse.npz', tf_matrix)

    with open('users.pickle', 'wb') as up:
        pickle.dump(users, up)
    with open('hashtags.pickle', 'wb') as hp:
        pickle.dump(hashtags, hp)

    end_3 = time.time()
    out_str("Saving sparse matrix took {0} seconds".format(end_3-end_2))
    out_str("Total time: {0} for {1} entries".format(end_2-start, limit))
    out_str("---------------------------------------")
