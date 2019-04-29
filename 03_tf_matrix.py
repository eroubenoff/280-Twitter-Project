"""Write list of users who tweeted to a CSV, user_list.csv. 
Also write a list of hashtags and their frequency, 
hashtag_list.csv

Also export numpy objects for faster processing.

The CSV has rows as (user id/hashtag, number of times tweeted).

This script assumes:
 - MongoDB is listening on localhost/27017
 - The database is called "twitter" with collection "tweets"
"""

import csv
import numpy as np
import pandas as pd
import pymongo
import time
import logging
import h5py

def generate_matrix(data):
    """Get all users and hashtags in collection and export as separate CSVs."""
    
    out_str("Querying {0} tweets from Mongo".format(limit))
    count = data.count()

    hashtags = set() 
    users = set() 

    for i, tweet in enumerate(data):
        # Maintain users as set of ids
        users.add(tweet["user"]["id"])
        
        # Maintain hashtags as set of hashtags
        for j, entity in enumerate(tweet["entities"]["hashtags"]):
            hashtags.add(entity["text"].lower())

        if i % 50000 == 0:
            out_str("Generate Matrix: Processed {0}/{1} tweets; have {2} users and {3} hashtags".format(i, count, len(users), len(hashtags)))

    # Apparentl #id is used as a hashtag and is creating collissions with the id column.  For now, just pop it.  Deal with it later:
    try: 
        del hashtags["id"]
        out_str("deleting #id")
    except:
        pass
    
    # Need to convert to list to maintain order.  "id" is always the first column
    users = list(users)
    hashtags = ['id'] + list(hashtags)
    out_str("Generated {0} users and {1} hashtags".format(len(users), len(hashtags) - 1))

    tf_matrix = np.empty((len(users), len(hashtags)))

    # Assign users list to first column in matrix 
    tf_matrix[:,0] = users
    
    # Enumerate the list to a dict of (key, index)
    users = {k: v for v, k in enumerate(users)} 
    hashtags = {k: v for v, k in enumerate(hashtags)} 

    return(tf_matrix, users, hashtags)




def fill_matrix(data, tf_matrix):

    tf_matrix, users, hashtags = tf_matrix
    out_str("Creating Aggregation Pipeline")

    count = 0 
    num_updated = 0 
    failure = 0

    for x, tweet in enumerate(data):
        for y, entity in enumerate(tweet["entities"]["hashtags"]):
            try: 
                i = users[tweet["user"]["id"]]
                j = hashtags[entity["text"].lower()]
                tf_matrix[i][j] += 1
                num_updated += 1
            except Exception as e:
                failure += 1

        count += 1

        if count % 50000 == 0:
            out_str("Fill Matrix: Processed {0} tweets; updated {1} hashtags".format(count, num_updated))

    out_str("Updated {0} tweets and {1} hashtags. {2} failed.".format(count, num_updated, failure))

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
    limit = 1000

    # Initialize pymongo client 
    client = pymongo.MongoClient("localhost", 27017)
    data = client["twitter"]["tweets"].find({}, {"_id": 0, "user": 1, "entities": 1}, no_cursor_timeout = True).limit(limit)
    start = time.time()
    
    # Generate empty and assign to tf_matrix
    tf_matrix =  generate_matrix(data)
    end_1 = time.time()
    out_str("generate_matrix took {0}".format(end_1 - start))
    
    data = data.rewind()
    tf_matrix = fill_matrix(data, tf_matrix)
    end_2 = time.time()
    out_str("fill_matrix took {0}".format(end_2-end_1))

    # out_str("saving matrix")
    # with h5py.File('tf_matrix.h5', 'w') as hf:
        # hf.create_dataset("tf_matrix", data = tf_matrix)

        # To read: 
        # with h5py.File('tf_matrix.h5', 'r') as hf:
        # tf_matrix = hf['tf_matrix'][:]
    out_str("Total time: {0} for {1} entries".format(end_2-start, limit))
    out_str("Matrix size: {} bytes".format(tf_matrix.nbytes))
    out_str("---------------------------------------")
