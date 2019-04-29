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

def generate_matrix(client, collection_name):
    """Get all users and hashtags in collection and export as separate CSVs."""
    
    limit = 100
    out_str("Querying {0} tweets from Mongo".format(limit))
    data = client["twitter"][collection_name].find(no_cursor_timeout = True).limit(limit)
    count = data.count()

    hashtags = {}
    users = {}

    for i, tweet in enumerate(data):
        # Maintain users as hashmap of (user:number of tweets)
        try:
            users[tweet["user"]["id"]] += 1
        except Exception as e:
            users[tweet["user"]["id"]] = 1
        
        # Maintain hashtags as a hashmap of (hashtag:number of tweets)
        for j, entity in enumerate(tweet["entities"]["hashtags"]):
            try:
                hashtags[entity["text"].lower()] += 1
            except Exception as e:
                hashtags[entity["text"].lower()] = 1

        if i % 50000 == 0:
            out_str("Generate Matrix: Processed {0}/{1} tweets; have {2} users and {3} hashtags".format(i, count, len(users.keys()), len(hashtags.keys())))

    # Apparentl #id is used as a hashtag and is creating collissions with the id column.  For now, just pop it.  Deal with it later:
    try: 
        del hashtags["id"]
        out_str("deleting #id")
    except:
        pass

    users = list(users.keys())
    hashtags = ['id'] + list(hashtags.keys())
    out_str("Generated {0} users and {1} hashtags".format(len(users), len(hashtags) - 1))

    tf_matrix = np.empty(
           (len(users), len(hashtags))
        )
    tf_matrix[:,0] = users

    users = {k: v for v, k in enumerate(users)} 
    hashtags = {k: v for v, k in enumerate(hashtags)} 

    return(tf_matrix, users, hashtags)




def fill_matrix(client, collection_name, tf_matrix):

    tf_matrix, users, hashtags = tf_matrix
    
    out_str("Creating Aggregation Pipeline")
    data = client["twitter"][collection_name].aggregate([
            {"$limit" : 10000},
            {"$unwind": "$user"},
            {"$unwind": "$entities"},
            {"$unwind": "$entities.hashtags"},
            {"$unwind": "$entities.hashtags.text"},
            {"$group": {"_id": {"user": "$user.id"}, 
                "hashtags": {"$push" : {"$toLower": "$entities.hashtags.text"}}}},
            {"$project": {"_id":0 ,
                "user": "$_id.user",
                "hashtags": "$hashtags"}}
        ],
        allowDiskUse = True
        )
    """
    data = client["twitter"][collection_name].aggregate([
            {"$limit" : 10000},
            {"$unwind": "$user"},
            {"$unwind": "$entities"},
            {"$unwind": "$entities.hashtags"},
            {"$unwind": "$entities.hashtags.text"},
            {"$group": {"_id": {"user": "$user.id"}, 
                "hashtags": {"$push" : {"$toLower": "$entities.hashtags.text"}}}},
            {"$project": {"_id":0 ,
                "user": "$_id.user",
                "hashtags": "$hashtags"}}
        ],
        allowDiskUse = True
        )
    """

    count = 0 
    num_updated = 0 
    failure = 0

    for user in data:
        for hashtag in user["hashtags"]:
            print(user, hashtag)
            try: 
                i = users[user['user']]
                j = hashtags[hashtag]

                tf_matrix[i][j] += 1
                num_updated += 1
            except Exception as e:
                failure += 1
                # print(repr(e))

        count += 1

        if count % 5000 == 0:
            out_str("Fill Matrix: Processed {0} users; updated {1}".format(count, num_updated))

    out_str("Updated {0} users and {1} hashtags. {2} failed.".format(count, num_updated, failure))

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

    # Initialize pymongo client 
    client = pymongo.MongoClient("localhost", 27017)
    start = time.time()
    
    # Generate empty and assign to tf_matrix
    tf_matrix =  generate_matrix(client, "tweets")
    end_1 = time.time()
    out_str("generate_matrix took {0}".format(end_1 - start))

    tf_matrix = fill_matrix(client, "tweets", tf_matrix)
    end_2 = time.time()
    out_str("fill_matrix took {0}".format(end_2-end_1))

    out_str("saving matrix")
    with h5py.File('tf_matrix.h5', 'w') as hf:
        hf.create_dataset("tf_matrix", data = tf_matrix)

        # To read: 
        # with h5py.File('tf_matrix.h5', 'r') as hf:
        # tf_matrix = hf['tf_matrix'][:]
    out_str("---------------------------------------")
