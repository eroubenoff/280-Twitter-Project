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
import pymongo

def export_userlist(client, collection_name):
    """Get all users and hashtags in collection and export as separate CSVs."""

    data = client["twitter"][collection_name].find(no_cursor_timeout = True)
    count = data.count()

    hashtags = {}
    users = {}

    for i, tweet in enumerate(data):
        # Maintain users as hashmap of (user:number of tweets)
        try:
            users[tweet["user"]["id_str"]] += 1
        except Exception as e:
            users[tweet["user"]["id_str"]] = 1
        
        # Maintain hashtags as a hashmap of (hashtag:number of tweets)
        for j, entity in enumerate(tweet["entities"]["hashtags"]):
            try:
                hashtags[entity["text"]] += 1
            except Exception as e:
                hashtags[entity["text"]] = 1

        if i % 50000 == 0:
            print("Processed {0}/{1} tweets; have {2} users and {3} hashtags".format(i, count, len(users.keys()), len(hashtags.keys())))

    np.save("user_list", users)
    np.save("hashtags_list", hashtags)


    """
    with open("user_list.csv", "w") as outfile:
        writer = csv.writer(outfile)
        for user_id, num_tweets in users.items():
            try:
                writer.writerow([user_id, num_tweets])
            except:
                continue 

    with open("hashtag_list.csv", "w") as outfile:
        writer = csv.writer(outfile)
        for hashtag, num_tweets in hashtags.items():
            try:
                writer.writerow([hashtag, num_tweets])
            except:
                continue 
    """

    return


if __name__ == "__main__":
    client = pymongo.MongoClient("localhost", 27017)
    export_userlist(client, "tweets")
