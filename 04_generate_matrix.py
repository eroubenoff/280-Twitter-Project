"""
05_generate_matrix.py

Generates Term Frequency (TF) matrix.  Each row is a user and each column is
a hashtag.  Each entry is the relative frequency of term i in the set of tweets
by user j.

TF_{ij} = [number of times user j uses hashtag i] / [total number of terms produced by user j]

This script assumes:
    - MongoDB is listening on localhost/27017
    - The databse is called "twitter" with collection "tweets"
    - There are numpy objects stored with the format:
        - Row: [object, frequency]

"""

import csv
import pymongo
import numpy as np

def create_matrix(client, collection_name):
    
    try: 
        hashtags = np.load("hashtags_list.npy")
        users = np.load("user_list.npy")
    except FileNotFoundError as e:
        print(".npy file not found") 

    # Remove hashtags with only one tweet
    # Remove users with only one tweet
    
    print(hashtags.shape, users.shape)
    print((hashtags.ndim))
    
    # hashtags = hashtags[]

    # data = client["twitter"][collection_name].find(no_cursor_timeout = True)
    # count = data.count()

    hashtags = []
    users = []

    return


if __name__ == "__main__":
    client = pymongo.MongoClient("localhost", 27017)

    create_matrix(client, "tweets")
        
