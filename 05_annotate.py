"""
Annotate a random subset of users and save those annotations.

Assumes that:
    -'users.pickle', a dict of users and row indices
    -MongoDB is listening on default port

Randomly specifies n users and returns all tweets by each user.  Script will
then display all tweets on screen for user to enter 'r, l, a' for each user.

Returns a dict of the annotated users and the affiliation.

Saves the annotation to 'training_data/annotated_users_{time.time()}'

The idea is that you can specity a training data to call in SVM, or it will
just use the most recently created file.
"""

import pymongo
import pickle
import random
import os
import time
import logging


def annotate_user(client, user, n, u):
    os.system('clear')
    print('\n'*3)
    print("User {0} of {1}".format(u + 1, n))
    print("Are the following tweets l(eft), r(ight), or a(mbiguous)?")
    print('\n'*3)

    # Query for that user id and only return fill that full text
    data = client.find({'user': user}, {"_id": 0, "full_text": 1})

    # Print each tweet text to the console
    for t in data:
        if "full_text" not in t:
            try:
                continue
            except Exception:
                break
        print(t["full_text"])

    print('\n'*3)
    resp = input("Please enter if user seems l(eft), r(ight),"
                 "or a(mbiguous): \n")
    while resp not in ["l", "r", "a"]:
        resp = input("Not a valid entry: only l, r, or a are valid:")

    return(resp)


def out_str(s):
    logging.info(s)
    print(s)


if __name__ == "__main__":
    # Initalize logging
    logging.basicConfig(
        filename="annotation_log.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    out_str("---------------------------------------")
    out_str("Annotating at {0}".format(time.localtime()))
    client = pymongo.MongoClient("localhost",
                                 27017)["twitter"]["tweets_filtered"]

    with open('users.pickle', 'rb') as up:
        users = pickle.load(up)

    # n is the number of users to annotate
    n = 100
    users = random.sample(list(users), n)

    annotated = {}

    for u in users:
        annotated[u] = annotate_user(client, u, n, users.index(u))
        out_str("Annotated user {0} as {1}".format(u, annotated[u]))
    # Save it as a pickle
    out_str("User entered: ")
    out_str(annotated)

    # Save each annotation as a unique file in './training_data/'
    try:
        os.mkdir('training_data')
    except FileExistsError:
        pass
    with open('training_data/annotated_users_{0}.pickle'.format(
            time.time()), 'wb') as up:
        pickle.dump(annotated, up)
