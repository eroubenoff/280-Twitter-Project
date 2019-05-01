"""
Support Vector Machines to predict tweet political polarity.

Assumes:
    -Training data is stored in 'training_data/'.  Will use the last-listed
    file unless otherwise specified.
    -The whole dataset is stored as tf_sparse.npz

"""

import logging
import os
import time
import pickle
from sklearn import svm
from scipy import sparse
import random


def train_model(tf_sparse, users, hashtags, training_data):
    # np.set_printoptions(threshold=np.inf)

    training_data_user = [users[x] for x in training_data.keys()]
    training_data_out = [x for x in training_data.values()]
    tf_fit = tf_sparse[training_data_user,:]

    hashtags_lookup = {v:k for k, v in hashtags.items()}

    clf = svm.SVC()
    fit = clf.fit(tf_fit, training_data_out)
    print(fit)

    for u in random.sample(list(users), 100):
        rows, cols = tf_sparse[users[u],:].nonzero()
        print([hashtags_lookup[x] for x in cols])
        print(fit.predict(tf_sparse[users[u],:]))




def out_str(s):
    logging.info(s)
    print(s)


if __name__ == "__main__":

    # Initalize logging
    logging.basicConfig(
        filename="SVM_log.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    out_str("---------------------------------------")
    out_str("Beginning at {0}".format(time.localtime()))

    # Load in all the necessary files

    # Sparse matrix
    tf_sparse = sparse.load_npz('tf_sparse.npz')
    # User hashmap
    with open('users.pickle', 'rb') as f:
        users = pickle.load(f)
    # Hashtag hashmap
    with open('hashtags.pickle', 'rb') as f:
        hashtags = pickle.load(f)
    # Training data (the most recently created one)
    fpath = "training_data/" + os.listdir('training_data')[-1]
    with open(fpath, 'rb') as f:
        training_data = pickle.load(f)

    model = train_model(tf_sparse, users, hashtags, training_data)
