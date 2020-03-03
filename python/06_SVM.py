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
import scipy as sp


def train_model(tf_sparse, users, hashtags, training_data):

    tf_sparse = count_to_proportion(tf_sparse)

    hashtags_lookup = {v: k for k, v in hashtags.items()}
    # This is the inverse of the hashtags dict.

    tf_fit = tf_sparse[[users[x] for x in training_data.keys()], :]

    clf = svm.SVC()
    fit = clf.fit(tf_fit, [x for x in training_data.values()])
    print(fit)
"""
    for u in random.sample(list(users), 100):
        rows, cols = tf_sparse[users[u], :].nonzero()
        print([hashtags_lookup[x] for x in cols])
        print(fit.predict(tf_sparse[users[u], :]))
"""

def validate(tf_sparse, users, hashtags, training_data, k):
    training_data_length = len(training_data)
    ordered_training_data = list(training_data.items())

    count = 0
    for subset in range(k):
        tmp = {}
        j = 0
        while j < (training_data_length // k):
            obj = ordered_training_data[count]
            tmp[obj[0]] = obj[1]
            count += 1
            j += 1
        print(tmp)
        train_model(tf_sparse, users, hashtags, tmp)


def count_to_proportion(tf_sparse):
    rowsums = sp.sum(tf_sparse, axis=1)
    tf_sparse = sparse.csr_matrix(tf_sparse, dtype="d")
    return(tf_sparse.multiply(1/rowsums))


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
    # validate(tf_sparse, users, hashtags, training_data, 5)
