"""
01_split_file.py
Splits one big file into smaller ones

Takes a file name as an argument and assumes that there is a
directory already called 'tweets_split/'

Sample call: python 01_split_file.py tweet_ids/election-tweets_1
Ethan Roubenoff 4/19

"""

from time import sleep
import sys

def split_tweets(bigfile_str):
    # Create individual files of 100,000 tweets each 
    lines_per_file = 100000
    smallfile = None

    with open(bigfile_str) as bigfile:
        for lineno, line in enumerate(bigfile):
            
            # A simple progress bar 
            sys.stdout.write('\r')
            sys.stdout.write("[%-20s] %d%%" % ('='*(lineno//100000), 5*lineno//100000))
            sys.stdout.flush()

            if lineno % lines_per_file == 0:
                if smallfile:
                    smallfile.close()
                small_filename = 'tweets_split/' + bigfile_str + '_subsample_{}'.format(lineno//lines_per_file)
                smallfile = open(small_filename, "w")
            smallfile.write(line)
        if smallfile:
            smallfile.close()

if __name__ == "__main__" :
    bigfile = sys.argv[1] 

    split_tweets(bigfile)


