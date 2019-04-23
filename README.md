# Twitter Project
## Ethan Roubenoff
## Started April 2019

Project goals: read in tweets from election 2016. Use SVM to generate political affiliations from tweets.  Plot spatially and use Bayesian SAE to generate estimates by district.  Compare with actual outcomes.

# 0: Download the tweet id files
Tweets are downloaded from the GMU data repository.  I downloaded the files `election-tweets[1-6]` and `election-day` from the [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PDI7IN).  To download the data, run:
```
wget -ibc urls.txt
```
Which will download all the relevant files. 

# 1: Split into manageable files
You will need to split the big tweet id files into more memory-manageable sizes.  Use `01_split_file.py` to split each file into subsets of 100,000 tweets each.  For example, a 50 million tweet file will be split into 500 files with unique names.  Below is a sample usage:
```
python 01_split_file.py [filename]
```
for each file.

# 2: Hydrate the tweets
Two options: hydrate using [hydrator](https://github.com/DocNow/hydrator) and import to mongoDB manually or do it all in one step (recommended).
This requires minimal setup.  [Install Mongodb](https://docs.mongodb.com/manual/administration/install-community/) and make sure it is listening on the default port 27017.  There is no need to create an initial database.  Since this can take a while the main call is commented in the file.  Make sure to uncomment when you're ready to run.

1. `02_store_to_db.py` will add the hydrated .csv files directly to mongo.  Thanks Tushar for this.
2. `02_hydrate_and_store.py` which will read in the tweet IDs, query to see if it is in the database already, then try to retrieve it from twitter and store directly to mongo. This script is modified from Twarc.

Best to run as `nohup python 02_hydrate_and_store.py &`.

I recommend using `hydrate_and_store` because it seems to be faster than hydrator, which can get bogged down with larger files.  This script assumes that the big files have all been split up into smaller files of 100,000 (the number itself isn't so important) tweet ids.  It maintains an additional collection called `read_ids` which just is a list of all the ids the script has already run through.  This script will query 100 tweets at a time and the possible ones to the database `tweets`.  On startup it can take a while to check all the `read_ids` but I haven't figured out a better way yet.

It seems to be better if your `tweets` collection uses zlib compression and (possibly) no compression for `read_ids`.  

You can hydrate 8,640,000 tweets per day with the standard API, so all of the files will take about a month.

# 3: Generate user list and hashtag list

Outputs two CSVs: one containing a list of users and how many tweets they tweeted and one containing a list of hashtags and how many times they were used.

```
python 03_collect_users.py
```




