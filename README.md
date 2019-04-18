# Twitter Project
## Ethan Roubenoff
## Started April 2019

Project goals: read in tweets from election 2016. Use SVM to generate political affiliations from tweets.  Plot spatially and use Bayesian SAE to generate estimates by district.  Compare with actual outcomes.

# 0: Download and Hydrate
Tweets are downloaded from the GMU data repository.  I downloaded the files `election-tweets[1-6]` and `election-day` from the [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PDI7IN) and hydrated using [hydrator](https://github.com/DocNow/hydrator).  To download the data, run:
```
wget -ibc urls.txt
```
Which will download all the relevant files. Run Hydrator. It will take weeks. 

# 1: Split into manageable files
You will need to split the big hydrated tweet files into more memory-manageable sizes.  Use `01_split_file.py` to split each file into subsets of 100,000 tweets each.  For example, a 50 million tweet file will be split into 500 files with unique names.  Below is a sample usage:
```
python 01_split_file.py [filename]
```
for each file.

# 2: Import to Mongodb.
This requires minimal setup.  [Install Mongodb](https://docs.mongodb.com/manual/administration/install-community/) and make sure it is listening on the default port 27017.  There is no need to create an initial database.  Since this can take a while the main call is commented in the file.  Make sure to uncomment when you're ready to run.
```
python 02_store_to_db.py
```
h/t to Tushar Chandra for this script.
