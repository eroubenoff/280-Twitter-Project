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
