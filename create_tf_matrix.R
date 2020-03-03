# Create the TF matrix within sql
library(RPostgreSQL)
library(tidyverse)



# psql --port=5433 --db=ethandb
pw <- scan("./pw.txt", what = "character")
drv <- dbDriver("PostgreSQL") 
con <- dbConnect(drv, dbname = "ethandb",
                 host = "localhost", port = 5433,
                 user = "eroubenoff", password = pw)
rm(pw)

q <- "SELECT hashtags.user, hashtags FROM hashtags"