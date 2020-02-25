#### PARSE TWITTER JSON TO POSTGRESQL #########################################
# This script takes the file TOPOSTGRES_tweets_filtered.json (34gb) and uploads
# it to the postgres database ethandb
###############################################################################
# Help from: https://www.r-bloggers.com/interact-with-postgis-from-r/

# In order to do this we'll have to parse each attribute separately and then
# link them
# Attributes: id <int>, user <int>, full_text <str>, hashtags <list of str>, 
# coordinates <list of int>, place <who knows>

# will need to create 4 tables: tweet text, hashtags, points, and places

library(RPostgreSQL)
library(rjson)
library(tidyverse)
library(data.table)
library(sf)
library(readr)
library(tmap)
library(lwgeom) # for fixing broken polygons
library(tictoc)

setwd("~/90days/twitter")
rm(list=ls())
gc()
f <- file("TOPOSTGRES_tweets_filtered.json")

# psql --port=5433 --db=ethandb
pw <- scan("./pw.txt", what = "character")

message("Starting script at ", Sys.time())
drv <- dbDriver("PostgreSQL") 
con <- dbConnect(drv, dbname = "ethandb",
                 host = "localhost", port = 5433,
                 user = "eroubenoff", password = pw)

updateDB <- function(x, pos = 0)  {
  
    # Arguments are x, pos to be compliant with readr callback. x is the chunk
    # and pos is the location (not necessary here)

    
  print(pos)
  
  if (length(x) == 0) {return(NULL)}
    
  # Convert from list of strings to JSON objs
  l <- lapply(x, rjson::fromJSON)
    
  tryCatch({suppressMessages({
    # Pull tweet text
    tweets <- l %>% 
      lapply(function(x) {x[c("_id", "user", "full_text")]}) %>% 
      rbindlist() %>%
      as_tibble()
    
    # Pull hashtags (in tidy/long format)
    hashtags <- l %>%
      lapply(function(x) {x[c("_id", "user", "hashtags")]}) %>%
      rbindlist() %>%
      as_tibble() %>%
      unnest(hashtags) 
    
    # Pull coordinates from tweets that have them
    points <- l %>% 
      lapply(function(x) {x[c("_id", "user", "coordinates")]}) %>%
      rbindlist() %>%
      as_tibble() %>% 
      filter(coordinates != "Point") %>%
      drop_na() %>%
      unnest_wider(coordinates) 
    
    if (nrow(points) > 0 ){
      points <- points %>%
        rename(x = 3,  # Select by position (needed some tweaking)
               y = 4) %>%
        st_as_sf(coords = c("x", "y"))
      }
    
    # Pull polygons from tweets that have them 
    # of course this is the complicated one
    places <- l %>%
      lapply(function(x) {x[c("_id", "user", "place")]}) %>%
      rbindlist() %>%
      as_tibble() %>%
      drop_na() 
    
    # There are multiple attributes for each polygon. As far as I can tell
    # they are a unique identifier, an api url location, type, name, location name,
    # country abbreviation, country, and list of coordinate bounding box. There
    # are 8 fields in total for each.  I will exploit that structure.
    
    if (nrow(places) > 0) {
      places.id <- places[seq(1, nrow(places), by = 8),] %>% 
        unnest(place) %>% 
        pull(place)
      places.api <- places[seq(2, nrow(places), by = 8),] %>% 
        unnest(place) %>%
        pull(place)
      places.type <- places[seq(3, nrow(places), by = 8),] %>% 
        unnest(place) %>%
        pull(place)
      places.name <- places[seq(4, nrow(places), by = 8),] %>% 
        unnest(place) %>%
        pull(place)
      places.longname <- places[seq(5, nrow(places), by = 8),] %>% 
        unnest(place) %>%
        pull(place)
      places.countryabbr <-  places[seq(6, nrow(places), by = 8),] %>% 
        unnest(place) %>%
        pull(place)
      places.country <- places[seq(7, nrow(places), by = 8),] %>% 
        unnest(place) %>%
        pull(place)
      
      # places.bbox is complicated. Need to remove the odd numbered rows 
      # and turn into spatial object
      places.bbox <-  places[seq(8, nrow(places), by = 8),] %>% unnest(place)
      places.bbox  <- places.bbox[seq(2, nrow(places.bbox), by = 2),]
      st_geometry(places.bbox) <- lapply(places.bbox %>% pull(place), function(x){
          x[[1]] %>% 
          unlist() %>%
          matrix(nrow = 4, ncol = 2, byrow = T) %>%
          rbind(.[1,])  %>% 
          list() %>%
          st_polygon()})  %>% 
        st_sfc() %>%
        st_make_valid()
      places.bbox <- places.bbox %>% select(-place)
      
      
      # Join all the places dfs together
      places <- cbind(places.bbox, places.id, places.api, places.type, 
                      places.name, places.longname, places.countryabbr, 
                      places.country)
      
      # Dop the temp objects (not strictly necessary, but neater while debugging)
      rm(places.bbox, places.id, places.api, places.type, places.name, 
         places.longname, places.countryabbr, places.country)
    
    }
    
    # browser()
    # write tables (non-spatial and spatial need different commands)
    dbWriteTable(con, "tweets", tweets, overwrite = FALSE, append = TRUE)
    dbWriteTable(con, "hashtags", hashtags, overwrite = FALSE, append = TRUE)
    if (nrow(points) > 0 ){
      sf::st_write(points, dsn = con, update  = T, append = T)
      # sf::dbWriteTable(con, "points", points, append = T, binary = F)
    }
    if (nrow(places) > 0) {
      sf::st_write(places, dsn = con, update  = T, append = T)
      # sf::dbWriteTable(con, "places", places, append = T, binary = F)
    }
    })},
      error = function(e) {
        # Keep having some random errors pop up.  I imagine it's some data
        # corruption.  I'll have it log the location (pos) and then later go and
        # manually insert.  Frustrating.
        print(e); print(pos)
        },
      finally = {return(NULL)}
    )
    
  # force close all connections on exit
    # on.exit({
    #   lapply(dbListConnections(drv = drv), function(x) {
    #       dbDisconnect(conn = x)})
    #   })
}

tic()
read_lines_chunked(f,  SideEffectChunkCallback$new(updateDB), chunk_size = 100000)
toc()

# # Test chunked read
# test_l <- readLines(f, n = 10000)
# read_lines_chunked(test_l,  SideEffectChunkCallback$new(updateDB), chunk_size = 1000)
# updateDB(test_l[1001:2000])

# 100
# 1,000: 45 sec
# 10,000 : 19 sec
# 100,00 : 19 sec
# 1,000,000 (in 10,000s) : 192 s
# 1,000,000 (in 100,000s): 214 sec 
# 1,000,000 (in 1,000,000s): buffer overflow :+(

# list fields
dbListFields(con, "tweets")
dbListFields(con, "hashtags")
dbListFields(con, "places")
dbListFields(con, "points")
# 
# # Read a table
# dbReadTable(con, "tweets")
# dbReadTable(con, "hashtags")
# st_read(con, layer = "places") # %>% qtm()
# st_read(con, layer = "points")  %>% qtm()

# Practice Query
res <- dbSendQuery(con, "SELECT * FROM TWEETS LIMIT 10")
dbFetch(res)
dbClearResult(res)
res <- dbSendQuery(con, "SELECT COUNT(*) FROM tweets")
dbFetch(res)
dbClearResult(res)

# Should have 151753255 tweets 
# have about 10 million fewer

# Select top 10 most freqent hashtags and return tweets that contain them
q <- "
SELECT hashtags, COUNT(*)
FROM hashtags
GROUP BY hashtags
ORDER BY count DESC
LIMIT 10"
res <- dbSendQuery(con, q)
dbFetch(res)
dbClearResult(res)
# 
# # # # Remove stuff
# dbRemoveTable(con, "tweets")
# dbRemoveTable(con, "hashtags")
# dbRemoveTable(con, "places")
# dbRemoveTable(con, "points")
# 
