library(RPostgreSQL)

## https://www.r-bloggers.com/getting-started-with-postgresql-in-r/
# install.packages("RPostgreSQL")
library("RPostgreSQL")

## see library DBI for db methods

# create a connection
# save the password that we can "hide" it as best as we can by collapsing it
## keyfitz has cluster instance 'ethan' running on port 5433 
## user:/ pw  eroubenoff / Mar1ach1  authenticating on localhost connection
## [ NB 'peer' default connection requires user to be eroubenoff, ident from kernel lookup]

pw <- {
  "Mar1ach1"
}

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")  # see max.con, fetch.default.rec args; ?PostgreSQL
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "ethandb",
                 host = "localhost", port = 5433,
                 user = "eroubenoff", password = pw)
#rm(pw) # removes the password
dbListTables(con)

# check for the cartable
dbExistsTable(con, "cartable")
# FALSE

# creates df, a data.frame with the necessary columns
data(mtcars)
df <- data.frame(carname = rownames(mtcars), 
                 mtcars, 
                 row.names = NULL)
df$carname <- as.character(df$carname)
rm(mtcars)

# writes df to the PostgreSQL database "postgres", table "cartable" 
dbWriteTable(con, "cartable", 
             value = df, append = TRUE, row.names = FALSE)

# query the data from postgreSQL 
df_postgres <- dbGetQuery(con, "SELECT * from cartable")

# compares the two data.frames
identical(df, df_postgres)
# TRUE

# Basic Graph of the Data
require(ggplot2)
ggplot(df_postgres, aes(x = as.factor(cyl), y = mpg, fill = as.factor(cyl))) + 
  geom_boxplot() + theme_bw()


dbListTables(con)
dbRemoveTable(con, "cartable")
dbDisconnect(con)


## db.rstudio.com  example
## https://github.com/r-dbi/RPostgres

library(DBI)
# Connect to the default postgres database
con <- dbConnect(RPostgres::Postgres(), dbname = "ethandb",
                 host = "localhost", port = 5433,
                 user = "eroubenoff", password = pw)
data(mtcars)
dbListTables(con)
dbWriteTable(con, "mtcars", mtcars)
dbListTables(con)

dbListFields(con, "mtcars")
dbReadTable(con, "mtcars")


# You can fetch all results:
res <- dbSendQuery(con, "SELECT * FROM mtcars WHERE cyl = 4")
dbFetch(res)
dbClearResult(res)

# Or a chunk at a time
res <- dbSendQuery(con, "SELECT * FROM mtcars WHERE cyl = 4")
while(!dbHasCompleted(res)){
  chunk <- dbFetch(res, n = 5)
  print(nrow(chunk))
}
# Clear the result
dbClearResult(res)

# drop table
dbRemoveTable(con, "mtcars")

# Disconnect from the database
dbDisconnect(con)
