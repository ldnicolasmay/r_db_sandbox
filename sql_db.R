# sql_db.R
# Script for trying out connecting to an Azure SQL database (SQL Server 13)

library(DBI)
library(odbc)
library(dplyr)
library(tictoc)
source('config.R')

# Connect to Azure SQL server
sort(unique(odbc::odbcListDrivers()[[1]]))
con <- DBI::dbConnect(odbc::odbc(), 
                      .connection_string = paste0(
                        "Driver={ODBC Driver 13 for SQL Server};
                         Driver={ODBC Driver 13 for SQL Server};
                         Server=tcp:hynso.database.windows.net,1433;
                         Database=mydb;
                         Uid=", uid, "@", uid, ";
                         Pwd=", pwd, ";
                         Encrypt=yes;
                         TrustServerCertificate=no;
                         Connection Timeout=30;"))
# con <- dbConnect(odbc::odbc(),
#                  driver = "ODBC Driver 13 for SQL Server",
#                  server = "tcp:hynso.database.windows.net,1433",
#                  database = "mydb",
#                  uid = "hynso",
#                  pwd = "sigeMund67")
# dbDisconnect(con)


# Set up dplyr interface to 'flights' in the SQL Server
flights_db <- tbl(con, 'flights')


DBI::dbGetInfo(con)

# Get `flights` data
flights <-
  flights::nycflights14(path = "~/Box Sync/Learning/R/CSCAR/data.table/",
                        dir = "flights",
                        verbose = TRUE)
print(object.size(flights), units = 'Mb')

# Write `flights` table to 'mydb' on SQL Server
# DBI::dbWriteTable(conn = con, name = 'flights', value = flights)

# List the tables in 'mydb' connection
DBI::dbListTables(con)
DBI::dbListTables(con)[grepl(pattern = "flights", DBI::dbListTables(con))]
DBI::dbExistsTable(con, "flights")

# List the fields in the 'flights' table
DBI::dbListFields(con, "flights")

# Returns 'flights' table in 'mydb' as a data.frame
# DBI::dbReadTable(con, "flights")

# Run a SQL query on the 'flights' table and return the result all at once
res <- DBI::dbSendQuery(con, 
                        "SELECT * FROM flights WHERE hour = 24;")
DBI::dbFetch(res)
DBI::dbClearResult(res)

# Do the same as above, but return a chunk at a time
res <- DBI::dbSendQuery(con, 
                        "SELECT * FROM flights WHERE hour = 1;")
while(!DBI::dbHasCompleted(res)) {
  chunk <- DBI::dbFetch(res, n = 50)
  print(nrow(chunk))
}
DBI::dbClearResult(res)

# DBI::dbGetQuery (send query, retrieve result, clear result)
hour_24 <- DBI::dbGetQuery(con,
                           "SELECT * FROM flights WHERE hour = 24;")
hour_24

DBI::dbGetQuery(con, 
                "SELECT TOP 5 * FROM flights 
                   WHERE origin = 'JFK' 
                   AND dest = 'LAX';")
DBI::dbGetQuery(con, 
                "SELECT TOP 5 * FROM flights;")


# Learning stuff from w3schools.com ----

# _ Select ----
dbGetQuery(con,
           "SELECT TOP 5 year, month, day, tailnum FROM flights;")
flights_db %>% 
  head(n = 5) %>% 
  select(year, month, day, tailnum)
flights %>% 
  head(n = 5) %>% 
  select(year, month, day, tailnum)

# _ Select Distinct ----
dbGetQuery(con,
           "SELECT DISTINCT carrier FROM flights;")
flights_db %>% 
  distinct(carrier) %>% print(n = 20)
flights %>% 
  distinct(carrier)

# _ Select Distinct + Count ----
dbGetQuery(con,
           "SELECT COUNT (DISTINCT carrier) FROM flights;")
flights_db %>% 
  summarize(n = n_distinct(carrier))
flights %>% 
  summarize(n = n_distinct(carrier))

# _ Where ----

# _ _ Operators in the WHERE clause: ----
#     =        equal
#     <>       not equal (some SQL versions: !=)
#     >        gt
#     <        lt
#     >=       gte
#     <=       lte
#     BETWEEN  between a certain range
#     LIKE     search for a pattern
#     IN       specify multiple possible values for a column

# _ _ _ = equal ----
dbGetQuery(con,
           "SELECT * FROM flights
            WHERE hour = 1;")
flights_db %>% 
  filter(hour == 1L)
flights %>% 
  filter(hour == 1L)

dbGetQuery(con,
           "SELECT * FROM flights
            WHERE dest = 'ANC';")
flights_db %>% 
  filter(dest == 'ANC')
flights %>% 
  filter(dest == 'ANC')

# _ _ _ <> or != ----
dbGetQuery(con,
           "SELECT * FROM flights
            WHERE origin <> 'JFK';") %>% nrow()
# dbGetQuery(con,                     # this works the same as above
#            "SELECT * FROM flights
#            WHERE origin != 'JFK';")
flights_db %>% 
  filter(origin != 'JFK') %>% collect() %>% nrow()

# _ _ _ >  ----
tic(); dbGetQuery(con,
           "SELECT year, month, day, dep_delay FROM flights
            WHERE dep_delay > 1000;"); toc()
tic(); flights_db %>% 
  select(year, month, day, dep_delay) %>% 
  filter(dep_delay > 1000L); toc()

# _ _ _ BETWEEN ----
tic(); dbGetQuery(con,
           "SELECT year, month, day, arr_delay, tailnum FROM flights
            WHERE arr_delay BETWEEN 950 AND 1050;"); toc()
tic(); flights_db %>% 
  select(year, month, day, arr_delay, tailnum) %>% 
  filter(between(arr_delay, 950L, 1050L)); toc()

# _ _ _ LIKE ----
# % is zero or multi-character wildcard, regex *
# _ is single-single character wildcard, regex .
dbGetQuery(con,
           "SELECT year, month, day, tailnum FROM flights
            WHERE tailnum LIKE 'N113%';") 
flights_db %>% 
  select(year, month, day, tailnum) %>% 
  filter(tailnum %like% 'N113%') # %like% takes SQL wildcards % and _

dbGetQuery(con,
           "SELECT year, month, day, tailnum FROM flights
            WHERE tailnum LIKE '%3SY';")
flights_db %>% 
  select(year, month, day, tailnum) %>% 
  filter(tailnum %like% '%3SY')

dbGetQuery(con,
           "SELECT year, month, day, tailnum FROM flights
            WHERE tailnum LIKE '%08S%';")

dbGetQuery(con,
           "SELECT year, month, day, tailnum FROM flights
            WHERE tailnum LIKE 'N7_8SW';")
flights_db %>% 
  select(year, month, day, tailnum) %>% 
  filter(tailnum %like% 'N7_8SW')

# _ _ _ IN ----
dbGetQuery(con,
           "SELECT year, month, day, tailnum, origin, dest FROM flights
            WHERE dest IN ('ANC', 'AGS');")
flights_db %>% 
  select(year, month, day, tailnum, origin, dest) %>% 
  filter(dest %in% c('ANC', 'AGS')) %>% collect()


# _ AND, OR, NOT Operators -----

# _ _ AND ----
dbGetQuery(con,
           "SELECT year, month, day, dep_delay, arr_delay, origin, dest 
            FROM flights
            WHERE dep_delay >= 1000 AND arr_delay > 1000;")
flights_db %>% 
  select(year, month, day, dep_delay, arr_delay, origin, dest) %>% 
  filter(dep_delay >= 1000L && arr_delay >= 1000L)

# _ _ OR ----
dbGetQuery(con,
           "SELECT year, month, day, dep_delay, arr_delay, origin, dest 
            FROM flights
            WHERE dep_delay >= 1000 OR arr_delay > 1000;")
flights_db %>% 
  select(year, month, day, dep_delay, arr_delay, origin, dest) %>% 
  filter(dep_delay >= 1000L || arr_delay >= 1000L)

# _ _ NOT ----
blah <- dbGetQuery(con,
           "SELECT year, month, day, dep_delay, arr_delay, origin, dest 
            FROM flights
            WHERE NOT month = 1;") 
blah <- flights_db %>% 
  select(year, month, day, dep_delay, arr_delay, origin, dest) %>% 
  filter(month != 1L) %>% collect()
rm(blah)

# _ _ Combining AND, OR, NOT ----
dbGetQuery(con,
           "SELECT year, month, day, dep_delay, arr_delay, origin, dest
           FROM flights
           WHERE origin = 'LGA'                         -- LaGuardia
           AND (dest = 'MEM' OR dest = 'AGS') 
           AND NOT dep_delay IS NULL;")
dbGetQuery(con,
           "SELECT year, month, day, dep_delay, arr_delay, origin, dest
           FROM flights
           WHERE NOT (origin = 'EWR' OR origin = 'JFK')  -- same as LaGuardia
           AND (dest = 'MEM' OR dest = 'AGS') 
           AND NOT dep_delay IS NULL;")

# # Discovery
# flights_db %>%
#   select(origin, dest) %>%
#   group_by(origin, dest) %>%
#   summarize(n = n()) %>%
#   arrange(n)
dbGetQuery(con,
           "SELECT TOP 10 origin, dest, COUNT(origin) -- or COUNT(dest)
           FROM flights
           GROUP BY origin, dest
           ORDER BY COUNT(origin) ASC;                -- or COUNT(dest)")   


# 

# RStudio tutorials
# Safe queries avoiding SQL injection - Parameterized queries 




# Disconnect the database connection
dbDisconnect(con)