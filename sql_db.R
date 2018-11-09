# sql_db.R
# Script for trying out connecting to an Azure SQL database (SQL Server 13)

library(DBI)
library(odbc)
library(dplyr)
library(tictoc)
library(ggplot2)
source('config.R')

# Connect to Azure SQL server ----
sort(unique(odbc::odbcListDrivers()[[1]]))
con <- DBI::dbConnect(odbc::odbc(), 
                      .connection_string = con_string)
# dbDisconnect(con)

# Get `flights` data ----
# flights <-
#   flights::nycflights14(path = "~/Box Sync/Learning/R/CSCAR/data.table/",
#                         dir = "flights",
#                         verbose = TRUE)
# print(object.size(flights), units = 'Mb')


# # Get `nycflights13` data tables ----
# # install.packages('nycflights13')
# airlines13 <- nycflights13::airlines
# airports13 <- nycflights13::airports
# flights13  <- nycflights13::flights
# planes13   <- nycflights13::planes
# weather13  <- nycflights13::weather


# Write `flights` table to 'mydb' on SQL Server ----
# DBI::dbWriteTable(conn = con, name = 'flights', value = flights)
# Write `nycflights13` data tables
# DBI::dbWriteTable(con, 'airlines13', airlines13)
# DBI::dbWriteTable(con, 'airports13', airports13)
# DBI::dbWriteTable(con, 'flights13', flights13)
# DBI::dbWriteTable(con, 'planes13', planes13)
# DBI::dbWriteTable(con, 'weather13', weather13)


# Set up dplyr interfaces ----
# _ Full flights data tables
flights_db <- tbl(con, 'flights')
# _ nycflights13 data tables 
# airlines13_db <- tbl(con, dbplyr::in_schema('dbo', 'airlines13'))
# glimpse(airlines13_db)
airlines13_db <- tbl(con, 'airlines13')
airports13_db <- tbl(con, 'airports13')
flights13_db  <- tbl(con, 'flights13')
planes13_db   <- tbl(con, 'planes13')
weather13_db  <- tbl(con, 'weather13')

# 
DBI::dbGetInfo(con)

# List the tables in 'mydb' connection
DBI::dbListObjects(con)
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

# Discovery
flights_db %>%
  select(origin, dest) %>%
  group_by(origin, dest) %>%
  summarize(n = n()) %>%
  arrange(n)
dbGetQuery(con,
           "SELECT TOP 10 origin, dest, COUNT(*)
           FROM flights
           GROUP BY origin, dest
           ORDER BY COUNT(*) ASC;")   

# _ ORDER BY ----
dbGetQuery(con,
           "SELECT TOP 10 year, month, day, dep_delay, arr_delay, origin, dest
           FROM flights
           WHERE NOT dep_delay < 1000 AND NOT arr_delay < 1000
           ORDER BY dep_delay ASC;")
flights_db %>% 
  select(year, month, day, dep_delay, arr_delay, origin, dest) %>% 
  filter(!(dep_delay < 1000L) && !(arr_delay < 1000L)) %>% 
  arrange(dep_delay) %>% 
  head(10)

dbGetQuery(con,
           "SELECT TOP 10 year, month, day, dep_delay, arr_delay, origin, dest
           FROM flights
           WHERE NOT dep_delay < 1000 AND NOT arr_delay < 1000
           ORDER BY arr_delay DESC;")
flights_db %>% 
  select(year, month, day, dep_delay, arr_delay, origin, dest) %>% 
  filter(!(dep_delay < 1000L) && !(arr_delay < 1000L)) %>% 
  arrange(desc(arr_delay)) %>% 
  head(10)


# RStudio webinar
# Best practices for working with databases

# 1. Ideally, analyze in-place using SQL engine
# "total sales by month" => "total # flights cancelled by month"
dbGetQuery(con,
           "SELECT month, SUM(cancelled) AS sum_canc
           FROM flights
           GROUP BY month
           ORDER BY sum_canc DESC;") %>% tibble::as_tibble()
flights_db %>% 
  select(month, cancelled) %>% 
  group_by(month) %>% 
  summarize(sum_canc = sum(cancelled, na.rm = TRUE)) %>% 
  arrange(desc(sum_canc)) %>% collect() %>% print(n = 12)

# "# of sales over $1K by month" => "# of dep delays over 1000 min by month"
dbGetQuery(con,
           "SELECT month, COUNT(dep_delay) AS dep_delay_n
           FROM flights
           WHERE dep_delay > 500
           GROUP BY month
           ORDER BY month ASC;")
flights_db %>% 
  select(month, dep_delay) %>% 
  filter(dep_delay > 500L) %>% 
  group_by(month) %>% 
  summarize(dep_delay_n = n()) %>% 
  arrange(month) %>% collect() %>% print(n = 12)

# 2. Use 'Connections' pane in RStudio ========>
#    - Preview structure of SQL data (using dropdown arrows)
#    - Preview first 1000 records of a table (little table icon)
#    - Refresh connections (refresh icon, circle arrow top right)
#    - Manage connections
#    - Open SQL document use SQL icon
#    - Disconnect from a (SQL) connection

#
dbGetQuery(con,
           "SELECT origin, dest, COUNT(*) AS flight_n
           FROM flights
           GROUP BY origin, dest;")

# Continuing with dplyr
flights_db %>% 
  group_by(year, month) %>% 
  tally() %>% 
  arrange(year, month) %>% collect() %>% print(n = nrow(.))
flights13_db %>% 
  group_by(year, month) %>% 
  tally() %>% 
  arrange(year, month) %>% collect() %>% print(n = nrow(.))

flights_db %>% 
  group_by(year, month) %>% 
  summarize(n_flights = n(),
            avg_dep_delay = mean(dep_delay, na.rm = TRUE),
            avg_arr_delay = mean(arr_delay, na.rm = TRUE)) %>% 
  arrange(year, month) %>% collect() %>% print(n = nrow(.))

# dplyr joins
glimpse(flights13_db)
glimpse(airports13_db)
# dplyr joins create a pointer to a SQL query 
flights13_airports13_db <- flights13_db %>% 
  inner_join(airports13_db, by = c('dest' = 'faa')) # match dest to faa code!
glimpse(flights13_airports13_db)
head(flights13_airports13_db)
# _ Top 10 destination airports by name
flights13_airports13_db %>% 
  group_by(name) %>% 
  tally() %>% 
  arrange(desc(n)) %>% 
  head(10) # %>% 
  # show_query()

# Visualizations
# _ dplyr::collect() => ggplot
t <- flights13_airports13_db %>% 
  group_by(name) %>% 
  tally() %>% 
  arrange(desc(n)) %>% 
  head(12) %>% 
  collect()
ggplot(t) +
  geom_col(aes(x = name, y = n)) +
  coord_flip()
# _ skip collect()
flights13_airports13_db %>% 
  group_by(lon, lat) %>% 
  tally() %>% 
  select(n, lon, lat) %>% 
  collect() %>% 
  ggplot() +
  geom_point(aes(x = lon, y = lat, size = n, color = n), alpha = 0.3)

# dbplot
library(dbplot)

flights_db %>% 
  dbplot_line(month)
flights_db %>% 
  dbplot_line(month, mean(dep_delay, na.rm = TRUE)) +
  scale_x_continuous(breaks = seq(1, 12, by = 1))

flights_db %>% 
  # mutate(hour = if_else(hour == 24L, 0L, hour)) %>% 
  dbplot_histogram(hour)





# RStudio tutorials
# Safe queries avoiding SQL injection - Parameterized queries 





# Disconnect the database connection
dbDisconnect(con)
