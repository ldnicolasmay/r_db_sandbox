# SQL upload experimentation

library(DBI)
library(odbc)
library(RSQLite)
library(dplyr)
library(nycflights13)

head(flights)
head(airlines)
head(airports)
head(planes)
head(weather)

str(flights)
# max_nchar_flights <- purrr::map_int(flights, 
#                                     ~ {max(nchar(.x), na.rm = TRUE)})
max_nchar_flights

nyc_fl_db <- dbConnect(drv = SQLite())
dbExecute(nyc_fl_db,
          "CREATE TABLE flights (
            id INTEGER,
            year INTEGER(4),
            month INTEGER(2),
            day INTEGER(2),
            dep_time INTEGER(4),
            sched_dep_time INTEGER(4),
            dep_delay DOUBLE,
            arr_time INTEGER(4),
            sched_arr_time INTEGER(4),
            arr_delay DOUBLE,
            carrier TEXT(2),
            flight INTEGER(4),
            tailnum TEXT(6),
            origin TEXT(3),
            dest TEXT(3),
            air_time DOUBLE,
            distance DOUBLE,
            hour DOUBLE,
            minute DOUBLE,
            time_hour DATETIME
          );")
# dbExecute(nyc_fl_db,
#           "DROP TABLE flights;")
dbGetQuery(nyc_fl_db,
           "SELECT * FROM flights;")
flights <- nycflights13::flights
flights <- flights %>% 
  mutate(id = row_number()) %>% 
  select(id, everything())
head(flights)
dbWriteTable(nyc_fl_db, 'flights', flights, append = TRUE)
dbGetQuery(nyc_fl_db,
           "SELECT * 
             FROM flights 
             WHERE id BETWEEN 336767 AND 336776;")










