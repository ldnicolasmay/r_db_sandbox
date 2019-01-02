# airontime_sql_db.R

library(DBI)
library(odbc)
library(dplyr)
library(data.table)
library(lubridate)
library(ggplot2)
library(dbplot)
library(tidypredict)
source('config_aot.R')

# # Get `nycflights13` data tables ----
# # install.packages('nycflights13')
airlines13 <- nycflights13::airlines
airports13 <- nycflights13::airports
flights13  <- nycflights13::flights
planes13   <- nycflights13::planes
weather13  <- nycflights13::weather

connect_to_db <- TRUE

# Connect to Azure SQL server ----
sort(unique(odbc::odbcListDrivers()[[1]]))
if (connect_to_db) {
  if ("ODBC Driver 17 for SQL Server" %in% 
      sort(unique(odbc::odbcListDrivers()[[1]]))) {
    con <- DBI::dbConnect(odbc::odbc(), 
                          .connection_string = con_string_aot_sql17)
  }
  else if ("ODBC Driver 13 for SQL Server" %in% 
           sort(unique(odbc::odbcListDrivers()[[1]]))) {
    con <- DBI::dbConnect(odbc::odbc(), 
                          .connection_string = con_string_aot_sql13)
  } 
}

DBI::dbListTables(con)[1:10]
DBI::dbListFields(con, 'aot_data')

# Single file name retrieval by year
# aot_1988_files <- list.files('~/Downloads/AirOnTimeCSV/', 
#                              pattern = '^airOT1988.*.csv$')
# Batch file name retrieval by year
aot_files <- lapply(as.character(1988:2012),
                    FUN = function(x) {
                      list.files('~/Downloads/AirOnTimeCSV/',
                                 pattern = paste0('^airOT', x, '.*.csv$'))
                    })
aot_files
start_year <- 1988
aot_files[[1988-start_year+1]]
aot_files[[2012-start_year+1]]

# READ DATA BY YEAR, PUSH TO SQL SERVER ---- 

aot_data_by_year <- function(year) {
  start_year = 1988
  rbindlist(
    lapply(paste0('~/Downloads/AirOnTimeCSV/', 
                  aot_files[[year-start_year+1]]), 
           FUN = fread, 
           sep = ',')
  )
}

print_size <- function(x) {
  print(object.size(x), units = 'Mb')
}

# # _ 20XX ----
# aot_data_20XX <- aot_data_by_year(20XX)
# str(aot_data_20XX)
# print_size(aot_data_20XX)

# _ 2012 ----
aot_data_2012 <- aot_data_by_year(2012)
str(aot_data_2012)
print_size(aot_data_2012)
# Write aot_data_2012 to 'aot' SQL database as 'aot_2012' table
DBI::dbWriteTable(con, name = 'aot_2012', value = aot_data_2012)
rm(aot_data_2012)

# _ 2011 ----
aot_data_2011 <- aot_data_by_year(2011)
str(aot_data_2011)
print_size(aot_data_2011)
# Append aot_data_2011 to 'aot_2012' SQL table (written above)
DBI::dbWriteTable(con, name = 'aot_2012', value = aot_data_2011, append = TRUE)
rm(aot_data_2011)

# Rename the table from 'aot_2012' to 'aot_data'
DBI::dbGetQuery(con, 'EXEC sp_rename aot_2012, aot_data')
DBI::dbListTables(con)[1:10]

# _ 2010 ----
aot_data_2010 <- aot_data_by_year(2010)
str(aot_data_2010)
print_size(aot_data_2010)
# Append aot_data_2011 to 'aot_data' SQL table (written above)
DBI::dbWriteTable(con, name = 'aot_data', value = aot_data_2010, append = TRUE)

# _ 2009 ----
aot_data_2009 <- aot_data_by_year(2009)
str(aot_data_2009)
print_size(aot_data_2009)
# DBI::dbWriteTable(con, name = 'aot_data', value = aot_data_2009, append = TRUE)

# _ 2008 ----
aot_data_2008 <- aot_data_by_year(2008)
str(aot_data_2008)
print_size(aot_data_2008)
# DBI::dbWriteTable(con, name = 'aot_data', value = aot_data_2008, append = TRUE)

# _ 2007 ----
aot_data_2007 <- aot_data_by_year(2007)
str(aot_data_2007)
print_size(aot_data_2007)
# DBI::dbWriteTable(con, name = 'aot_data', value = aot_data_2007, append = TRUE)

# ...
# ...
# ...

# _ 1990 ----
aot_data_1990 <- aot_data_by_year(1990)
str(aot_data_1990)
print_size(aot_data_1990)
# DBI::dbWriteTable(con, name = 'aot_1990', value = aot_data_1990)

# _ 1989 ----
aot_data_1989 <- aot_data_by_year(1989)
str(aot_data_1989)
print_size(aot_data_1989)
# DBI::dbWriteTable(con, name = 'aot_1989', value = aot_data_1989)

# _ 1988 ----
aot_data_1988 <- aot_data_by_year(1988)
str(aot_data_1988)
print_size(aot_data_1988)
# DBI::dbWriteTable(con, name = 'aot_1988', value = aot_data_1988)


# dplyr summaries
aot_data_db <- tbl(con, 'aot_data')
aot_data_db %>% 
  summarize(n = n())
dbGetQuery(con, 'SELECT COUNT(*) FROM aot_data;')

year_carr_n <- aot_data_db %>% 
  group_by(YEAR, UNIQUE_CARRIER) %>% 
  summarize(n = n()) %>% 
  collect()
targ_airlines <- c('AA', 'DL', 'UA', 'US', 'WN')
year_carr_n %>% 
  left_join(airlines13, by = c('UNIQUE_CARRIER' = 'carrier')) %>%
  filter(UNIQUE_CARRIER %in% targ_airlines) %>% 
  ggplot(aes(x = YEAR, y = n, col = name)) + 
  geom_line()

y_m_d_carr_n <- aot_data_db %>% 
  group_by(YEAR, MONTH, DAY_OF_MONTH, UNIQUE_CARRIER) %>% 
  summarize(n = n()) %>% 
  collect()
ymd_carr_n <- y_m_d_carr_n %>% 
  ungroup() %>% 
  mutate(date = as.Date(paste0(YEAR, '-', MONTH, '-', DAY_OF_MONTH))) %>% 
  select(UNIQUE_CARRIER, date, n)
ymd_carr_n %>%
  left_join(airlines13, by = c('UNIQUE_CARRIER' = 'carrier')) %>% 
  filter(UNIQUE_CARRIER %in% targ_airlines) %>% 
  ggplot(aes(x = date, y = n, col = name)) +
  geom_line()

# Y_M_D_ORIG
y_m_d_orig_n <- aot_data_db %>% 
  group_by(YEAR, MONTH, DAY_OF_MONTH, ORIGIN) %>% 
  summarize(n = n()) %>% 
  collect()
# YMD_ORIG
ymd_orig_n <- y_m_d_orig_n %>% 
  ungroup() %>% 
  mutate(date = as.Date(paste0(YEAR, '-', MONTH, '-', DAY_OF_MONTH))) %>% 
  select(ORIGIN, date, n)
targ_airports <- ymd_orig_n %>% 
  group_by(ORIGIN) %>% 
  summarize(sum_n = sum(n)) %>% 
  arrange(desc(sum_n)) %>% 
  head(5) %>% 
  pull(ORIGIN)
ymd_orig_n %>% 
  filter(ORIGIN %in% targ_airports) %>% 
  left_join(airports13, by = c('ORIGIN' = 'faa')) %>% 
  ggplot(aes(x = date, y = n, col = name)) + 
  geom_line()
# YMD_W_ORIG
ymd_w_orig_n <- y_m_d_orig_n %>% 
  ungroup() %>% 
  mutate(date = as.Date(paste0(YEAR, '-', MONTH, '-', DAY_OF_MONTH)),
         cum_week = (interval(min(date), date) %/% weeks(1)) + 1) %>%
  select(ORIGIN, cum_week, n) %>% 
  group_by(ORIGIN, cum_week) %>% 
  summarize(cum_week_n = sum(n))
max_week <- max(ymd_w_orig_n$cum_week)
ymd_w_orig_n %>% 
  filter(ORIGIN %in% targ_airports) %>% 
  left_join(airports13, by = c('ORIGIN' = 'faa')) %>% 
  ggplot(aes(x = cum_week, y = cum_week_n, col = name)) +
  geom_line() +
  scale_x_continuous(breaks = seq(1, max_week, by = 52), 
                     limits = c(1,max_week-1))


y_m_d_dest_n <- aot_data_db %>% 
  group_by(YEAR, MONTH, DAY_OF_MONTH, DEST) %>% 
  summarize(n = n()) %>% 
  collect()

y_m_d_orig_dest_n <- aot_data_db %>% 
  group_by(YEAR, MONTH, DAY_OF_MONTH, ORIGIN, DEST) %>% 
  summarize(n = n()) %>% 
  collect()








# # TEST ROW BINDS ----
# 
# # Bind 1988-1990
# aot_data_1988__1990 <- rbind(aot_data_1988, aot_data_1989, aot_data_1990)
# class(aot_data_1988__1990)
# str(aot_data_1988__1990)
# print(object.size(aot_data_1988__1990), units = 'Gb')
# rm(aot_data_1988__1990)
# rm(aot_data_1988); rm(aot_data_1989); rm(aot_data_1990)
# 
# # Bind 1988, 2011, 2012
# aot_data_1988_2011_2012 <- rbind(aot_data_1988, aot_data_2011, aot_data_2012)
# str(aot_data_1988_2011_2012)
# print_size(aot_data_1988_2011_2012)
# 
# # Bind 2009-2012
# aot_data_2009__2012 <- rbind(# aot_data_2009, 
#                              aot_data_2010,
#                              aot_data_2011, 
#                              aot_data_2012)

# dbGetQuery(con, 'DROP TABLE aot_1988')

# Disconnect the database connection
dbDisconnect(con)










