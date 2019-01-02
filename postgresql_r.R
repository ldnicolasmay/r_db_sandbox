# postgresql_r.R

library(DBI)
library(odbc)
library(RPostgreSQL)

# "PostgreSQL" DNS set in:
#   /usr/local/etc/odbc.ini
#   /usr/local/etc/odbcinst.ini
# ... OR ...:
#   /etc/odbcinst.ini
#   ~/.odbc.ini
con <- DBI::dbConnect(odbc::odbc(), "PostgreSQL AWS dvdrental")

# db_connect <- function(){
#   DBI::dbConnect(odbc::odbc(),
#                  Driver     = "PostgreSQL",
#                  servername = server_ip_or_server_dns,
#                  database   = "mydb",
#                  UID        = rstudioapi::askForPassword("Database user"),
#                  PWD        = rstudioapi::askForPassword("Database password"),
#                  Port       = 5432)
# }
# con <- db_connect()

dbListTables(con)

dbGetQuery(con,
           "SELECT * FROM actor;")
dbGetQuery(con,
           "SELECT * 
             FROM actor
             WHERE first_name LIKE 'P%';")
dbGetQuery(con,
           "SELECT * 
             FROM actor
             WHERE last_name LIKE 'P%';")



dbDisconnect(con)
