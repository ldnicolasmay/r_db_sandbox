-- !preview conn=world_db

-- notice that the connection (conn) is set to world_db above

SELECT Name, Continent, Region 
  FROM Country 
  LIMIT 10;
