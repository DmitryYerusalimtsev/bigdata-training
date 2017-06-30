USE flights;

SELECT uniquecarrier, COUNT(*) AS flights_count 
FROM flying 
GROUP BY uniquecarrier;