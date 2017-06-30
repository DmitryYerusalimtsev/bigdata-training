USE flights;

SELECT c.description AS carrier, f.flights_count AS flights_count 
FROM carriers AS c
JOIN (
	SELECT uniquecarrier, 
  		   COUNT(*) AS flights_count
  	FROM flying
  	GROUP BY uniquecarrier
) AS f ON c.code = f.uniquecarrier
SORT BY flights_count DESC
LIMIT 1