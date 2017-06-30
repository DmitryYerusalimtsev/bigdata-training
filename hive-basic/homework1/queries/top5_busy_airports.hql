USE flights;

SELECT a.airport, SUM(f.flights_count) AS flights_count 
FROM airports AS a
JOIN (
	SELECT flying.origin AS iata, 
  		   COUNT(*) AS flights_count
  	FROM flying
    	WHERE month in (6, 7, 8) --June, July, August
  	GROUP BY flying.origin
  	UNION ALL
  	SELECT flying.dest AS iata,
  		   COUNT(*) AS flights_count
  	FROM flying
    	WHERE month in (6, 7, 8) --June, July, August
  	GROUP BY flying.dest
) AS f ON a.iata = f.iata
WHERE a.country = "USA"
GROUP BY a.airport
SORT BY flights_count DESC
LIMIT 5