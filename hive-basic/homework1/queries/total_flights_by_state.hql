USE flights;

SELECT a.airport, SUM(f.flights_count) AS flights_count 
FROM airports AS a
JOIN (
	SELECT flying.origin AS iata, 
  		   COUNT(*) AS flights_count
  	FROM flying
    WHERE month = 6 --June
  	GROUP BY flying.origin
  	UNION ALL
  	SELECT flying.dest AS iata,
  		   COUNT(*) AS flights_count
  	FROM flying
    WHERE month = 6 --June
  	GROUP BY flying.dest
) AS f ON a.iata = f.iata
WHERE a.state = "NY"
GROUP BY a.airport;