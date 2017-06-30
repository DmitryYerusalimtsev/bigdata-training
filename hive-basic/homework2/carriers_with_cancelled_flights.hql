USE flights; 

CREATE TEMPORARY TABLE IF NOT EXISTS cancelled_flights 
( 
  carrier_code string, 
  dep_city string 
); 

INSERT OVERWRITE TABLE cancelled_flights 
SELECT c.code, f.city 
FROM carriers AS c 
JOIN ( 
  SELECT f.uniquecarrier AS carrier_code, a.city 
  FROM flying AS f 
  JOIN airports AS a ON f.origin = a.iata 
  WHERE f.cancelled = 1 -- true 
) AS f ON c.code = f.carrier_code;

SELECT carrier_code AS carrier, 
	   count(*) AS cancelled,
	   concat_ws(", ", collect_set(dep_city)) AS cities 
FROM cancelled_flights 
GROUP BY carrier_code
SORT BY cancelled DESC;