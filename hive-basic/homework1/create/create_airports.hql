USE flights;

CREATE TABLE IF NOT EXISTS airports
(
  iata char(3),
  airport string,
  country varchar(3), 
  state char(2),
  city string,
  lat float,
  long float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1")