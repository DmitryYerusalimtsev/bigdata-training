USE flights;

CREATE TABLE IF NOT EXISTS flying
(
  Year smallint,
  Month smallint,
  DayofMonth smallint,
  DayOfWeek smallint,
  DepTime smallint,
  CRSDepTime smallint,
  ArrTime smallint,
  CRSArrTime smallint,
  UniqueCarrier char(2),
  FlightNum smallint,
  TailNum string,
  ActualElapsedTime smallint,
  CRSElapsedTime smallint,
  AirTime smallint,
  ArrDelay smallint,
  DepDelay smallint,
  Origin char(3),
  Dest char(3),
  Distance smallint,
  TaxiIn smallint,
  TaxiOut smallint,
  Cancelled boolean,
  CancellationCode char(1),
  Diverted boolean,	
  CarrierDelay smallint,
  WeatherDelay smallint,
  NASDelay smallint,
  SecurityDelay smallint,
  LateAircraftDelay smallint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1")

