USE access_logs;

CREATE TABLE IF NOT EXISTS logs (
  request string
)
ROW FORMAT DELIMITED
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;