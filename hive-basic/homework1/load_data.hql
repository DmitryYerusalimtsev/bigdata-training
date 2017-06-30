USE flights;

LOAD DATA INPATH '/dima/hive-basics/homework1/data/carriers.csv' OVERWRITE INTO TABLE carriers;
LOAD DATA INPATH '/dima/hive-basics/homework1/data/airports.csv' OVERWRITE INTO TABLE airports;
LOAD DATA INPATH '/dima/hive-basics/homework1/data/2007.csv' OVERWRITE INTO TABLE flying;