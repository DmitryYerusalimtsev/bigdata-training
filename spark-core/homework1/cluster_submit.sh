#!/bin/bash 

USER=$1

if [ -z ${USER} ]; then
    echo "Please provide your HDFS user name"
    exit 1
fi 

spark-submit --master yarn-client --driver-memory 2g --num-executors 3 --executor-memory 8g --conf spark.executor.cores=5 spark-core-homework-scala-1.0.0-hadoop.jar /common/training/motels.home/bids /common/training/motels.home/motels /common/training/motels.home/exchange_rates /user/$USER/spark-core-output
