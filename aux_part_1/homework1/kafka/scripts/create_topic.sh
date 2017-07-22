cd /usr/hdp/current/kafka-broker/bin/

./kafka-topics.sh --create --zookeeper sandbox.hortonworks.com:2181 --replication-factor 2 --partitions 4 --topic fibonacci

./kafka-topics.sh --zookeeper sandbox.hortonworks.com:2181 --list