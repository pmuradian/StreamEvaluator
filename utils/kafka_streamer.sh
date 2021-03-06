#!/bin/bash
# Kafka quick setup + stream example execution
# Based on:
#   https://kafka.apache.org/quickstart
#   https://kafka.apache.org/10/documentation/streams/tutorial

SCALA_VERSION="2.11"
KAFKA_VERSION="1.1.1"

directory="kafka_$SCALA_VERSION-$KAFKA_VERSION"
binary_file="$directory.tgz"

cd "$directory"
trap "exit" INT TERM ERR

echo -n "Resetting data directory"
rm -rf "/tmp/zookeeper"
rm -rf "/tmp/kafka-streams"
rm -rf "/tmp/kafka-logs"
echo " done"

echo -n "Starting ZooKeeper"
mkdir -p logs
./bin/zookeeper-server-start.sh config/zookeeper.properties >logs/zookeeper-server.log 2>&1 &
zookeeper_pid=$!
echo " done with pid $zookeeper_pid"

stop_zookeeper () {
  echo -n "Stopping ZooKeeper($zookeeper_pid)"
  kill -TERM "$zookeeper_pid"
  wait "$zookeeper_pid"
  echo " done"
}
trap stop_zookeeper EXIT

sleep 5
echo -n "Starting Kafka"
./bin/kafka-server-start.sh config/server.properties >logs/kafka-server.log 2>&1 &
kafka_pid=$!
echo " done with pid $kafka_pid"

stop_kafka () {
  echo -n "Stopping kafka($kafka_pid)"
  kill -TERM "$kafka_pid"
  wait "$kafka_pid"
  echo " done"
  stop_zookeeper
}
trap stop_kafka EXIT

sleep 5
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo-kv

keep_sending=1
i=0
while [[ "$keep_sending" = "1" ]]; do
  echo "($(( (RANDOM % 1000 ) )),$(( (RANDOM % 10000 ) )),$(( (RANDOM % 10 ) + 1 )))"
  i=$((i+1))
  sleep 1
done | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic demo-kv >/dev/null &
demo_kv_pid=$!

stop_demo_data() {
  kill -TERM "$demo_kv_pid"
  wait "$demo_kv_pid"
  stop_kafka
}

trap stop_demo_data EXIT

echo "Press Ctrl+C to stop the server"
wait