#!/bin/bash
./kafka_2.11-1.1.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $1 --property print.key=true
