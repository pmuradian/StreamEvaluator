1. Overview
This project contains streaming examples and exercises in Beam framework.

Each pipe works with one on more streams read from Kafka. To start kafka locally and populate it with topics and sample data simply run script `./kafka_demo.sh` from `utils`
directory.

Each example and exercise can be run with:
```
mvn compile exec:java -Dexec.mainClass=CLASS -Pdirect-runner
```

where CLASS should be mvn compile exec:java -Dexec.mainClass=CLASS -Pdirect-runnereplaced with class name with main you would like to run. For example `exercises.Example01Pipe`.

Useful links:
Beam programming model: https://beam.apache.org/documentation/programming-guide/