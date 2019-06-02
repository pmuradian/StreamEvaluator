1. Overview
This project contains streaming examples and a StreamSampler project in Beam framework.

To start kafka locally and populate it with topics and sample data simply run script `./kafka_demo.sh` from `utils`
directory. This will download kafka into `utils` directory and start a kafka streamer. After doing this once, use `./kafka_streamer.sh` script from `utils`.
This will skip downloading and unpacking kafka.

2. StreamSampler project can be run with: `run_stream_sampler.sh` in main directory

3. Each example and exercise can be run with:
```
mvn compile exec:java -Dexec.mainClass=CLASS -Pdirect-runner
```
where CLASS should be mvn compile exec:java -Dexec.mainClass=CLASS -Pdirect-runnereplaced with class name with main you would like to run. For example `exercises.Example01Pipe`.

Useful links:
Beam programming model: https://beam.apache.org/documentation/programming-guide/