package exercises;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.UUID;

class Hasher extends DoFn< KV<String, String>, KV<String, String> > {
    @ProcessElement
    public void processElement(@Element KV<String, String> kv, OutputReceiver<KV<String, String>> out) {
        Integer key = this.hash();
        System.out.println("hashing");
        System.out.println(kv.getKey());
        System.out.println(kv.getValue());
        if (key < StreamSampler.samplePercentage) {
            StreamSampler.appendToOutputString(kv);
            if (StreamSampler.getSampleCounter() >= StreamSampler.samplesPerFile) {
                HDFSWriter.writeTo(StreamSampler.getOutputString());
                StreamSampler.reset();
            }
        }
        out.output(kv);
    }

    private Integer hash() {
        return (new Random()).nextInt(StreamSampler.N);
    }
}

class HDFSWriter {

    private final static String hdfsPath = "hdfs://localhost:8020";
    private final static String hdfsName = "fs.defaultFS";
    private final static String samplePath = "hdfs://localhost:8020/user/azazel/";
    private final static String sampleFileName = "_sample.csv";
    private static Integer fileCounter = 0;

    static boolean writeTo(String content) {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set(hdfsName, hdfsPath);
        System.out.println("Getting FileSystem");
        try {
            Path path = new Path( samplePath + StreamSampler.evaluatorID + "_" + fileCounter.toString() + sampleFileName);

            FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(path)) {
                System.out.println("creating file");
                FSDataOutputStream outputStream = fs.create(path);
                System.out.println("file created, writing....");
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, "UTF-8");
                BufferedWriter writer = new BufferedWriter(outputStreamWriter);
                writer.write(content);
                writer.flush();
                outputStreamWriter.flush();
                outputStream.flush();
                writer.close();
                outputStreamWriter.close();
                outputStream.close();
                fs.close();
                System.out.println("Done!");
                fileCounter++;
            } else {
                System.out.println("file exists");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}

public class StreamSampler {
    private static Integer sampleCounter = 0;
    private static String outputString = "";
    public static final Integer N = 100;
    public static final Integer samplePercentage = 100;
    public static final Integer samplesPerFile = 100;
    public static String evaluatorID = UUID.randomUUID().toString();

    public static void main(String[] args) throws IOException {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply(kafkaReader())
                .apply(ParDo.of(new Hasher()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    public static void appendToOutputString(KV<String, String> kv) {
        outputString += kv.getKey() + "," + kv.getValue() + "\n";
        sampleCounter++;
    }

    public static Integer getSampleCounter() {
        return sampleCounter;
    }

    public static String getOutputString() {
        return outputString;
    }

    public static void reset() {
        outputString = "";
        sampleCounter = 0;
    }

    private static PTransform<PBegin,PCollection<KV<String, String>>> kafkaReader() {
        return KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("demo-kv")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata();
    }
}
