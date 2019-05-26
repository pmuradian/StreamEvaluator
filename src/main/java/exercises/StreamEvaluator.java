package exercises;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

//class Hasher extends DoFn< KV<String, String>, KV<String, String> > {
//    @ProcessElement
//    public void processElement(@Element KV<String, String> kv, OutputReceiver<KV<String, String>> out) {
//        System.out.println(kv.getKey() + "*****" + kv.getValue());
//        Integer key = this.hash();
//        if (key < 3) {
//            StreamEvaluator.buckets.get(this.hash()).add(kv);
////            HDFSWriter.writeTo("/bvpu.txt", kv);
//            System.out.println("******************* " + kv.getKey());
//        }
//        out.output(kv);
//    }
//
//    private Integer hash() {
//        return (new Random()).nextInt(StreamEvaluator.N);
//    }
//}

class HDFSWriter {

    final static String hdfsPath = "hdfs://localhost:8020";
    final static String hdfsName = "fs.defaultFS";

    static boolean writeTo(String pathString, String content) {

        Configuration conf = new Configuration();
//        conf.addResource(new Path("file://home/azazel/Documents/hadoop/hadoop-2.8.3/etc/hadoop/core-site.xml"));
//        conf.addResource(new Path("file://home/azazel/Documents/hadoop/hadoop-2.8.3/etc/hadoop/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.")
        conf.set(hdfsName, hdfsPath);
        System.out.println(conf.toString());
        try {
            Path path = new Path(pathString);

            FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(path)) {
                System.out.println("writing bvpu");
                FSDataOutputStream outputStream = fs.create(path);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                writer.write(content);
                writer.close();
                fs.copyFromLocalFile(path, new Path("hdfs://localhost:8020/user/azazel/input/bvpu.txt"));
                fs.close();
                System.out.println("bvpu written");
            } else {
                System.out.println("file exists");
            }
        }
        catch (Exception e) {


            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return true;
    }
}

public class StreamEvaluator {
    // Read streamed data from Kafka and save it to HDFS

//    public static HashMap<Integer, ArrayList<KV<String, String>>> buckets = new HashMap<>();
//    public static final Integer N = 100;
//    private static final Integer samplePercentage = 3;

    public static void main(String[] args) {
        // Implement a sampling algorithm which keeps a random sample of size N of elements seen up to this points.
        // Note that each element should have the same probability of being in the sample. After seeing M elements it should be min(N/M, 1)
//        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
//        Pipeline pipeline = Pipeline.create(pipelineOptions);
//
//        for (int i = 0; i < samplePercentage; i++) {
//            buckets.put(i, new ArrayList<>());
//        }

//        pipeline.apply(kafkaReader())
//                .apply(doubleValue())
//                .apply("ComputeWordLengths",                     // the transform name
//                        ParDo.of(new DoFn<String, String>() {    // a DoFn as an anonymous inner class instance
//                            @ProcessElement
//                            public void processElement(@Element KV<String, String> word, OutputReceiver<KV<String, String>> out) {
//                                out.output(word);
//                            }
//                        }))
//                .apply(kafkaWriter());

//        pipeline.apply(kafkaReader())
//                .apply(ParDo.of(new Hasher()));

        System.out.println("writing to hdfs");
        HDFSWriter.writeTo("hdfs://localhost:8020/user/azazel/input/bvpu.txt", "bvpu");
        System.out.println("write completed");

//        PipelineResult result = pipeline.run();
//        try {
//            result.waitUntilFinish();
//        } catch (Exception exc) {
//            result.cancel();
//        }
    }

//    private static PTransform<PBegin,PCollection<KV<String, String>>> kafkaReader() {
//        return KafkaIO.<String, String>read()
//                .withBootstrapServers("localhost:9092")
//                .withTopic("demo-kv")
//                .withKeyDeserializer(StringDeserializer.class)
//                .withValueDeserializer(StringDeserializer.class)
//                .withoutMetadata();
//    }
}
