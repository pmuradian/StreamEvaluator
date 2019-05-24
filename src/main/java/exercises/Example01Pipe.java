package exercises;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;

public class Example01Pipe {
    public static void main(String[] args) throws IOException {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply(kafkaReader())
                .apply(kafkaWriter());

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    private static PTransform<PBegin,PCollection<KV<String, String>>> kafkaReader() {
        return KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("demo-kv")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata();
    }

    private static KafkaIO.Write<String, String> kafkaWriter() {
        return KafkaIO.<String, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("demo-kv-output")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class);
    }
}
