package exercises;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;

public class Exercise01Average {
    // Implement a pipeline reading numbers from demo-kv topic and returning a sliding average.
    // After each element return average of last 10 elements

    public static void main(String[] args) throws IOException {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply(kafkaReader())
                .apply(doubleValue())
                .apply(kafkaWriter());

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    private static PTransform<PBegin, PCollection<KV<String, String>>> kafkaReader() {
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

    private static MapElements<KV<String, String>, KV<String, String>> doubleValue() {
        return MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((KV<String, String> kv) -> KV.of(kv.getKey(), "" + Long.parseLong(kv.getValue()) * 2));
    }
}
