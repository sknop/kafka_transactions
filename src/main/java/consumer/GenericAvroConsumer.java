package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import picocli.CommandLine;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

@CommandLine.Command(name = "GenericAvroConsumer",
        version = "GenericAvroConsumer 1.0",
        description = "Reads any AVRO messages from a topic")
public class GenericAvroConsumer extends AbstractBaseConsumer<Integer, GenericRecord> {
    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the customers (default = ${DEFAULT-VALUE})")
    private String topic = "customer";

    @CommandLine.Option(names = {"--single"},
            description = "Commit each message (default = false)")
    private boolean single = false;

    @CommandLine.Option(names = {"--asynch"},
            description = "Commit asynchronously (default = ${DEFAULT-VALUE})")
    private boolean async = false;
    final private Duration duration = Duration.ofMillis(10000);

    @Override
    protected Collection<String> getTopicsList() {
        return Collections.singletonList(topic);
    }

    @Override
    protected int consumeBatch(KafkaConsumer<Integer, GenericRecord> consumer) {
        ConsumerRecords<Integer, GenericRecord> records = consumer.poll(duration);
        System.out.printf("*** Batch size %d%n", records.count());
        for (ConsumerRecord<Integer, GenericRecord> record : records) {
            Schema writerSchema = record.value().getSchema();

            System.out.printf("Found \"%s\" [%d] %s%n", writerSchema.getFullName(), record.key(), record.value().toString());
            if (single) {
                commitMessages(consumer);
            }
        }
        if (!single) {
            commitMessages(consumer);
        }

        return records.count();
    }

    private void commitMessages(KafkaConsumer<Integer, GenericRecord> consumer) {
        if (async) {
            consumer.commitAsync((offsets, exception) -> System.out.println(offsets));
        }
        else {
            consumer.commitSync();
        }
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "genericAvroConsumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        super.addProperties(properties);
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new GenericAvroConsumer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
