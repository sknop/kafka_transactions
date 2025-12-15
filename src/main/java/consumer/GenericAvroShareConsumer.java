package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import picocli.CommandLine;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

// To use this consumer, you need to enable queues first
//
// kafka-features --bootstrap-server localhost:9092 upgrade --feature share.version=1

@CommandLine.Command(name = "GenericAvroSharedConsumer",
        version = "GenericAvroSharedConsumer 1.0",
        description = "Reads any AVRO messages from a topic in a shared consumer")
public class GenericAvroShareConsumer extends AbstractBaseShareConsumer<Integer, GenericRecord> {
    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the customers (default = ${DEFAULT-VALUE})")
    private String topic = "customer";
    @CommandLine.Option(names = {"-q", "--quiet"},
            description = "Suppress output")
    private boolean quiet = false;

    final private Duration duration = Duration.ofMillis(10000);

    @Override
    protected Collection<String> getTopicsList() {
        return Collections.singletonList(topic);
    }

    @Override
    protected int consumeBatch(KafkaShareConsumer<Integer, GenericRecord> consumer) {
        ConsumerRecords<Integer, GenericRecord> records = consumer.poll(duration);
        if (!quiet)
            System.out.printf("*** Batch size %d%n", records.count());

        for (ConsumerRecord<Integer, GenericRecord> record : records) {
            Schema writerSchema = record.value().getSchema();
            long threadId = Thread.currentThread().threadId();

            if (!quiet)
                System.out.printf("Thread %d Found \"%s\" [%s] [%d] %s%n", threadId, writerSchema.getFullName(), record.partition(), record.key(), record.value().toString());
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        }

        return records.count();
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "genericAvroShareConsumer");
        properties.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        super.addProperties(properties);
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new GenericAvroShareConsumer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
