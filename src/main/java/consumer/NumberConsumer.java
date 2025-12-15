package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import picocli.CommandLine;

import java.time.Duration;
import java.util.*;

@CommandLine.Command(name = "NumberConsumer",
        version = "NumberConsumer 1.0",
        description = "Reads Longs from a topic")
public class NumberConsumer extends AbstractBaseConsumer<Void, Long> {
    @CommandLine.Option(names = {"--number-topic"},
            description = "Topic for the numbers (default = ${DEFAULT-VALUE})",
            defaultValue = "numbers")
    private String numberTopic = "numbers";

    @CommandLine.Option(names = {"--single"},
            description = "Commit each message (default = false)")
    private boolean single = false;

    @CommandLine.Option(names = {"--asynch"},
            description = "Commit asynchronously (default = ${DEFAULT-VALUE}")
    private boolean async = false;
    private final Duration duration = Duration.ofMillis(10000);

    @Override
    protected Collection<String> getTopicsList() {
        return Collections.singletonList(numberTopic);
    }

    @Override
    protected int consumeBatch(KafkaConsumer<Void, Long> consumer) {
        ConsumerRecords<Void, Long> records = consumer.poll(duration);
        System.out.printf("*** Batch size %d%n", records.count());
        for (ConsumerRecord<Void, Long> record : records) {
            System.out.println("Found " + record.value());
            if (single) {
                commitMessages(consumer);
            }
        }
        if (!single) {
            commitMessages(consumer);
        }

        printOffsets(consumer);

        return records.count();
    }

    private void commitMessages(KafkaConsumer<Void, Long> consumer) {
        if (async) {
            consumer.commitAsync((offsets, exception) -> System.out.println(offsets));
        }
        else {
            consumer.commitSync();
        }
    }

    private void printOffsets(KafkaConsumer<Void, Long> consumer) {

    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumeNumbers");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        super.addProperties(properties);
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new NumberConsumer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
