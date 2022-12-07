package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import picocli.CommandLine;
import schema.Customer;
import schema.Product;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@CommandLine.Command(name = "CustomerConsumer",
        version = "CustomerConsumer 1.0",
        description = "Reads Consumers from a topic")
public class CustomerConsumer extends AbstractBaseConsumer<Integer, Customer> {
    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the customers (default = ${DEFAULT-VALUE})")
    private String customerTopic = "customer";

    @CommandLine.Option(names = {"--single"},
            description = "Commit each message (default = false)")
    private boolean single = false;

    @CommandLine.Option(names = {"--asynch"},
            description = "Commit asynchronously (default = ${DEFAULT-VALUE})")
    private boolean async = false;
    final private Duration duration = Duration.ofMillis(10000);

    @Override
    protected Collection<String> getTopicsList() {
        return Arrays.asList(customerTopic);
    }

    @Override
    protected void subscribe(KafkaConsumer<Integer, Customer> consumer) {
        consumer.subscribe(Arrays.asList(customerTopic));
    }

    @Override
    protected int consumeBatch(KafkaConsumer<Integer, Customer> consumer) {
        ConsumerRecords<Integer, Customer> records = consumer.poll(duration);
        System.out.printf("*** Batch size %d%n", records.count());
        for (ConsumerRecord<Integer, Customer> record : records) {
            System.out.println("Found " + record.value());
            if (single) {
                commitMessages(consumer);
            }
        }
        if (!single) {
            commitMessages(consumer);
        }

        return records.count();
    }

    private void commitMessages(KafkaConsumer<Integer, Customer> consumer) {
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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumeProducts");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        super.addProperties(properties);
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CustomerConsumer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
