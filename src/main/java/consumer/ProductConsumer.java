package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import picocli.CommandLine;
import schema.Product;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@CommandLine.Command(name = "ProductConsumer",
        version = "ProductConsumer 1.0",
        description = "Reads Products from a topic")
public class ProductConsumer extends AbstractBaseConsumer<Integer, Product> {
    @CommandLine.Option(names = {"--product-topic"},
            description = "Topic for the products (default = ${DEFAULT-VALUE})")
    private String productTopic = "product";

    @CommandLine.Option(names = {"--single"},
            description = "Commit each message (default = false)")
    private boolean single = false;

    @CommandLine.Option(names = {"--asynch"},
            description = "Commit asynchronously (default = ${DEFAULT-VALUE})")
    private boolean async = false;
    private Duration duration = Duration.ofMillis(10000);

    @Override
    protected Collection<String> getTopicsList() {
        return Arrays.asList(productTopic);
    }

    @Override
    protected void subscribe(KafkaConsumer<Integer, Product> consumer) {
        consumer.subscribe(Arrays.asList(productTopic));
    }

    @Override
    protected int consumeBatch(KafkaConsumer<Integer, Product> consumer) {
        ConsumerRecords<Integer, Product> records = consumer.poll(duration);
        System.out.printf("*** Batch size %d%n", records.count());
        for (ConsumerRecord<Integer, Product> record : records) {
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

    private void commitMessages(KafkaConsumer<Integer, Product> consumer) {
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
            new CommandLine(new ProductConsumer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
